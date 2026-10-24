defmodule Jobforge.StressTest do
  @moduledoc """
  Randomised end-to-end check: many jobs with random priorities, delays,
  failures, crashes, snoozes and cancellations. Afterwards every job must be
  accounted for exactly once, no attempt may exceed max_attempts, and the
  concurrency limit must hold throughout.
  """
  use ExUnit.Case, async: true
  alias Jobforge.{Job, Queue}

  defmodule Chaos do
    use Jobforge.Worker

    @impl true
    def perform(%Job{
          args: %{"c" => c, "runs" => runs, "plan" => plan, "to" => to},
          attempt: attempt,
          id: id
        }) do
      # Index the plan by run number, not attempt: a snooze reruns the same attempt.
      run = :ets.update_counter(runs, id, 1, {id, 0})
      :counters.add(c, 1, 1)
      running = :counters.get(c, 1)
      if running > :counters.get(c, 2), do: :counters.put(c, 2, running)
      Process.sleep(:erlang.phash2({id, run}, 4))
      :counters.sub(c, 1, 1)
      send(to, {:attempt, id, attempt})

      case Enum.at(plan, run - 1, :ok) do
        :ok -> :ok
        :error -> {:error, :planned}
        :raise -> raise "planned crash"
        :snooze -> {:snooze, 3}
        :cancel -> {:cancel, :planned}
      end
    end
  end

  @tag timeout: 60_000
  test "random workload is fully accounted for" do
    for seed <- 1..5 do
      :rand.seed(:exsss, {seed, 7, 11})
      run_workload(seed)
    end
  end

  defp run_workload(seed) do
    counters = :counters.new(2, [])
    parent = self()
    events = :ets.new(:events, [:public, :bag])
    runs = :ets.new(:runs, [:public, :set])

    q =
      start_supervised!(
        {Queue,
         concurrency: 4,
         paused: true,
         backoff: {:exponential, base: 2, max: 10, jitter: 0.5},
         on_event: fn event, job, _ -> :ets.insert(events, {job.id, event}) end},
        id: {:q, seed}
      )

    outcomes = [:ok, :ok, :ok, :error, :raise, :snooze, :cancel]

    jobs =
      for _ <- 1..150 do
        plan = for _ <- 1..4, do: Enum.random(outcomes)
        max = Enum.random(1..3)

        opts =
          [priority: Enum.random(0..3), max_attempts: max] ++
            if(:rand.uniform() < 0.3, do: [schedule_in: Enum.random(1..20)], else: [])

        {:ok, job} =
          Queue.insert(
            q,
            Chaos,
            %{"c" => counters, "runs" => runs, "plan" => plan, "to" => parent},
            opts
          )

        {job, plan}
      end

    to_cancel = jobs |> Enum.take_random(15) |> Enum.map(fn {j, _} -> j.id end)
    # Cancel while paused: killing a running attempt would skip its gauge decrement.
    Enum.each(to_cancel, &Queue.cancel(q, &1))
    :ok = Queue.resume(q)

    :ok = Queue.drain(q, 20_000)
    assert :counters.get(counters, 2) <= 4
    assert :counters.get(counters, 1) == 0

    remaining = Map.new(Queue.list(q), &{&1.id, &1})

    for {job, plan} <- jobs do
      evs = for {_, e} <- :ets.lookup(events, job.id), do: e
      finals = Enum.filter(evs, &(&1 in [:completed, :discarded, :cancelled]))
      assert length(finals) == 1, "job #{job.id} had final events #{inspect(evs)}"

      case {hd(finals), remaining[job.id]} do
        {:completed, nil} -> :ok
        {:discarded, %Job{state: :discarded} = j} -> assert j.attempt == j.max_attempts
        {:cancelled, %Job{state: :cancelled}} -> :ok
        other -> flunk("inconsistent outcome #{inspect(other)} for plan #{inspect(plan)}")
      end

      if j = remaining[job.id], do: assert(j.attempt <= j.max_attempts)
    end

    assert Enum.all?(Map.values(remaining), &Job.terminal?/1)
    stop_supervised({:q, seed})
  end
end
