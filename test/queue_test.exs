defmodule Jobforge.QueueTest do
  use ExUnit.Case, async: true
  alias Jobforge.{Job, Queue}

  # Workers report to the test process registered under args["to"].
  defmodule Recorder do
    use Jobforge.Worker

    @impl true
    def perform(%Job{args: %{"to" => to} = args} = job) do
      send(to, {:ran, args["tag"], job.attempt, self()})

      case args["result"] do
        nil -> :ok
        {:sleep, ms} -> Process.sleep(ms) && :ok
        other -> other
      end
    end
  end

  defmodule Flaky do
    use Jobforge.Worker, max_attempts: 4

    @impl true
    def perform(%Job{args: %{"to" => to, "fail_until" => n}, attempt: attempt}) do
      send(to, {:attempt, attempt})
      if attempt < n, do: {:error, {:not_yet, attempt}}, else: :ok
    end

    @impl true
    def backoff(_job), do: 5
  end

  defmodule Crasher do
    use Jobforge.Worker, max_attempts: 1

    @impl true
    def perform(%Job{args: :raise}), do: raise("boom")
    def perform(%Job{args: :kill}), do: Process.exit(self(), :kill)
  end

  defp start(opts \\ []) do
    q = start_supervised!({Queue, Keyword.put_new(opts, :backoff, {:constant, 5})})
    q
  end

  defp args(tag, extra \\ %{}), do: Map.merge(%{"to" => self(), "tag" => tag}, extra)

  test "runs a job and forgets it once completed" do
    q = start()
    {:ok, job} = Queue.insert(q, Recorder, args(:a))
    assert_receive {:ran, :a, 1, _}
    :ok = Queue.drain(q)
    assert Queue.get(q, job.id) == {:error, :not_found}
    assert %{completed_total: 1, available: 0, executing: 0} = Queue.stats(q)
  end

  test "higher priority (lower number) runs first, FIFO within a priority" do
    q = start(concurrency: 1, paused: true)

    for {tag, prio} <- [low1: 5, high1: 0, low2: 5, mid: 2, high2: 0] do
      {:ok, _} = Queue.insert(q, Recorder, args(tag), priority: prio)
    end

    :ok = Queue.resume(q)
    :ok = Queue.drain(q)
    order = for _ <- 1..5, do: assert_receive({:ran, _tag, _, _}) |> elem(1)
    assert order == [:high1, :high2, :mid, :low1, :low2]
  end

  test "never exceeds the concurrency limit" do
    parent = self()
    counter = :counters.new(2, [])

    defmodule Gauge do
      use Jobforge.Worker

      @impl true
      def perform(%Job{args: %{"c" => c, "to" => to}}) do
        :counters.add(c, 1, 1)
        now = :counters.get(c, 1)
        if now > :counters.get(c, 2), do: :counters.put(c, 2, now)
        Process.sleep(10)
        :counters.sub(c, 1, 1)
        send(to, :done)
        :ok
      end
    end

    q = start(concurrency: 3)
    for _ <- 1..20, do: {:ok, _} = Queue.insert(q, Gauge, %{"c" => counter, "to" => parent})
    :ok = Queue.drain(q)
    for _ <- 1..20, do: assert_receive(:done)
    assert :counters.get(counter, 2) == 3
  end

  test "scheduled jobs wait for their time" do
    q = start()
    t0 = System.monotonic_time(:millisecond)
    {:ok, job} = Queue.insert(q, Recorder, args(:later), schedule_in: 80)
    assert job.state == :scheduled
    assert %{scheduled: 1} = Queue.stats(q)
    refute_receive {:ran, :later, _, _}, 40
    assert_receive {:ran, :later, 1, _}, 500
    assert System.monotonic_time(:millisecond) - t0 >= 80
  end

  test "retries with backoff until success" do
    q = start()
    {:ok, job} = Queue.insert(q, Flaky, %{"to" => self(), "fail_until" => 3})
    :ok = Queue.drain(q)
    for n <- 1..3, do: assert_receive({:attempt, ^n})
    refute_receive {:attempt, 4}, 20
    assert Queue.get(q, job.id) == {:error, :not_found}
    assert %{failed_attempts: 2, completed_total: 1} = Queue.stats(q)
  end

  test "discards after max attempts and keeps the error history" do
    q = start()
    {:ok, job} = Queue.insert(q, Flaky, %{"to" => self(), "fail_until" => 99})
    :ok = Queue.drain(q)
    {:ok, dead} = Queue.get(q, job.id)
    assert dead.state == :discarded
    assert dead.attempt == 4
    assert Enum.map(dead.errors, & &1.error) == for(n <- 4..1//-1, do: {:not_yet, n})
    assert [%Job{id: id}] = Queue.list(q, :discarded)
    assert id == job.id

    # A manual retry gets a fresh set of attempts.
    {:ok, retried} = Queue.retry(q, job.id)
    assert retried.attempt == 0
    :ok = Queue.drain(q)
    assert {:ok, %Job{state: :discarded, attempt: 4}} = Queue.get(q, job.id)
    assert :ok = Queue.delete(q, job.id)
    assert Queue.get(q, job.id) == {:error, :not_found}
  end

  test "exceptions and kills become errors" do
    q = start()
    {:ok, raised} = Queue.insert(q, Crasher, :raise)
    {:ok, killed} = Queue.insert(q, Crasher, :kill)
    :ok = Queue.drain(q)
    {:ok, raised} = Queue.get(q, raised.id)
    {:ok, killed} = Queue.get(q, killed.id)
    assert [%{error: msg}] = raised.errors
    assert msg =~ "boom"
    assert [%{error: {:exit, exit_msg}}] = killed.errors
    assert exit_msg =~ "killed"
    assert Process.alive?(q)
  end

  test "attempts that run too long are killed and retried" do
    q = start()

    {:ok, job} =
      Queue.insert(q, Recorder, args(:slow, %{"result" => {:sleep, 1_000}}),
        timeout: 30,
        max_attempts: 2
      )

    assert_receive {:ran, :slow, 1, pid1}
    assert_receive {:ran, :slow, 2, _pid2}, 500
    refute Process.alive?(pid1)
    :ok = Queue.drain(q, 2_000)
    {:ok, job} = Queue.get(q, job.id)
    assert job.state == :discarded
    assert [%{error: {:timeout, 30}}, %{error: {:timeout, 30}}] = job.errors
  end

  test "snooze reschedules without using an attempt" do
    q = start()

    {:ok, job} =
      Queue.insert(q, Recorder, args(:snoozy, %{"result" => {:snooze, 20}}), max_attempts: 1)

    assert_receive {:ran, :snoozy, 1, _}
    assert_receive {:ran, :snoozy, 1, _}, 500
    {:ok, _} = Queue.cancel(q, job.id)
    assert %{snoozed_total: n} = Queue.stats(q)
    assert n >= 1
  end

  test "workers can cancel themselves" do
    q = start()
    {:ok, job} = Queue.insert(q, Recorder, args(:bail, %{"result" => {:cancel, :not_needed}}))
    :ok = Queue.drain(q)

    assert {:ok, %Job{state: :cancelled, errors: [%{error: {:cancelled, :not_needed}}]}} =
             Queue.get(q, job.id)

    refute_receive {:ran, :bail, 2, _}, 30
  end

  test "cancel waiting and running jobs" do
    q = start()
    {:ok, later} = Queue.insert(q, Recorder, args(:never), schedule_in: 50)
    assert {:ok, %Job{state: :cancelled}} = Queue.cancel(q, later.id)
    refute_receive {:ran, :never, _, _}, 100

    {:ok, running} = Queue.insert(q, Recorder, args(:long, %{"result" => {:sleep, 5_000}}))
    assert_receive {:ran, :long, 1, pid}
    assert {:ok, %Job{state: :cancelled}} = Queue.cancel(q, running.id)
    refute Process.alive?(pid)
    assert Queue.cancel(q, running.id) == {:error, :already_finished}
    assert Queue.cancel(q, "nope") == {:error, :not_found}
    :ok = Queue.drain(q)
  end

  test "unique keys block duplicates until the job finishes" do
    q = start(paused: true)
    {:ok, first} = Queue.insert(q, Recorder, args(:u), unique: "report-42")

    assert Queue.insert(q, Recorder, args(:u2), unique: "report-42") ==
             {:error, {:duplicate, first.id}}

    :ok = Queue.resume(q)
    :ok = Queue.drain(q)
    assert {:ok, _} = Queue.insert(q, Recorder, args(:u3), unique: "report-42")
    :ok = Queue.drain(q)
  end

  test "worker defaults apply and invalid workers are rejected" do
    q = start(paused: true)
    {:ok, job} = Queue.insert(q, Flaky, %{"to" => self(), "fail_until" => 1})
    assert job.max_attempts == 4
    {:ok, job} = Queue.insert(q, Flaky, %{"to" => self(), "fail_until" => 1}, max_attempts: 9)
    assert job.max_attempts == 9
    assert Queue.insert(q, String, nil) == {:error, {:invalid_worker, String}}
    assert {:error, {:invalid_priority, _}} = Queue.insert(q, Recorder, nil, priority: "x")
  end

  test "rate limit spaces out starts" do
    q = start(rate_limit: {50, 1}, concurrency: 10)
    t0 = System.monotonic_time(:millisecond)
    for i <- 1..6, do: {:ok, _} = Queue.insert(q, Recorder, args(i))
    :ok = Queue.drain(q, 2_000)
    elapsed = System.monotonic_time(:millisecond) - t0
    # 1 immediate + 5 more at 20 ms each.
    assert elapsed >= 95
  end

  test "pause stops new attempts" do
    q = start(paused: true)
    {:ok, _} = Queue.insert(q, Recorder, args(:p))
    refute_receive {:ran, :p, _, _}, 50
    assert %{available: 1, paused: true} = Queue.stats(q)
    :ok = Queue.resume(q)
    assert_receive {:ran, :p, 1, _}
  end

  test "events are reported" do
    parent = self()

    q =
      start(on_event: fn event, job, meta -> send(parent, {:event, event, job.attempt, meta}) end)

    {:ok, _} = Queue.insert(q, Flaky, %{"to" => self(), "fail_until" => 2})
    :ok = Queue.drain(q)
    assert_receive {:event, :inserted, 0, _}
    assert_receive {:event, :started, 1, _}
    assert_receive {:event, :failed, 1, %{retry_in_ms: 5, error: {:not_yet, 1}}}
    assert_receive {:event, :started, 2, _}
    assert_receive {:event, :completed, 2, %{duration_ms: d}} when d >= 0
  end

  describe "with a DETS store" do
    setup do
      path = Path.join(System.tmp_dir!(), "jobforge-q-#{System.unique_integer([:positive])}.dets")
      on_exit(fn -> File.rm(path) end)
      {:ok, store: {Jobforge.Store.DETS, path: path}}
    end

    test "pending jobs survive a restart", %{store: store} do
      q = start_supervised!({Queue, store: store, paused: true}, id: :first)
      {:ok, a} = Queue.insert(q, Recorder, args(:a))
      {:ok, b} = Queue.insert(q, Recorder, args(:b), schedule_in: 30)
      :ok = stop_supervised(:first)

      q = start_supervised!({Queue, store: store}, id: :second)
      assert_receive {:ran, :a, 1, _}
      assert_receive {:ran, :b, 1, _}, 500
      :ok = Queue.drain(q)
      assert Queue.get(q, a.id) == {:error, :not_found}
      assert Queue.get(q, b.id) == {:error, :not_found}
    end

    test "an attempt interrupted by a crash runs again", %{store: store} do
      Process.flag(:trap_exit, true)
      {:ok, q} = Queue.start_link(store: store)
      {:ok, job} = Queue.insert(q, Recorder, args(:x, %{"result" => {:sleep, 5_000}}))
      assert_receive {:ran, :x, 1, _}
      Process.exit(q, :kill)
      assert_receive {:EXIT, ^q, :killed}

      q = start_supervised!({Queue, store: store})
      assert_receive {:ran, :x, 2, _}, 1_000
      {:ok, reloaded} = Queue.get(q, job.id)
      assert [%{error: :interrupted, attempt: 1}] = reloaded.errors
      {:ok, _} = Queue.cancel(q, job.id)
    end

    test "dead letters are kept across restarts", %{store: store} do
      q = start_supervised!({Queue, store: store}, id: :first)
      {:ok, job} = Queue.insert(q, Crasher, :raise)
      :ok = Queue.drain(q)
      :ok = stop_supervised(:first)

      q = start_supervised!({Queue, store: store}, id: :second)
      assert {:ok, %Job{state: :discarded}} = Queue.get(q, job.id)
    end
  end
end
