defmodule Mix.Tasks.Jobforge.Demo do
  @shortdoc "Runs a small jobforge demo with flaky, slow and scheduled jobs"
  @moduledoc """
  Runs a self-contained demo queue and prints its event stream.

      mix jobforge.demo [--jobs 12] [--concurrency 3] [--rate 20] [--fail-rate 0.3] [--seed 1]

  Jobs sleep for a random 20 to 120 ms and fail with probability `--fail-rate`.
  Failures retry with a short exponential backoff until they succeed or run
  out of attempts. A couple of jobs are scheduled into the future and one
  is cancelled before it runs.
  """
  use Mix.Task

  alias Jobforge.{Job, Queue}

  defmodule DemoWorker do
    @moduledoc false
    use Jobforge.Worker, max_attempts: 4

    @impl true
    def perform(%Job{args: %{"ms" => ms, "fail_rate" => fail_rate}}) do
      Process.sleep(ms)
      if :rand.uniform() < fail_rate, do: {:error, :flaky_upstream}, else: :ok
    end
  end

  @switches [
    jobs: :integer,
    concurrency: :integer,
    rate: :integer,
    fail_rate: :float,
    seed: :integer
  ]

  @impl Mix.Task
  def run(argv) do
    {opts, _, _} = OptionParser.parse(argv, strict: @switches)
    Mix.Task.run("compile")
    seed = Keyword.get(opts, :seed, 1)
    :rand.seed(:exsss, {seed, seed, seed})

    t0 = System.monotonic_time(:millisecond)

    log = fn event, job, meta ->
      t = System.monotonic_time(:millisecond) - t0

      extra =
        meta
        |> Map.take([:duration_ms, :retry_in_ms, :error])
        |> Enum.map_join(" ", fn {k, v} -> "#{k}=#{inspect(v)}" end)

      IO.puts(
        String.pad_leading("#{t}ms", 7) <>
          "  #{pad(event)} job=#{job.args["n"]} attempt=#{job.attempt} #{extra}"
      )
    end

    {:ok, q} =
      Queue.start_link(
        concurrency: Keyword.get(opts, :concurrency, 3),
        rate_limit: {Keyword.get(opts, :rate, 20), 3},
        backoff: {:exponential, base: 50, max: 400, jitter: 0.2},
        on_event: log
      )

    fail_rate = Keyword.get(opts, :fail_rate, 0.3)
    n = Keyword.get(opts, :jobs, 12)

    ids =
      for i <- 1..n do
        sched = if rem(i, 5) == 0, do: [schedule_in: 300], else: []
        args = %{"n" => i, "ms" => 20 + :rand.uniform(100), "fail_rate" => fail_rate}
        {:ok, job} = Queue.insert(q, DemoWorker, args, [priority: rem(i, 3)] ++ sched)
        job.id
      end

    # Change of plans: cancel the last scheduled job before it runs.
    if n >= 10, do: {:ok, _} = Queue.cancel(q, Enum.at(ids, 9))

    :ok = Queue.drain(q, 30_000)
    stats = Queue.stats(q)
    IO.puts("\nfinished in #{System.monotonic_time(:millisecond) - t0}ms")

    IO.puts(
      "completed=#{stats.completed_total} failed_attempts=#{stats.failed_attempts} " <>
        "discarded=#{stats.discarded} cancelled=#{stats.cancelled}"
    )

    Queue.stop(q)
  end

  defp pad(event), do: event |> Atom.to_string() |> String.pad_trailing(9)
end
