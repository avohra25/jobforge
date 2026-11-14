defmodule Jobforge.CrontabTest do
  use ExUnit.Case, async: true
  alias Jobforge.{Job, Queue}

  defmodule Tick do
    use Jobforge.Worker, max_attempts: 1

    @impl true
    def perform(%Job{args: %{"to" => to} = args} = job) do
      send(to, {:tick, args["tag"], job.meta, self()})
      if ms = args["sleep"], do: Process.sleep(ms)
      :ok
    end
  end

  defp start(opts) do
    me = self()
    on_event = fn event, job, meta -> send(me, {:event, event, job, meta}) end
    start_supervised!({Queue, Keyword.put_new(opts, :on_event, on_event)})
  end

  defp args(tag, extra \\ %{}), do: Map.merge(%{"to" => self(), "tag" => tag}, extra)

  test "an entry from :crontab inserts a job on every tick" do
    q = start(crontab: [{"every-second", "* * * * * *", Tick, args(:sec)}])
    assert [%{name: "every-second", skipped: 0, next_at: next_at}] = Queue.crontab(q)
    assert rem(next_at, 1000) == 0

    assert_receive {:tick, :sec, %{cron: "every-second", scheduled_for: t1}, _}, 2_500
    assert_receive {:tick, :sec, %{scheduled_for: t2}, _}, 2_500
    assert t2 - t1 == 1_000
    assert %{cron_entries: 1, completed_total: done} = Queue.stats(q)
    assert done >= 1
  end

  test "schedule_cron/6 adds and replaces entries, unschedule_cron/2 removes them" do
    q = start([])
    assert {:ok, at} = Queue.schedule_cron(q, :report, "0 3 * * *", Tick, args(:r), priority: 2)

    assert DateTime.from_unix!(at, :millisecond) |> Map.take([:hour, :minute, :second]) ==
             %{hour: 3, minute: 0, second: 0}

    assert {:ok, _} = Queue.schedule_cron(q, :report, "* * * * * *", Tick, args(:r2))
    assert [%{name: :report, expr: "* * * * * *"}] = Queue.crontab(q)
    assert_receive {:tick, :r2, %{cron: :report}, _}, 2_500

    assert :ok = Queue.unschedule_cron(q, :report)
    assert {:error, :not_found} = Queue.unschedule_cron(q, :report)
    assert Queue.crontab(q) == []
    :ok = Queue.drain(q)
    flush()
    refute_receive {:tick, _, _, _}, 1_300
  end

  test "an entry never overlaps itself by default" do
    q = start(crontab: [{"slow", "* * * * * *", Tick, args(:slow, %{"sleep" => 2_200})}])
    assert_receive {:tick, :slow, _, pid}, 2_500

    assert_receive {:event, :cron_skipped, %Job{meta: %{cron: "slow"}}, %{running: running}},
                   1_500

    assert {:ok, %Job{state: :executing}} = Queue.get(q, running)
    assert Process.alive?(pid)
    refute_received {:tick, :slow, _, _}
    assert [%{skipped: skipped}] = Queue.crontab(q)
    assert skipped >= 1
  end

  test "a custom :unique lets runs overlap" do
    q =
      start(
        concurrency: 5,
        crontab: [{"fast", "* * * * * *", Tick, args(:f, %{"sleep" => 1_600}), unique: nil}]
      )

    assert_receive {:tick, :f, _, p1}, 2_500
    assert_receive {:tick, :f, _, p2}, 1_500
    assert p1 != p2 and Process.alive?(p1)
    refute_received {:event, :cron_skipped, _, _}
    Queue.unschedule_cron(q, "fast")
  end

  test "job options and worker defaults reach the inserted jobs" do
    q =
      start(
        paused: true,
        crontab: [{"x", "* * * * * *", Tick, args(:x), priority: 7, meta: %{"team" => "ops"}}]
      )

    assert_receive {:event, :inserted, %Job{} = job, %{cron: "x"}}, 2_500
    assert job.priority == 7
    assert job.max_attempts == 1
    assert job.unique == {:jobforge_cron, "x"}
    assert %{"team" => "ops", cron: "x", scheduled_for: _} = job.meta
    assert [%{last_job_id: id}] = Queue.crontab(q)
    assert id == job.id
  end

  test "invalid entries are rejected" do
    q = start([])

    assert {:error, {:invalid_cron, {:hour, "25", _}}} =
             Queue.schedule_cron(q, "a", "0 25 * * *", Tick, %{})

    assert {:error, {:field_count, 2}} = Queue.schedule_cron(q, "a", "* *", Tick, %{})
    assert {:error, {:cron_never_fires, _}} = Queue.schedule_cron(q, "a", "0 0 30 2 *", Tick, %{})
    assert {:error, {:invalid_worker, Nope}} = Queue.schedule_cron(q, "a", "@daily", Nope, %{})

    assert {:error, {:unsupported_cron_opts, [:schedule_in]}} =
             Queue.schedule_cron(q, "a", "@daily", Tick, %{}, schedule_in: 5)

    assert {:error, {:invalid_priority, :high}} =
             Queue.schedule_cron(q, "a", "@daily", Tick, %{}, priority: :high)

    assert Queue.crontab(q) == []

    Process.flag(:trap_exit, true)

    assert {:error, {:duplicate_cron, "d"}} =
             Queue.start_link(crontab: [{"d", "@daily", Tick, %{}}, {"d", "@hourly", Tick, %{}}])

    assert {:error, {:invalid_crontab_entry, :oops}} = Queue.start_link(crontab: [:oops])
  end

  test "drain/2 doesn't wait for future ticks" do
    q = start(crontab: [{"daily", "@daily", Tick, args(:d)}])
    assert :ok = Queue.drain(q, 500)
  end

  defp flush do
    receive do
      {:tick, _, _, _} -> flush()
    after
      0 -> :ok
    end
  end
end
