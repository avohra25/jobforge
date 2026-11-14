defmodule Jobforge.DependencyTest do
  use ExUnit.Case, async: true
  alias Jobforge.{Job, Queue}

  defmodule Step do
    use Jobforge.Worker, max_attempts: 1

    @impl true
    def perform(%Job{args: %{"to" => to, "name" => name} = args}) do
      send(to, {:ran, name})
      if ms = args["sleep"], do: Process.sleep(ms)
      args["result"] || :ok
    end
  end

  defp start(opts \\ []), do: start_supervised!({Queue, opts}, id: make_ref())

  defp step(q, name, opts \\ [], extra \\ %{}) do
    {:ok, job} = Queue.insert(q, Step, Map.merge(%{"to" => self(), "name" => name}, extra), opts)
    job
  end

  defp ran do
    receive do
      {:ran, name} -> [name | ran()]
    after
      0 -> []
    end
  end

  test "Job.new/3 validates depends_on" do
    assert {:ok, %Job{state: :blocked, depends_on: ["a", "b"]}} =
             Job.new(Step, %{}, depends_on: ["a", %Job{id: "b", worker: Step}, "a"])

    assert {:ok, %Job{state: :available, depends_on: []}} = Job.new(Step, %{}, depends_on: [])
    assert {:error, {:invalid_depends_on, 5}} = Job.new(Step, %{}, depends_on: [5])
    assert {:error, {:invalid_depends_on, "x"}} = Job.new(Step, %{}, depends_on: "x")
    assert {:error, {:depends_on_itself, "me"}} = Job.new(Step, %{}, id: "me", depends_on: ["me"])
  end

  test "a job waits for its dependencies, then runs" do
    q = start(paused: true)
    a = step(q, :a)
    b = step(q, :b)
    c = step(q, :c, depends_on: [a, b.id])
    assert c.state == :blocked
    assert %{blocked: 1, available: 2} = Queue.stats(q)

    :ok = Queue.resume(q)
    :ok = Queue.drain(q)
    assert [x, y, :c] = ran()
    assert Enum.sort([x, y]) == [:a, :b]
    assert Queue.get(q, c.id) == {:error, :not_found}
  end

  test "chains run in order even with concurrency" do
    q = start(concurrency: 8)
    first = step(q, 0, [], %{"sleep" => 30})

    Enum.reduce(1..5, first, fn i, prev -> step(q, i, depends_on: [prev]) end)

    :ok = Queue.drain(q)
    assert ran() == [0, 1, 2, 3, 4, 5]
  end

  test "unknown ids count as completed; scheduled jobs keep their run_at" do
    q = start()
    j = step(q, :late, depends_on: ["gone"], schedule_in: 60)
    assert j.state == :scheduled
    blocker = step(q, :blocker, [], %{"sleep" => 20})
    k = step(q, :after, depends_on: [blocker], schedule_in: 200)
    assert k.state == :blocked
    Process.sleep(80)
    assert {:ok, %Job{state: :scheduled}} = Queue.get(q, k.id)
    :ok = Queue.drain(q)
    assert ran() == [:blocker, :late, :after]
  end

  test "a failed dependency cancels the whole chain" do
    q = start(paused: true)
    a = step(q, :a, [], %{"result" => {:error, :boom}})
    b = step(q, :b, depends_on: [a])
    c = step(q, :c, depends_on: [b])
    d = step(q, :d)
    e = step(q, :e, depends_on: [d, c])
    :ok = Queue.resume(q)
    :ok = Queue.drain(q)

    assert Enum.sort(ran()) == [:a, :d]
    assert {:ok, %Job{state: :discarded}} = Queue.get(q, a.id)

    assert {:ok, %Job{state: :cancelled, errors: [%{error: {:dependency_failed, aid}}]}} =
             Queue.get(q, b.id)

    assert aid == a.id

    assert {:ok, %Job{state: :cancelled, errors: [%{error: {:dependency_failed, bid}}]}} =
             Queue.get(q, c.id)

    assert bid == b.id
    assert {:ok, %Job{state: :cancelled}} = Queue.get(q, e.id)
    assert %{cancelled_total: 3, discarded_total: 1} = Queue.stats(q)
  end

  test "cancelling a dependency cancels dependents; inserting after a failure cancels at once" do
    q = start(paused: true)
    a = step(q, :a)
    b = step(q, :b, depends_on: [a])
    {:ok, _} = Queue.cancel(q, a.id)
    assert {:ok, %Job{state: :cancelled}} = Queue.get(q, b.id)

    late = step(q, :late, depends_on: [a])
    assert late.state == :cancelled
    assert [%{error: {:dependency_failed, _}}] = late.errors
  end

  test "retry re-checks dependencies" do
    q = start()
    a = step(q, :a, [], %{"result" => {:cancel, :nope}})
    b = step(q, :b, depends_on: [a])
    :ok = Queue.drain(q)
    assert ran() == [:a]

    assert Queue.retry(q, b.id) == {:error, {:dependency_failed, a.id}}

    # Make the dependency succeed, then the dependent can be retried.
    :ok = Queue.delete(q, a.id)
    assert {:ok, %Job{state: :available}} = Queue.retry(q, b.id)
    :ok = Queue.drain(q)
    assert ran() == [:b]
  end

  test "blocked jobs survive a restart and are re-linked" do
    dir = Path.join(System.tmp_dir!(), "jobforge-deps-#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(dir) end)
    store = {Jobforge.Store.DETS, path: Path.join(dir, "jobs.dets")}

    {:ok, q} = Queue.start_link(store: store, paused: true)
    a = step(q, :a)
    b = step(q, :b, depends_on: [a])
    c = step(q, :c, depends_on: [b])
    bad = step(q, :bad, [], %{"result" => {:error, :x}})
    d = step(q, :d, depends_on: [bad])
    :ok = Queue.stop(q)

    q = start(store: store)
    :ok = Queue.drain(q)
    assert ran() |> Enum.sort() == [:a, :b, :bad, :c]
    assert {:error, :not_found} = Queue.get(q, c.id)
    assert {:ok, %Job{state: :cancelled}} = Queue.get(q, d.id)
  end

  # Random DAGs with random outcomes: every job runs only after all its
  # dependencies completed, runs exactly when they all completed, and is
  # otherwise cancelled with {:dependency_failed, _} naming a failed one.
  test "random dependency graphs" do
    :rand.seed(:exsss, {3, 1, 4})

    for round <- 1..25 do
      me = self()
      on_event = fn ev, job, _ -> send(me, {:ev, ev, job.args["name"]}) end
      {:ok, q} = Queue.start_link(concurrency: :rand.uniform(6), paused: true, on_event: on_event)
      n = 8 + :rand.uniform(20)

      jobs =
        Enum.reduce(0..(n - 1), %{}, fn i, acc ->
          deps = for j <- 0..(i - 1)//1, :rand.uniform(4) == 1, do: j
          result = Enum.random([:ok, :ok, :ok, :ok, {:error, :e}, {:cancel, :c}])
          extra = %{"result" => result, "sleep" => :rand.uniform(3)}
          job = step(q, i, [depends_on: Enum.map(deps, &acc[&1].job.id)], extra)
          Map.put(acc, i, %{job: job, deps: deps, result: result})
        end)

      :ok = Queue.resume(q)
      :ok = Queue.drain(q, 10_000)

      events = collect([])
      order = for {:ev, :started, name} <- events, do: name
      finished = for {:ev, ev, name} <- events, ev in [:completed], do: name
      assert length(order) == length(Enum.uniq(order))

      done =
        Enum.reduce(0..(n - 1), %{}, fn i, ok ->
          %{deps: deps, result: result} = jobs[i]
          deps_ok = Enum.all?(deps, &ok[&1])
          Map.put(ok, i, deps_ok and result == :ok)
        end)

      for i <- 0..(n - 1) do
        %{job: job, deps: deps} = jobs[i]
        deps_ok = Enum.all?(deps, &done[&1])

        if deps_ok do
          assert i in order, "round #{round}: #{i} should have run"
          pos = Enum.find_index(order, &(&1 == i))

          for d <- deps,
              do:
                assert(
                  Enum.find_index(finished, &(&1 == d)) != nil and
                    Enum.find_index(order, &(&1 == d)) < pos
                )
        else
          refute i in order, "round #{round}: #{i} ran with a failed dependency"

          assert {:ok, %Job{state: :cancelled, errors: [%{error: {:dependency_failed, dep}} | _]}} =
                   Queue.get(q, job.id)

          assert Enum.any?(deps, &(jobs[&1].job.id == dep and not done[&1]))
        end
      end

      assert %{blocked: 0} = Queue.stats(q)
      :ok = Queue.stop(q)
    end
  end

  defp collect(acc) do
    receive do
      {:ev, _, _} = e -> collect([e | acc])
      {:ran, _} -> collect(acc)
    after
      0 -> Enum.reverse(acc)
    end
  end
end
