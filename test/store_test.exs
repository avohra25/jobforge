defmodule Jobforge.StoreTest do
  use ExUnit.Case, async: true
  alias Jobforge.Job
  alias Jobforge.Store.{DETS, Memory}

  setup do
    dir = Path.join(System.tmp_dir!(), "jobforge-store-#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(dir) end)
    {:ok, path: Path.join([dir, "nested", "jobs.dets"])}
  end

  defp job(n), do: Job.new!(W, %{"n" => n}, now: 1_000 + n)

  for {name, mod} <- [{"memory", Memory}, {"dets", DETS}] do
    describe "#{name} store" do
      test "put, overwrite, delete and list", %{path: path} do
        {:ok, s} = unquote(mod).init(path: path)
        [a, b, c] = Enum.map([3, 1, 2], &job/1)
        s = Enum.reduce([a, b, c], s, &unquote(mod).put(&2, &1))
        assert Enum.map(unquote(mod).all(s), & &1.args["n"]) == [1, 2, 3]

        s = unquote(mod).put(s, %{b | state: :executing, attempt: 1})
        s = unquote(mod).delete(s, c)
        s = unquote(mod).delete(s, "missing-id")
        jobs = unquote(mod).all(s)
        assert length(jobs) == 2
        assert Enum.find(jobs, &(&1.id == b.id)).state == :executing
        assert unquote(mod).close(s) == :ok
      end
    end
  end

  test "dets store survives reopen", %{path: path} do
    {:ok, s} = DETS.init(path: path)
    jobs = Enum.map(1..25, &job/1)
    s = Enum.reduce(jobs, s, &DETS.put(&2, &1))
    s = DETS.delete(s, hd(jobs).id)
    :ok = DETS.close(s)

    {:ok, s} = DETS.init(path: path)
    reloaded = DETS.all(s)
    assert length(reloaded) == 24
    assert reloaded == tl(jobs)
    DETS.close(s)
  end

  test "dets store recovers after its owner is killed", %{path: path} do
    parent = self()

    pid =
      spawn(fn ->
        {:ok, s} = DETS.init(path: path)
        Enum.reduce(1..10, s, &DETS.put(&2, job(&1)))
        send(parent, :written)
        Process.sleep(:infinity)
      end)

    assert_receive :written
    ref = Process.monitor(pid)
    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^ref, :process, _, :killed}

    {:ok, s} = DETS.init(path: path)
    assert length(DETS.all(s)) == 10
    DETS.close(s)
  end

  test "dets init reports unusable paths" do
    file = Path.join(System.tmp_dir!(), "jobforge-notadir-#{System.unique_integer([:positive])}")
    File.write!(file, "x")
    on_exit(fn -> File.rm(file) end)
    assert_raise File.Error, fn -> DETS.init(path: Path.join(file, "jobs.dets")) end
  end
end
