defmodule Jobforge.WorkerTest do
  use ExUnit.Case, async: true
  alias Jobforge.{Job, Worker}

  defmodule Echo do
    use Jobforge.Worker, max_attempts: 2, priority: 3, queue: "echo"
    @impl true
    def perform(%Job{args: result}), do: result
  end

  defmodule Raiser do
    use Jobforge.Worker
    @impl true
    def perform(%Job{args: :raise}), do: raise("kaboom")
    def perform(%Job{args: :throw}), do: throw(:ball)
    def perform(%Job{args: :exit}), do: exit(:gone)
  end

  defmodule Plain do
    def perform(_job), do: :ok
  end

  test "use declares defaults and a new/2 helper" do
    assert Worker.defaults(Echo) == [max_attempts: 2, priority: 3, queue: "echo"]
    assert Worker.defaults(Plain) == []
    {:ok, job} = Echo.new(:ok, priority: 0)
    assert {job.worker, job.max_attempts, job.priority, job.queue} == {Echo, 2, 0, "echo"}
  end

  test "valid? checks for perform/1" do
    assert Worker.valid?(Echo)
    assert Worker.valid?(Plain)
    refute Worker.valid?(String)
    refute Worker.valid?(NotAModule)
    refute Worker.valid?("Echo")
  end

  test "run normalises return values" do
    run = fn result -> Worker.run(Job.new!(Echo, result)) end
    assert run.(:ok) == :ok
    assert run.({:ok, 42}) == :ok
    assert run.({:error, :nope}) == {:error, :nope}
    assert run.({:snooze, 10}) == {:snooze, 10}
    assert run.({:cancel, :bye}) == {:cancel, :bye}
    assert run.(:weird) == {:error, {:bad_return, :weird}}
    assert run.({:snooze, -1}) == {:error, {:bad_return, {:snooze, -1}}}
  end

  test "run captures raises, throws and exits" do
    assert {:error, msg} = Worker.run(Job.new!(Raiser, :raise))
    assert msg =~ "kaboom"
    assert {:error, msg} = Worker.run(Job.new!(Raiser, :throw))
    assert msg =~ ":ball"
    assert {:error, msg} = Worker.run(Job.new!(Raiser, :exit))
    assert msg =~ ":gone"
  end

  test "rejects unknown use options" do
    assert_raise ArgumentError, ~r/unsupported/, fn ->
      Code.compile_string("defmodule BadWorker do use Jobforge.Worker, colour: :red end")
    end
  end
end
