defmodule Jobforge.JobTest do
  use ExUnit.Case, async: true
  alias Jobforge.Job

  test "defaults" do
    {:ok, job} = Job.new(MyWorker, %{"x" => 1}, now: 1_000)
    assert job.state == :available
    assert job.run_at == 1_000
    assert job.inserted_at == 1_000
    assert job.priority == 0
    assert job.max_attempts == 5
    assert job.attempt == 0
    assert job.queue == "default"
    assert job.errors == []
    assert byte_size(job.id) == 16
  end

  test "ids are unique" do
    ids = for _ <- 1..1_000, do: Job.new!(W, nil).id
    assert length(Enum.uniq(ids)) == 1_000
  end

  test "schedule_in and run_at produce scheduled jobs" do
    assert {:ok, %Job{state: :scheduled, run_at: 1_500}} =
             Job.new(W, nil, now: 1_000, schedule_in: 500)

    assert {:ok, %Job{state: :scheduled, run_at: 9_000}} =
             Job.new(W, nil, now: 1_000, run_at: 9_000)

    assert {:ok, %Job{state: :available}} = Job.new(W, nil, now: 1_000, run_at: 500)
    assert {:ok, %Job{state: :available}} = Job.new(W, nil, now: 1_000, schedule_in: 0)
  end

  test "unique :args derives a key from worker and args" do
    {:ok, job} = Job.new(W, [1, 2], unique: :args)
    assert job.unique == {W, [1, 2]}
    {:ok, job} = Job.new(W, [1, 2], unique: "custom")
    assert job.unique == "custom"
  end

  test "validation errors" do
    assert {:error, {:invalid_worker, "nope"}} = Job.new("nope", nil)
    assert {:error, {:invalid_priority, :high}} = Job.new(W, nil, priority: :high)
    assert {:error, {:invalid_max_attempts, 0}} = Job.new(W, nil, max_attempts: 0)
    assert {:error, {:invalid_timeout, -5}} = Job.new(W, nil, timeout: -5)
    assert {:error, {:invalid_schedule_in, -1}} = Job.new(W, nil, schedule_in: -1)

    assert {:error, :run_at_and_schedule_in_are_exclusive} =
             Job.new(W, nil, schedule_in: 1, run_at: 2)

    assert {:error, {:unknown_options, [:colour]}} = Job.new(W, nil, colour: :red)
    assert_raise ArgumentError, fn -> Job.new!(W, nil, priority: "x") end
  end

  test "lifecycle helpers" do
    job = Job.new!(W, nil, now: 0, max_attempts: 2)
    refute Job.terminal?(job)
    assert Job.due?(job, 0)
    assert Job.retryable?(%{job | attempt: 1})
    refute Job.retryable?(%{job | attempt: 2})
    assert Job.terminal?(%{job | state: :discarded})

    job =
      %{job | attempt: 1}
      |> Job.record_error(:boom, 10)
      |> Map.put(:attempt, 2)
      |> Job.record_error(:bang, 20)

    assert [%{attempt: 2, error: :bang, at: 20}, %{attempt: 1, error: :boom, at: 10}] = job.errors
  end
end
