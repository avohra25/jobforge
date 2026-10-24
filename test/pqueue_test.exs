defmodule Jobforge.PQueueTest do
  use ExUnit.Case, async: true
  alias Jobforge.PQueue

  test "empty queue" do
    q = PQueue.new()
    assert PQueue.empty?(q)
    assert PQueue.size(q) == 0
    assert PQueue.pop(q) == :empty
    assert PQueue.peek(q) == :empty
  end

  test "pops in key order" do
    q = PQueue.from_list([{5, :e}, {1, :a}, {3, :c}, {2, :b}, {4, :d}])
    assert PQueue.size(q) == 5
    assert PQueue.peek(q) == {:ok, {1, :a}}
    assert PQueue.to_list(q) == [{1, :a}, {2, :b}, {3, :c}, {4, :d}, {5, :e}]
  end

  test "equal keys come out in insertion order" do
    q = Enum.reduce(1..50, PQueue.new(), fn i, q -> PQueue.push(q, rem(i, 3), i) end)
    values = q |> PQueue.to_list() |> Enum.map(&elem(&1, 1))
    expected = Enum.sort_by(1..50, &{rem(&1, 3), &1})
    assert values == expected
  end

  test "tuple keys order by priority then time" do
    q =
      PQueue.new()
      |> PQueue.push({1, 200}, :late_low)
      |> PQueue.push({0, 300}, :later_high)
      |> PQueue.push({1, 100}, :early_low)

    assert Enum.map(PQueue.to_list(q), &elem(&1, 1)) == [:later_high, :early_low, :late_low]
  end

  test "is persistent: old versions are unaffected" do
    q1 = PQueue.from_list([{2, :b}, {1, :a}])
    {:ok, {1, :a}, q2} = PQueue.pop(q1)
    assert PQueue.size(q1) == 2
    assert PQueue.size(q2) == 1
    assert PQueue.peek(q1) == {:ok, {1, :a}}
  end

  test "randomised against a sorted list" do
    :rand.seed(:exsss, {1, 2, 3})

    for _round <- 1..50 do
      ops =
        for _ <- 1..200, do: if(:rand.uniform() < 0.6, do: {:push, :rand.uniform(40)}, else: :pop)

      Enum.reduce(ops, {PQueue.new(), [], 0}, fn
        {:push, k}, {q, model, n} ->
          {PQueue.push(q, k, n), model ++ [{k, n}], n + 1}

        :pop, {q, [], n} ->
          assert PQueue.pop(q) == :empty
          {q, [], n}

        :pop, {q, model, n} ->
          best = Enum.min_by(model, fn {k, i} -> {k, i} end)
          assert {:ok, ^best, q} = PQueue.pop(q)
          {q, List.delete(model, best), n}
      end)
    end
  end
end
