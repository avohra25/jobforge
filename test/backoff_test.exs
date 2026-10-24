defmodule Jobforge.BackoffTest do
  use ExUnit.Case, async: true
  alias Jobforge.Backoff

  defp mid, do: 0.5

  test "exponential doubles and caps" do
    p = {:exponential, base: 100, max: 1_000, jitter: 0.0}
    assert Enum.map(1..6, &Backoff.compute(p, &1)) == [100, 200, 400, 800, 1_000, 1_000]
  end

  test "exponential survives huge attempt numbers" do
    assert Backoff.compute({:exponential, base: 10, max: 5_000, jitter: 0.0}, 10_000) == 5_000
  end

  test "jitter stays within bounds and is centred" do
    p = {:exponential, base: 1_000, max: 10_000, jitter: 0.2}
    assert Backoff.compute(p, 1, &mid/0) == 1_000
    assert Backoff.compute(p, 1, fn -> 0.0 end) == 800
    assert Backoff.compute(p, 1, fn -> 0.999999 end) == 1_200

    for _ <- 1..500 do
      d = Backoff.compute(p, 2)
      assert d >= 1_600 and d <= 2_400
    end
  end

  test "linear grows by base" do
    p = {:linear, base: 250, max: 1_000}
    assert Enum.map(1..5, &Backoff.compute(p, &1)) == [250, 500, 750, 1_000, 1_000]
  end

  test "constant and function policies" do
    assert Backoff.compute({:constant, 42}, 7) == 42
    assert Backoff.compute(fn a -> a * a end, 4) == 16
  end

  test "default policy is exponential from one second" do
    assert Backoff.compute(Backoff.default(), 1, &mid/0) == 1_000
    assert Backoff.compute(Backoff.default(), 3, &mid/0) == 4_000
  end

  test "rejects bad input" do
    assert_raise ArgumentError, fn -> Backoff.compute({:constant, 1}, 0) end
    assert_raise ArgumentError, fn -> Backoff.compute({:bogus, 1}, 1) end
    assert_raise ArgumentError, fn -> Backoff.compute(fn _ -> -1 end, 1) end
    assert_raise ArgumentError, fn -> Backoff.compute({:exponential, jitter: 2}, 1) end
  end
end
