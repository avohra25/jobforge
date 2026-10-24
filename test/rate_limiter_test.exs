defmodule Jobforge.RateLimiterTest do
  use ExUnit.Case, async: true
  alias Jobforge.RateLimiter, as: RL

  test "starts full and allows a burst" do
    b = RL.new(10, 3, 0)
    {:ok, b} = RL.take(b, 0)
    {:ok, b} = RL.take(b, 0)
    {:ok, b} = RL.take(b, 0)
    assert {:wait, 100, _} = RL.take(b, 0)
  end

  test "refills continuously" do
    b = RL.new(4, 2, 0)
    {:ok, b} = RL.take(b, 0, 2)
    assert {:wait, 250, b} = RL.take(b, 0)
    assert {:wait, 100, b} = RL.take(b, 150)
    assert {:ok, b} = RL.take(b, 250)
    assert_in_delta RL.available(b, 250), 0.0, 1.0e-9
    assert_in_delta RL.available(b, 10_000), 2.0, 1.0e-9
  end

  test "time going backwards does not mint tokens" do
    b = RL.new(1, 1, 1_000)
    {:ok, b} = RL.take(b, 1_000)
    assert {:wait, _, _} = RL.take(b, 500)
  end

  test "long-run throughput matches the rate" do
    # Simulate a caller that retries exactly when told to for 10 seconds.
    {count, _} =
      Stream.iterate(0, & &1)
      |> Enum.reduce_while({0, {RL.new(50, 5, 0), 0}}, fn _, {n, {b, now}} ->
        if now > 10_000 do
          {:halt, {n, b}}
        else
          case RL.take(b, now) do
            {:ok, b} -> {:cont, {n + 1, {b, now}}}
            {:wait, ms, b} -> {:cont, {n, {b, now + ms}}}
          end
        end
      end)

    # 5 burst tokens plus 50/s for 10s.
    assert count in 500..506
  end

  test "validates arguments" do
    assert_raise ArgumentError, fn -> RL.new(0, 1, 0) end
    assert_raise ArgumentError, fn -> RL.new(1, 0, 0) end
    assert_raise ArgumentError, fn -> RL.take(RL.new(1, 2, 0), 0, 3) end
  end
end
