defmodule Jobforge.Backoff do
  @moduledoc """
  Retry delay policies.

  A policy is one of:

    * `{:exponential, opts}` - `base * 2^(attempt - 1)`, capped at `:max`
    * `{:linear, opts}` - `base * attempt`, capped at `:max`
    * `{:constant, ms}` - always `ms`
    * a 1-arity function receiving the attempt number and returning milliseconds

  Exponential and linear policies accept `:base` (default 1000 ms), `:max`
  (default 300_000 ms) and `:jitter`, a fraction in `0.0..1.0` (default 0.1
  for exponential, 0.0 for linear). Jitter spreads the delay uniformly over
  `delay * (1 ± jitter)` so that a burst of failures doesn't retry in lockstep.
  """

  @type policy ::
          {:exponential, keyword()}
          | {:linear, keyword()}
          | {:constant, non_neg_integer()}
          | (pos_integer() -> non_neg_integer())

  @doc "The policy used when a queue does not configure one."
  @spec default() :: policy()
  def default, do: {:exponential, base: 1_000, max: 300_000, jitter: 0.1}

  @doc """
  Computes the delay in milliseconds before retry number `attempt` (1-based).

  `rand` returns a float in `[0, 1)`; it is injectable for deterministic tests.
  """
  @spec compute(policy(), pos_integer(), (-> float())) :: non_neg_integer()
  def compute(policy, attempt, rand \\ &:rand.uniform/0)

  def compute(_policy, attempt, _rand) when not is_integer(attempt) or attempt < 1 do
    raise ArgumentError, "attempt must be a positive integer, got: #{inspect(attempt)}"
  end

  def compute({:exponential, opts}, attempt, rand) do
    base = Keyword.get(opts, :base, 1_000)
    # Cap the exponent so huge attempt counts can't build enormous integers.
    raw = base * Integer.pow(2, min(attempt - 1, 62))
    raw |> min(Keyword.get(opts, :max, 300_000)) |> jitter(Keyword.get(opts, :jitter, 0.1), rand)
  end

  def compute({:linear, opts}, attempt, rand) do
    base = Keyword.get(opts, :base, 1_000)

    (base * attempt)
    |> min(Keyword.get(opts, :max, 300_000))
    |> jitter(Keyword.get(opts, :jitter, 0.0), rand)
  end

  def compute({:constant, ms}, _attempt, _rand) when is_integer(ms) and ms >= 0, do: ms

  def compute(fun, attempt, _rand) when is_function(fun, 1) do
    case fun.(attempt) do
      ms when is_integer(ms) and ms >= 0 -> ms
      other -> raise ArgumentError, "backoff function returned #{inspect(other)}"
    end
  end

  def compute(policy, _attempt, _rand) do
    raise ArgumentError, "unknown backoff policy: #{inspect(policy)}"
  end

  defp jitter(delay, j, _rand) when j == 0, do: delay

  defp jitter(delay, j, rand) when is_number(j) and j > 0 and j <= 1 do
    max(0, round(delay * (1 + j * (2 * rand.() - 1))))
  end

  defp jitter(_delay, j, _rand),
    do: raise(ArgumentError, "jitter must be in 0.0..1.0, got: #{inspect(j)}")
end
