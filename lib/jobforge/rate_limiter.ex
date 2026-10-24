defmodule Jobforge.RateLimiter do
  @moduledoc """
  A pure token-bucket rate limiter.

  The bucket holds up to `burst` tokens and refills continuously at `rate`
  tokens per second. Each job start takes one token. When the bucket is empty,
  `take/3` says how long to wait instead of blocking, so the queue server can
  set a timer and keep handling messages.

  All functions take the current time in milliseconds explicitly, which keeps
  the limiter deterministic and easy to test.
  """

  @enforce_keys [:rate, :burst, :tokens, :updated_at]
  defstruct [:rate, :burst, :tokens, :updated_at]

  @type t :: %__MODULE__{
          rate: number(),
          burst: pos_integer(),
          tokens: float(),
          updated_at: integer()
        }

  @doc "A full bucket refilling at `rate` tokens/second with capacity `burst`."
  @spec new(number(), pos_integer(), integer()) :: t()
  def new(rate, burst, now) do
    unless is_number(rate) and rate > 0,
      do: raise(ArgumentError, "rate must be positive, got: #{inspect(rate)}")

    unless is_integer(burst) and burst >= 1,
      do: raise(ArgumentError, "burst must be a positive integer, got: #{inspect(burst)}")

    %__MODULE__{rate: rate, burst: burst, tokens: burst * 1.0, updated_at: now}
  end

  @doc """
  Tries to take `n` tokens at time `now`.

  Returns `{:ok, bucket}` or `{:wait, ms, bucket}` where `ms` is the shortest
  wait after which the same request would succeed.
  """
  @spec take(t(), integer(), pos_integer()) :: {:ok, t()} | {:wait, pos_integer(), t()}
  def take(bucket, now, n \\ 1)

  def take(%__MODULE__{burst: burst}, _now, n) when n > burst do
    raise ArgumentError, "cannot take #{n} tokens from a bucket of size #{burst}"
  end

  def take(%__MODULE__{} = bucket, now, n) do
    bucket = refill(bucket, now)

    if bucket.tokens >= n do
      {:ok, %{bucket | tokens: bucket.tokens - n}}
    else
      wait = ceil((n - bucket.tokens) * 1_000 / bucket.rate)
      {:wait, max(wait, 1), bucket}
    end
  end

  @doc "Tokens available at `now` (fractional)."
  @spec available(t(), integer()) :: float()
  def available(bucket, now), do: refill(bucket, now).tokens

  defp refill(%__MODULE__{updated_at: last} = b, now) when now <= last, do: b

  defp refill(%__MODULE__{} = b, now) do
    tokens = min(b.burst * 1.0, b.tokens + (now - b.updated_at) * b.rate / 1_000)
    %{b | tokens: tokens, updated_at: now}
  end
end
