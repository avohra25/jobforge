defmodule Jobforge.Job do
  @moduledoc """
  A unit of work: which worker to run, with what arguments, and its lifecycle.

  States:

    * `:available` - ready to run now
    * `:scheduled` - waiting for `run_at` (new delayed job or a retry in backoff)
    * `:executing` - currently running
    * `:completed` - finished successfully (terminal)
    * `:discarded` - ran out of attempts (terminal, kept as a dead letter)
    * `:cancelled` - cancelled by the caller or by the worker (terminal)

  Times are wall-clock milliseconds (`System.os_time(:millisecond)`) so they
  stay meaningful after a restart when jobs are reloaded from a store.
  """

  @states [:available, :scheduled, :executing, :completed, :discarded, :cancelled]
  @terminal [:completed, :discarded, :cancelled]

  @enforce_keys [:id, :worker]
  defstruct [
    :id,
    :worker,
    :run_at,
    :inserted_at,
    :attempted_at,
    :unique,
    :timeout,
    args: nil,
    queue: "default",
    priority: 0,
    attempt: 0,
    max_attempts: 5,
    state: :available,
    errors: [],
    meta: %{}
  ]

  @type state :: :available | :scheduled | :executing | :completed | :discarded | :cancelled
  @type error_entry :: %{attempt: non_neg_integer(), at: integer(), error: term()}
  @type t :: %__MODULE__{
          id: String.t(),
          worker: module(),
          args: term(),
          queue: String.t(),
          priority: integer(),
          attempt: non_neg_integer(),
          max_attempts: pos_integer(),
          run_at: integer(),
          inserted_at: integer(),
          attempted_at: integer() | nil,
          state: state(),
          errors: [error_entry()],
          unique: term(),
          timeout: pos_integer() | :infinity | nil,
          meta: map()
        }

  @allowed_opts [
    :id,
    :queue,
    :priority,
    :max_attempts,
    :run_at,
    :schedule_in,
    :unique,
    :timeout,
    :meta,
    :now
  ]

  @doc "All job states."
  def states, do: @states

  @doc """
  Builds a new job.

  Options:

    * `:priority` - integer, lower runs first (default `0`)
    * `:max_attempts` - total executions before discarding (default `5`)
    * `:schedule_in` - delay in ms, or `:run_at` - absolute ms timestamp
    * `:unique` - a key; at most one non-terminal job per key may exist in a queue.
      Pass `:args` to use `{worker, args}` as the key.
    * `:timeout` - per-attempt timeout in ms (`nil` uses the queue default)
    * `:queue`, `:meta`, `:id` - labels, free-form metadata, explicit id
  """
  @spec new(module(), term(), keyword()) :: {:ok, t()} | {:error, term()}
  def new(worker, args, opts \\ []) do
    with :ok <- check_opts(opts),
         :ok <- check(is_atom(worker) and not is_nil(worker), {:invalid_worker, worker}),
         priority = Keyword.get(opts, :priority, 0),
         :ok <- check(is_integer(priority), {:invalid_priority, priority}),
         max_attempts = Keyword.get(opts, :max_attempts, 5),
         :ok <-
           check(
             is_integer(max_attempts) and max_attempts > 0,
             {:invalid_max_attempts, max_attempts}
           ),
         timeout = Keyword.get(opts, :timeout),
         :ok <- check(valid_timeout?(timeout), {:invalid_timeout, timeout}),
         now = Keyword.get_lazy(opts, :now, fn -> System.os_time(:millisecond) end),
         {:ok, run_at} <- run_at(opts, now) do
      unique =
        case Keyword.get(opts, :unique) do
          :args -> {worker, args}
          key -> key
        end

      {:ok,
       %__MODULE__{
         id: Keyword.get_lazy(opts, :id, &generate_id/0),
         worker: worker,
         args: args,
         queue: to_string(Keyword.get(opts, :queue, "default")),
         priority: priority,
         max_attempts: max_attempts,
         run_at: run_at,
         inserted_at: now,
         state: if(run_at > now, do: :scheduled, else: :available),
         unique: unique,
         timeout: timeout,
         meta: Map.new(Keyword.get(opts, :meta, %{}))
       }}
    end
  end

  @doc "Like `new/3` but raises on invalid options."
  @spec new!(module(), term(), keyword()) :: t()
  def new!(worker, args, opts \\ []) do
    case new(worker, args, opts) do
      {:ok, job} -> job
      {:error, reason} -> raise ArgumentError, "invalid job: #{inspect(reason)}"
    end
  end

  @doc "True for `:completed`, `:discarded` and `:cancelled`."
  @spec terminal?(t()) :: boolean()
  def terminal?(%__MODULE__{state: state}), do: state in @terminal

  @doc "True when the job is waiting and its `run_at` has passed."
  @spec due?(t(), integer()) :: boolean()
  def due?(%__MODULE__{state: state, run_at: run_at}, now),
    do: state in [:available, :scheduled] and run_at <= now

  @doc "True when another attempt is allowed after the current one."
  @spec retryable?(t()) :: boolean()
  def retryable?(%__MODULE__{attempt: a, max_attempts: m}), do: a < m

  @doc "Prepends an error for the current attempt (newest first)."
  @spec record_error(t(), term(), integer()) :: t()
  def record_error(%__MODULE__{} = job, error, now) do
    %{job | errors: [%{attempt: job.attempt, at: now, error: error} | job.errors]}
  end

  @doc "Generates a random 16-character hex id."
  @spec generate_id() :: String.t()
  def generate_id, do: :crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower)

  defp check_opts(opts) do
    case Keyword.keys(opts) -- @allowed_opts do
      [] -> :ok
      unknown -> {:error, {:unknown_options, unknown}}
    end
  end

  defp check(true, _reason), do: :ok
  defp check(false, reason), do: {:error, reason}

  defp valid_timeout?(t), do: is_nil(t) or t == :infinity or (is_integer(t) and t > 0)

  defp run_at(opts, now) do
    case {Keyword.get(opts, :run_at), Keyword.get(opts, :schedule_in)} do
      {nil, nil} -> {:ok, now}
      {nil, ms} when is_integer(ms) and ms >= 0 -> {:ok, now + ms}
      {at, nil} when is_integer(at) -> {:ok, at}
      {nil, ms} -> {:error, {:invalid_schedule_in, ms}}
      {at, nil} -> {:error, {:invalid_run_at, at}}
      _ -> {:error, :run_at_and_schedule_in_are_exclusive}
    end
  end
end
