defmodule Jobforge.Worker do
  @moduledoc """
  Behaviour for modules that perform jobs.

      defmodule MyApp.SendEmail do
        use Jobforge.Worker, max_attempts: 3, priority: 1, timeout: 10_000

        @impl true
        def perform(%Jobforge.Job{args: %{"to" => to}}) do
          case Mailer.deliver(to) do
            :ok -> :ok
            {:error, :rate_limited} -> {:snooze, 5_000}
            {:error, :bad_address} -> {:cancel, :bad_address}
            {:error, reason} -> {:error, reason}
          end
        end
      end

  `perform/1` return values:

    * `:ok` or `{:ok, value}` - the job is completed
    * `{:error, reason}` - the attempt failed; retried with backoff until
      `max_attempts` is reached, then discarded
    * `{:snooze, ms}` - run again in `ms` without using up an attempt
    * `{:cancel, reason}` - stop for good without retrying

  Raising or exiting counts as `{:error, formatted_exception}`.

  Implement the optional `backoff/1` callback to override the queue's retry
  policy for this worker; it receives the job after the failed attempt.

  `use Jobforge.Worker` accepts default job options (`:priority`,
  `:max_attempts`, `:timeout`, `:queue`, `:unique`, `:meta`). They apply when
  the worker is inserted, and per-insert options override them.
  """

  alias Jobforge.Job

  @type result ::
          :ok
          | {:ok, term()}
          | {:error, term()}
          | {:snooze, non_neg_integer()}
          | {:cancel, term()}

  @callback perform(Job.t()) :: result()
  @callback backoff(Job.t()) :: non_neg_integer()
  @optional_callbacks backoff: 1

  @default_keys [:priority, :max_attempts, :timeout, :queue, :unique, :meta]

  defmacro __using__(opts) do
    case Keyword.keys(opts) -- @default_keys do
      [] -> :ok
      bad -> raise ArgumentError, "unsupported Jobforge.Worker options: #{inspect(bad)}"
    end

    quote do
      @behaviour Jobforge.Worker

      @doc false
      def __jobforge_defaults__, do: unquote(opts)

      @doc "Builds a job for this worker using its defaults merged with `opts`."
      def new(args, opts \\ []),
        do: Jobforge.Job.new(__MODULE__, args, Keyword.merge(__jobforge_defaults__(), opts))
    end
  end

  @doc "The default options declared by `use Jobforge.Worker`, or `[]`."
  @spec defaults(module()) :: keyword()
  def defaults(worker) when is_atom(worker) do
    if Code.ensure_loaded?(worker) and function_exported?(worker, :__jobforge_defaults__, 0),
      do: worker.__jobforge_defaults__(),
      else: []
  end

  @doc "True when `worker` is a loaded module exporting `perform/1`."
  @spec valid?(module()) :: boolean()
  def valid?(worker) when is_atom(worker),
    do: Code.ensure_loaded?(worker) and function_exported?(worker, :perform, 1)

  def valid?(_), do: false

  @doc """
  Runs `worker.perform(job)` and normalises the result, turning exceptions,
  throws and exits into `{:error, message}`.
  """
  @spec run(Job.t()) :: :ok | {:error, term()} | {:snooze, non_neg_integer()} | {:cancel, term()}
  def run(%Job{worker: worker} = job) do
    try do
      normalize(worker.perform(job))
    rescue
      e -> {:error, Exception.format(:error, e, __STACKTRACE__) |> first_line()}
    catch
      kind, reason -> {:error, Exception.format(kind, reason, __STACKTRACE__) |> first_line()}
    end
  end

  defp normalize(:ok), do: :ok
  defp normalize({:ok, _}), do: :ok
  defp normalize({:error, _} = e), do: e
  defp normalize({:snooze, ms} = s) when is_integer(ms) and ms >= 0, do: s
  defp normalize({:cancel, _} = c), do: c
  defp normalize(other), do: {:error, {:bad_return, other}}

  defp first_line(text), do: text |> String.split("\n", parts: 2) |> hd() |> String.trim()
end
