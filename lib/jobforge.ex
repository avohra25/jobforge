defmodule Jobforge do
  @moduledoc """
  A small, dependency-free background job queue built on OTP.

      defmodule Greeter do
        use Jobforge.Worker, max_attempts: 3

        @impl true
        def perform(%Jobforge.Job{args: %{"name" => name}}) do
          IO.puts("hello " <> name)
          :ok
        end
      end

      {:ok, q} = Jobforge.start_link(concurrency: 5)
      {:ok, _job} = Jobforge.insert(q, Greeter, %{"name" => "Ada"})
      :ok = Jobforge.drain(q)

  See `Jobforge.Queue` for options and `Jobforge.Worker` for the worker contract.
  """

  alias Jobforge.Queue

  defdelegate start_link(opts \\ []), to: Queue
  defdelegate insert(queue, worker, args, opts \\ []), to: Queue
  defdelegate insert_job(queue, job), to: Queue
  defdelegate get(queue, id), to: Queue
  defdelegate list(queue, state \\ nil), to: Queue
  defdelegate cancel(queue, id), to: Queue
  defdelegate retry(queue, id), to: Queue
  defdelegate delete(queue, id), to: Queue
  defdelegate pause(queue), to: Queue
  defdelegate resume(queue), to: Queue
  defdelegate stats(queue), to: Queue
  defdelegate drain(queue, timeout \\ 5_000), to: Queue
  defdelegate stop(queue), to: Queue

  @doc "Child spec so a queue can sit directly in a supervision tree."
  def child_spec(opts), do: Queue.child_spec(opts)
end
