defmodule Jobforge.Store.Memory do
  @moduledoc "In-memory job store. Nothing survives a restart."
  @behaviour Jobforge.Store

  alias Jobforge.Store

  @impl true
  def init(_opts), do: {:ok, %{}}

  @impl true
  def put(jobs, job), do: Map.put(jobs, job.id, job)

  @impl true
  def delete(jobs, job_or_id), do: Map.delete(jobs, Store.id(job_or_id))

  @impl true
  def all(jobs), do: jobs |> Map.values() |> Store.sort()

  @impl true
  def close(_jobs), do: :ok
end
