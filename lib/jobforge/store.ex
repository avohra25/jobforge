defmodule Jobforge.Store do
  @moduledoc """
  Behaviour for where a queue keeps its jobs.

  The queue server owns the store state and calls these functions from its
  own process, so implementations don't need to be concurrency-safe. Every
  state change of a job is written through `put/2` before the queue acts on
  it, which makes the store the source of truth after a restart.

  Two implementations ship with Jobforge:

    * `Jobforge.Store.Memory` - a map; fast, gone when the queue stops
    * `Jobforge.Store.DETS` - a disk table; jobs survive restarts and crashes
  """

  alias Jobforge.Job

  @type state :: term()

  @callback init(keyword()) :: {:ok, state()} | {:error, term()}
  @callback put(state(), Job.t()) :: state()
  @callback delete(state(), Job.t() | String.t()) :: state()
  @callback all(state()) :: [Job.t()]
  @callback close(state()) :: :ok

  @doc "Sorts jobs by insertion time, then id, so reloads are deterministic."
  @spec sort([Job.t()]) :: [Job.t()]
  def sort(jobs), do: Enum.sort_by(jobs, &{&1.inserted_at, &1.id})

  @doc false
  def id(%Job{id: id}), do: id
  def id(id) when is_binary(id), do: id
end
