defmodule Jobforge.Store.DETS do
  @moduledoc """
  Disk-backed job store using Erlang's DETS.

  Options:

    * `:path` (required) - file to store jobs in; parent directories are created
    * `:sync` - flush to disk after every write (default `true`). With `false`
      DETS still writes through its own buffer, but a hard crash may lose the
      last few changes.

  DETS repairs its file on open after an unclean shutdown, so a queue that was
  killed mid-write reloads every job that was fully written.
  """
  @behaviour Jobforge.Store

  alias Jobforge.{Job, Store}

  defstruct [:table, :path, sync: true]

  @impl true
  def init(opts) do
    path = opts |> Keyword.fetch!(:path) |> Path.expand()
    File.mkdir_p!(Path.dirname(path))

    case :dets.open_file(make_ref(), file: String.to_charlist(path), type: :set, repair: true) do
      {:ok, table} ->
        {:ok, %__MODULE__{table: table, path: path, sync: Keyword.get(opts, :sync, true)}}

      {:error, reason} ->
        {:error, {:dets_open_failed, path, reason}}
    end
  end

  @impl true
  def put(%__MODULE__{table: t} = s, %Job{} = job) do
    :ok = :dets.insert(t, {job.id, job})
    maybe_sync(s)
  end

  @impl true
  def delete(%__MODULE__{table: t} = s, job_or_id) do
    :ok = :dets.delete(t, Store.id(job_or_id))
    maybe_sync(s)
  end

  @impl true
  def all(%__MODULE__{table: t}) do
    fn {_id, %Job{} = job}, acc -> [job | acc] end
    |> :dets.foldl([], t)
    |> Store.sort()
  end

  @impl true
  def close(%__MODULE__{table: t}) do
    _ = :dets.close(t)
    :ok
  end

  defp maybe_sync(%__MODULE__{sync: false} = s), do: s

  defp maybe_sync(%__MODULE__{table: t} = s) do
    :ok = :dets.sync(t)
    s
  end
end
