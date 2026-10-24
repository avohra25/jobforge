defmodule Jobforge.Queue do
  @moduledoc """
  A queue server: one GenServer that owns a set of jobs, a store, and a
  `Task.Supervisor` that runs job attempts in isolated processes.

  ## Options

    * `:name` - optional registered name
    * `:store` - `{module, opts}` implementing `Jobforge.Store`
      (default `{Jobforge.Store.Memory, []}`)
    * `:concurrency` - maximum attempts running at once (default `10`)
    * `:rate_limit` - `{per_second, burst}` cap on attempt starts (default none)
    * `:backoff` - a `Jobforge.Backoff` policy (default `Jobforge.Backoff.default/0`)
    * `:timeout` - default per-attempt timeout in ms (default `:infinity`)
    * `:on_event` - `fn event, job, meta -> any end`, called from the queue
      process for `:inserted`, `:started`, `:completed`, `:failed`,
      `:discarded`, `:snoozed` and `:cancelled`. Keep it fast.
    * `:paused` - start without running jobs (default `false`)

  ## Guarantees

  Every state change is written to the store before the queue acts on it.
  If the queue stops while an attempt is executing, the job is reloaded as
  `:available` on the next start and runs again, so delivery is
  at-least-once. Completed jobs are deleted from the store; discarded and
  cancelled jobs are kept until `delete/2` so they can be inspected or retried.
  """

  use GenServer
  require Logger

  alias Jobforge.{Backoff, Job, PQueue, RateLimiter, Worker}

  defstruct [
    :store_mod,
    :store,
    :task_sup,
    :limiter,
    :backoff,
    :default_timeout,
    :on_event,
    :timer,
    jobs: %{},
    ready: PQueue.new(),
    scheduled: PQueue.new(),
    running: %{},
    unique: %{},
    concurrency: 10,
    paused: false,
    waiters: [],
    counts: %{completed: 0, failed: 0, discarded: 0, cancelled: 0, snoozed: 0}
  ]

  @type queue :: GenServer.server()

  ## Client API

  @doc "Starts a queue linked to the caller. See the module docs for options."
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    {gen_opts, opts} = Keyword.split(opts, [:name])
    GenServer.start_link(__MODULE__, opts, gen_opts)
  end

  @doc """
  Inserts a job for `worker` with `args`. Worker defaults from
  `use Jobforge.Worker` apply first, then `opts` (see `Jobforge.Job.new/3`).

  Returns `{:error, {:duplicate, id}}` when a `:unique` key is already taken
  by a job that hasn't finished.
  """
  @spec insert(queue(), module(), term(), keyword()) :: {:ok, Job.t()} | {:error, term()}
  def insert(queue, worker, args, opts \\ []) do
    with {:ok, job} <- Job.new(worker, args, Keyword.merge(Worker.defaults(worker), opts)) do
      insert_job(queue, job)
    end
  end

  @doc "Inserts a job built with `Jobforge.Job.new/3` or `MyWorker.new/2`."
  @spec insert_job(queue(), Job.t()) :: {:ok, Job.t()} | {:error, term()}
  def insert_job(queue, %Job{} = job) do
    if Worker.valid?(job.worker),
      do: GenServer.call(queue, {:insert, job}),
      else: {:error, {:invalid_worker, job.worker}}
  end

  @doc "Fetches a job that is still known to the queue (completed jobs are forgotten)."
  @spec get(queue(), String.t()) :: {:ok, Job.t()} | {:error, :not_found}
  def get(queue, id), do: GenServer.call(queue, {:get, id})

  @doc "Lists jobs, optionally only those in `state`, oldest first."
  @spec list(queue(), Job.state() | nil) :: [Job.t()]
  def list(queue, state \\ nil), do: GenServer.call(queue, {:list, state})

  @doc "Cancels a waiting job, or kills a running attempt and cancels it."
  @spec cancel(queue(), String.t()) :: {:ok, Job.t()} | {:error, :not_found | :already_finished}
  def cancel(queue, id), do: GenServer.call(queue, {:cancel, id})

  @doc "Re-enqueues a discarded or cancelled job with a fresh set of attempts."
  @spec retry(queue(), String.t()) :: {:ok, Job.t()} | {:error, term()}
  def retry(queue, id), do: GenServer.call(queue, {:retry, id})

  @doc "Deletes a finished (discarded or cancelled) job from the queue and store."
  @spec delete(queue(), String.t()) :: :ok | {:error, :not_found | :not_finished}
  def delete(queue, id), do: GenServer.call(queue, {:delete, id})

  @doc "Stops starting new attempts. Running attempts finish normally."
  @spec pause(queue()) :: :ok
  def pause(queue), do: GenServer.call(queue, :pause)

  @doc "Resumes starting attempts."
  @spec resume(queue()) :: :ok
  def resume(queue), do: GenServer.call(queue, :resume)

  @doc "Counts by state plus lifetime counters."
  @spec stats(queue()) :: map()
  def stats(queue), do: GenServer.call(queue, :stats)

  @doc """
  Blocks until no job is available, scheduled or executing, or `timeout`
  elapses (then exits like any `GenServer.call/3`). Handy in tests and scripts.
  """
  @spec drain(queue(), timeout()) :: :ok
  def drain(queue, timeout \\ 5_000), do: GenServer.call(queue, :drain, timeout)

  @doc "Stops the queue, closing its store."
  @spec stop(queue()) :: :ok
  def stop(queue), do: GenServer.stop(queue)

  ## Server

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)
    {store_mod, store_opts} = Keyword.get(opts, :store, {Jobforge.Store.Memory, []})

    with {:ok, store} <- store_mod.init(store_opts),
         {:ok, sup} <- Task.Supervisor.start_link() do
      now = now()

      limiter =
        case Keyword.get(opts, :rate_limit) do
          nil -> nil
          {rate, burst} -> RateLimiter.new(rate, burst, now)
        end

      s = %__MODULE__{
        store_mod: store_mod,
        store: store,
        task_sup: sup,
        limiter: limiter,
        backoff: Keyword.get(opts, :backoff, Backoff.default()),
        default_timeout: Keyword.get(opts, :timeout, :infinity),
        on_event: Keyword.get(opts, :on_event),
        concurrency: Keyword.get(opts, :concurrency, 10),
        paused: Keyword.get(opts, :paused, false)
      }

      s = store_mod.all(store) |> Enum.reduce(s, &recover(&2, &1, now))
      {:ok, s, {:continue, :dispatch}}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  # An attempt that was executing when the queue went down never reported
  # back. Hand it out again (at-least-once) and note why.
  defp recover(s, %Job{state: :executing} = job, now) do
    job =
      job |> Job.record_error(:interrupted, now) |> Map.merge(%{state: :available, run_at: now})

    s |> put_job(job) |> enqueue(job)
  end

  defp recover(s, job, _now), do: s |> track(job) |> enqueue(job)

  @impl true
  def handle_continue(:dispatch, s), do: {:noreply, dispatch(s)}

  @impl true
  def handle_call({:insert, %Job{} = job}, _from, s) do
    case Map.fetch(s.unique, job.unique) do
      {:ok, existing} when not is_nil(job.unique) ->
        {:reply, {:error, {:duplicate, existing}}, s}

      _ ->
        if Map.has_key?(s.jobs, job.id) do
          {:reply, {:error, {:duplicate_id, job.id}}, s}
        else
          s = s |> put_job(job) |> enqueue(job)
          emit(s, :inserted, job, %{})
          {:reply, {:ok, job}, dispatch(s)}
        end
    end
  end

  def handle_call({:get, id}, _from, s) do
    {:reply,
     with({:ok, job} <- Map.fetch(s.jobs, id), do: {:ok, job}, else: (_ -> {:error, :not_found})),
     s}
  end

  def handle_call({:list, state}, _from, s) do
    jobs = s.jobs |> Map.values() |> Enum.filter(&(is_nil(state) or &1.state == state))
    {:reply, Jobforge.Store.sort(jobs), s}
  end

  def handle_call({:cancel, id}, _from, s) do
    case Map.fetch(s.jobs, id) do
      :error ->
        {:reply, {:error, :not_found}, s}

      {:ok, job} ->
        cond do
          Job.terminal?(job) ->
            {:reply, {:error, :already_finished}, s}

          job.state == :executing ->
            {ref, {_id, pid, timer, _started}} =
              Enum.find(s.running, fn {_, {jid, _, _, _}} -> jid == id end)

            cancel_timer(timer)
            _ = Task.Supervisor.terminate_child(s.task_sup, pid)
            Process.demonitor(ref, [:flush])
            s = %{s | running: Map.delete(s.running, ref)}
            {s, job} = mark_cancelled(s, job, :cancelled_by_caller)
            {:reply, {:ok, job}, dispatch(s)}

          true ->
            # Heap entries for this job are now stale and get skipped on pop.
            {s, job} = mark_cancelled(s, job, :cancelled_by_caller)
            {:reply, {:ok, job}, dispatch(s)}
        end
    end
  end

  def handle_call({:retry, id}, _from, s) do
    with {:ok, job} <- fetch(s, id),
         true <- job.state in [:discarded, :cancelled] || {:error, :not_finished},
         :ok <- unique_free(s, job) do
      job = %{job | state: :available, attempt: 0, run_at: now()}
      s = s |> put_job(job) |> enqueue(job)
      {:reply, {:ok, job}, dispatch(s)}
    else
      {:error, _} = e -> {:reply, e, s}
    end
  end

  def handle_call({:delete, id}, _from, s) do
    with {:ok, job} <- fetch(s, id),
         true <- job.state in [:discarded, :cancelled] || {:error, :not_finished} do
      {:reply, :ok, forget(s, job)}
    else
      {:error, _} = e -> {:reply, e, s}
    end
  end

  def handle_call(:pause, _from, s), do: {:reply, :ok, %{s | paused: true}}
  def handle_call(:resume, _from, s), do: {:reply, :ok, dispatch(%{s | paused: false})}

  def handle_call(:stats, _from, s) do
    by_state = Map.new(Job.states() -- [:completed], &{&1, 0})

    by_state =
      Enum.reduce(Map.values(s.jobs), by_state, fn j, acc ->
        Map.update!(acc, j.state, &(&1 + 1))
      end)

    stats =
      by_state
      |> Map.merge(%{
        completed_total: s.counts.completed,
        failed_attempts: s.counts.failed,
        discarded_total: s.counts.discarded,
        cancelled_total: s.counts.cancelled,
        snoozed_total: s.counts.snoozed,
        concurrency: s.concurrency,
        paused: s.paused
      })

    {:reply, stats, s}
  end

  def handle_call(:drain, from, s) do
    if idle?(s), do: {:reply, :ok, s}, else: {:noreply, %{s | waiters: [from | s.waiters]}}
  end

  @impl true
  def handle_info({ref, result}, s) when is_reference(ref) and is_map_key(s.running, ref) do
    Process.demonitor(ref, [:flush])
    {:noreply, finish(s, ref, result)}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, s) when is_map_key(s.running, ref) do
    {:noreply, finish(s, ref, {:error, {:exit, Exception.format_exit(reason)}})}
  end

  def handle_info({:attempt_timeout, ref, ms}, s) when is_map_key(s.running, ref) do
    {_id, pid, _timer, _started} = Map.fetch!(s.running, ref)
    _ = Task.Supervisor.terminate_child(s.task_sup, pid)
    Process.demonitor(ref, [:flush])
    {:noreply, finish(s, ref, {:error, {:timeout, ms}})}
  end

  def handle_info({:tick, token}, %{timer: {token, _, _}} = s),
    do: {:noreply, dispatch(%{s | timer: nil})}

  def handle_info({:EXIT, pid, reason}, %{task_sup: pid} = s), do: {:stop, reason, s}

  # Late results of cancelled/timed-out attempts, stale timer ticks, etc.
  def handle_info(_msg, s), do: {:noreply, s}

  @impl true
  def terminate(_reason, s) do
    s.store_mod.close(s.store)
  end

  ## Dispatch

  defp dispatch(s) do
    now = now()
    s |> promote_due(now) |> start_ready(now) |> arm_timer(now) |> notify_waiters()
  end

  # Move scheduled jobs whose time has come into the ready heap.
  defp promote_due(s, now) do
    case PQueue.peek(s.scheduled) do
      {:ok, {run_at, id}} when run_at <= now ->
        {:ok, _, rest} = PQueue.pop(s.scheduled)
        s = %{s | scheduled: rest}

        case Map.fetch(s.jobs, id) do
          {:ok, %Job{state: :scheduled, run_at: ^run_at} = job} ->
            job = %{job | state: :available}
            s |> put_job(job) |> enqueue(job) |> promote_due(now)

          _stale ->
            promote_due(s, now)
        end

      _ ->
        s
    end
  end

  defp start_ready(%{paused: true} = s, _now), do: s
  defp start_ready(s, _now) when map_size(s.running) >= s.concurrency, do: s

  defp start_ready(s, now) do
    with {:ok, {_key, id}} <- PQueue.peek(s.ready) do
      case Map.fetch(s.jobs, id) do
        {:ok, %Job{state: :available} = job} ->
          case take_token(s, now) do
            {:ok, s} ->
              {:ok, _, rest} = PQueue.pop(s.ready)
              %{s | ready: rest} |> start_attempt(job, now) |> start_ready(now)

            {:wait, s} ->
              s
          end

        _stale ->
          {:ok, _, rest} = PQueue.pop(s.ready)
          start_ready(%{s | ready: rest}, now)
      end
    else
      :empty -> s
    end
  end

  defp take_token(%{limiter: nil} = s, _now), do: {:ok, s}

  defp take_token(s, now) do
    case RateLimiter.take(s.limiter, now) do
      {:ok, l} -> {:ok, %{s | limiter: l}}
      {:wait, _ms, l} -> {:wait, %{s | limiter: l}}
    end
  end

  defp start_attempt(s, job, now) do
    job = %{job | state: :executing, attempt: job.attempt + 1, attempted_at: now}
    s = put_job(s, job)
    task = Task.Supervisor.async_nolink(s.task_sup, Worker, :run, [job])

    timer =
      case job.timeout || s.default_timeout do
        :infinity -> nil
        ms -> Process.send_after(self(), {:attempt_timeout, task.ref, ms}, ms)
      end

    emit(s, :started, job, %{})
    %{s | running: Map.put(s.running, task.ref, {job.id, task.pid, timer, now})}
  end

  defp finish(s, ref, result) do
    {{id, _pid, timer, started}, running} = Map.pop(s.running, ref)
    cancel_timer(timer)
    s = %{s | running: running}
    now = now()
    job = Map.fetch!(s.jobs, id)
    meta = %{duration_ms: now - started}

    s =
      case result do
        :ok ->
          emit(s, :completed, %{job | state: :completed}, meta)
          s |> forget(job) |> bump(:completed)

        {:snooze, ms} ->
          job = %{job | state: :scheduled, run_at: now + ms, attempt: job.attempt - 1}
          emit(s, :snoozed, job, Map.put(meta, :snooze_ms, ms))
          s |> put_job(job) |> enqueue(job) |> bump(:snoozed)

        {:cancel, reason} ->
          {s, _job} = mark_cancelled(s, Job.record_error(job, {:cancelled, reason}, now), nil)
          s

        {:error, reason} ->
          job = Job.record_error(job, reason, now)
          s = bump(s, :failed)

          if Job.retryable?(job) do
            delay = retry_delay(s, job)
            job = %{job | state: :scheduled, run_at: now + delay}
            emit(s, :failed, job, Map.merge(meta, %{error: reason, retry_in_ms: delay}))
            s |> put_job(job) |> enqueue(job)
          else
            job = %{job | state: :discarded}
            emit(s, :discarded, job, Map.put(meta, :error, reason))
            s |> put_job(job) |> bump(:discarded)
          end
      end

    dispatch(s)
  end

  defp retry_delay(s, job) do
    if function_exported?(job.worker, :backoff, 1),
      do: job.worker.backoff(job),
      else: Backoff.compute(s.backoff, job.attempt)
  end

  # Keep a single timer armed for the earliest moment something can change
  # without a message arriving: the next scheduled job or a rate-limit refill.
  defp arm_timer(s, now) do
    next =
      [next_scheduled(s), next_token(s, now)]
      |> Enum.reject(&is_nil/1)
      |> Enum.min(fn -> nil end)

    case {s.timer, next} do
      {nil, nil} ->
        s

      {{_token, at, _ref}, at} ->
        s

      {old, _} ->
        if old, do: cancel_timer(elem(old, 2))

        if next do
          token = make_ref()
          ref = Process.send_after(self(), {:tick, token}, max(next - now, 0))
          %{s | timer: {token, next, ref}}
        else
          %{s | timer: nil}
        end
    end
  end

  defp next_scheduled(s) do
    case PQueue.peek(s.scheduled) do
      {:ok, {run_at, _}} -> run_at
      :empty -> nil
    end
  end

  defp next_token(%{limiter: nil}, _now), do: nil
  defp next_token(%{paused: true}, _now), do: nil

  defp next_token(s, now) do
    if map_size(s.running) < s.concurrency and not PQueue.empty?(s.ready) do
      case RateLimiter.take(s.limiter, now) do
        {:ok, _} -> now
        {:wait, ms, _} -> now + ms
      end
    end
  end

  defp notify_waiters(%{waiters: []} = s), do: s

  defp notify_waiters(s) do
    if idle?(s) do
      Enum.each(s.waiters, &GenServer.reply(&1, :ok))
      %{s | waiters: []}
    else
      s
    end
  end

  defp idle?(s) do
    map_size(s.running) == 0 and
      not Enum.any?(s.jobs, fn {_, j} -> j.state in [:available, :scheduled, :executing] end)
  end

  ## Bookkeeping

  defp enqueue(s, %Job{state: :available} = job),
    do: %{s | ready: PQueue.push(s.ready, job.priority, job.id)}

  defp enqueue(s, %Job{state: :scheduled} = job),
    do: %{s | scheduled: PQueue.push(s.scheduled, job.run_at, job.id)}

  defp enqueue(s, _job), do: s

  defp put_job(s, job) do
    s = %{s | store: s.store_mod.put(s.store, job)}
    track(s, job)
  end

  defp track(s, job) do
    s = %{s | jobs: Map.put(s.jobs, job.id, job)}

    cond do
      is_nil(job.unique) -> s
      Job.terminal?(job) -> release_unique(s, job)
      true -> %{s | unique: Map.put(s.unique, job.unique, job.id)}
    end
  end

  defp forget(s, job) do
    s = %{s | store: s.store_mod.delete(s.store, job), jobs: Map.delete(s.jobs, job.id)}
    release_unique(s, job)
  end

  defp release_unique(s, %Job{unique: nil}), do: s

  defp release_unique(s, %Job{unique: key, id: id}) do
    case s.unique do
      %{^key => ^id} -> %{s | unique: Map.delete(s.unique, key)}
      _ -> s
    end
  end

  defp unique_free(_s, %Job{unique: nil}), do: :ok

  defp unique_free(s, %Job{unique: key}) do
    case Map.fetch(s.unique, key) do
      {:ok, other} -> {:error, {:duplicate, other}}
      :error -> :ok
    end
  end

  defp mark_cancelled(s, job, reason) do
    job = if reason, do: Job.record_error(job, reason, now()), else: job
    job = %{job | state: :cancelled}
    emit(s, :cancelled, job, %{})
    {s |> put_job(job) |> bump(:cancelled), job}
  end

  defp fetch(s, id) do
    case Map.fetch(s.jobs, id) do
      {:ok, job} -> {:ok, job}
      :error -> {:error, :not_found}
    end
  end

  defp bump(s, key), do: %{s | counts: Map.update!(s.counts, key, &(&1 + 1))}

  defp cancel_timer(nil), do: :ok
  defp cancel_timer(ref), do: Process.cancel_timer(ref) && :ok

  defp emit(%{on_event: nil}, _event, _job, _meta), do: :ok

  defp emit(%{on_event: fun}, event, job, meta) do
    fun.(event, job, meta)
  rescue
    e -> Logger.error("jobforge on_event handler crashed: " <> Exception.message(e))
  end

  defp now, do: System.os_time(:millisecond)
end
