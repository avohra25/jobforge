# jobforge

A small, dependency-free background job queue for Elixir, built on plain OTP
(`GenServer`, `Task.Supervisor` and `:dets`).

```elixir
defmodule Greeter do
  use Jobforge.Worker, max_attempts: 3

  @impl true
  def perform(%Jobforge.Job{args: %{"name" => name}}) do
    IO.puts("hello " <> name)
    :ok
  end
end

{:ok, q} = Jobforge.start_link(concurrency: 5, store: {Jobforge.Store.DETS, path: "jobs.dets"})
{:ok, _job} = Jobforge.insert(q, Greeter, %{"name" => "Ada"}, priority: 1, schedule_in: 1_000)
:ok = Jobforge.drain(q)
```

## Features

- **Priorities** with FIFO ordering inside a priority (lower number runs first)
- **Delayed jobs** via `schedule_in:` (ms) or `run_at:` (a `DateTime`)
- **Bounded concurrency**; every attempt runs in its own supervised process
- **Retries with backoff**: exponential, linear, constant or a custom function, with jitter
- **Per-attempt timeouts** that kill the attempt and count it as a failure
- **Snooze and cancel** from inside a job, and `cancel/2` / `retry/2` from outside
- **Unique jobs** (`unique: :args` or any key) so duplicates aren't enqueued
- **Rate limiting** with a token bucket on attempt starts
- **Persistence**: an in-memory store, or a DETS store that survives restarts
- **Dead letters**: discarded and cancelled jobs stay around to inspect or retry
- **Dependencies**: `depends_on:` holds a job until other jobs complete, and cancels it if one fails
- **Recurring jobs** from a crontab (cron expressions, optional seconds), without overlap by default
- **Events** through an `on_event` callback, plus `stats/1` counters

## Worker return values

| `perform/1` returns | Result |
| --- | --- |
| `:ok` or `{:ok, value}` | completed and removed from the store |
| `{:error, reason}` | retried with backoff until `max_attempts`, then discarded |
| `{:snooze, ms}` | runs again in `ms` without using an attempt |
| `{:cancel, reason}` | stopped for good, no retry |

Raising, throwing or exiting counts as `{:error, formatted_reason}`. A worker
can override the queue's retry policy with the optional `backoff/1` callback.

## Queue options

| Option | Default | Meaning |
| --- | --- | --- |
| `:name` | none | registered name |
| `:store` | `{Jobforge.Store.Memory, []}` | `{module, opts}` implementing `Jobforge.Store` |
| `:concurrency` | `10` | max attempts running at once |
| `:rate_limit` | none | `{per_second, burst}` cap on attempt starts |
| `:backoff` | exponential, 1s base, 5 min cap, 10% jitter | a `Jobforge.Backoff` policy |
| `:timeout` | `:infinity` | default per-attempt timeout in ms |
| `:on_event` | none | `fn event, job, meta -> any end` |
| `:paused` | `false` | start without running jobs |
| `:crontab` | `[]` | recurring entries, `{name, expr, worker, args}` or `{name, expr, worker, args, job_opts}` |

## Dependencies

```elixir
{:ok, fetch} = Jobforge.insert(q, Fetch, %{"url" => url})
{:ok, thumbs} = Jobforge.insert(q, Thumbnail, %{"url" => url}, depends_on: [fetch])
{:ok, index} = Jobforge.insert(q, Index, %{"url" => url}, depends_on: [fetch])
{:ok, _done} = Jobforge.insert(q, Notify, %{"url" => url}, depends_on: [thumbs.id, index.id])
```

A job with `depends_on` (job ids or `%Job{}`s) sits in the `:blocked` state
until every dependency has completed, then becomes available, or scheduled if
its own `run_at` is still ahead. If a dependency is discarded or cancelled,
everything waiting on it is cancelled with `{:dependency_failed, id}`, down the
whole chain. Since completed jobs are forgotten, an id the queue doesn't know
counts as completed, and a job inserted after one of its dependencies already
failed is cancelled right away. `retry/2` on such a job returns
`{:error, {:dependency_failed, id}}` until that dependency is retried to
success or deleted. Blocked jobs are stored like any other and re-linked when a
DETS-backed queue restarts.

## Recurring jobs

Give the queue a crontab, or manage entries at runtime:

```elixir
{:ok, q} =
  Jobforge.start_link(
    crontab: [
      {"nightly-report", "0 3 * * *", ReportWorker, %{"kind" => "daily"}},
      {"heartbeat", "*/30 * * * * *", PingWorker, %{}, priority: -1}
    ]
  )

{:ok, next_at_ms} = Jobforge.schedule_cron(q, "cleanup", "@hourly", CleanupWorker, %{})
Jobforge.crontab(q)          # entries with :next_at, :last_job_id and :skipped
:ok = Jobforge.unschedule_cron(q, "cleanup")
```

Each tick inserts an ordinary job (so retries, timeouts, priorities and rate
limits all apply) tagged with `meta.cron` and `meta.scheduled_for`. An entry
never overlaps itself: while its previous job is still waiting, retrying or
running, the tick is skipped and a `:cron_skipped` event is emitted. Pass your
own `unique:` in the job options (`unique: nil` to allow overlap) to change
that. Missed ticks aren't backfilled; after a pause or restart an entry fires
once to catch up, then follows its schedule. The crontab is configuration, so
it isn't written to the store, and it doesn't keep `drain/2` waiting.

## Cron expressions

`Jobforge.Cron` parses Vixie-style cron expressions and computes fire times,
in UTC:

```elixir
cron = Jobforge.Cron.parse!("*/15 9-17 * * MON-FRI")
Jobforge.Cron.next(cron, ~U[2026-11-13 17:50:00Z])
#=> {:ok, ~U[2026-11-16 09:00:00Z]}
Jobforge.Cron.stream(cron, DateTime.utc_now()) |> Enum.take(3)
```

- Five fields (minute, hour, day of month, month, day of week), or six with a
  leading seconds field (`"*/10 * * * * *"`)
- `*`, values, `a-b` ranges, `*/n`, `a-b/n` and `a/n` steps, comma lists,
  `JAN`-`DEC` and `SUN`-`SAT` names; day of week 0 and 7 are both Sunday
- Vixie's rule for days: if both day of month and day of week are restricted,
  either one matching is enough (`"0 0 1 * MON"` is the 1st *and* Mondays)
- Macros `@yearly`/`@annually`, `@monthly`, `@weekly`, `@daily`/`@midnight`, `@hourly`
- Errors say which field is wrong, e.g. `{:error, {:hour, "5-2", {:reversed_range, 5, 2}}}`;
  schedules that can never fire (`"0 0 30 2 *"`) return `{:error, :never}`

## Guarantees

Every state change is written to the store before the queue acts on it. If the
queue stops while an attempt is running, that job is reloaded as available on
the next start and runs again, so delivery is **at-least-once**: make jobs
idempotent. Completed jobs are deleted; discarded and cancelled jobs are kept
until `Jobforge.delete/2`.

## Design notes

- Ready and scheduled jobs live in two persistent pairing heaps
  (`Jobforge.PQueue`), keyed by `{priority, seq}` and `{run_at, seq}`. Insert is
  O(1) and pop is amortised O(log n).
- The queue keeps a single timer armed for the earliest scheduled job or
  rate-limit refill instead of polling.
- Attempts are started with `Task.Supervisor.async_nolink`, so a crashing job
  never takes the queue down; the queue sees a `:DOWN` and records the error.
- The DETS store syncs after each write by default (`sync: false` trades that
  for speed) and opens with `repair: true`, so a half-written file is fixed on
  restart.

## Try it

```sh
mix jobforge.demo --jobs 12 --concurrency 3 --fail-rate 0.3
mix run examples/pipeline.exs
```

The demo prints the event stream for a mix of flaky, slow and scheduled jobs.
The pipeline example chains fetch and parse jobs on a DETS store; stop it
partway through and rerun it to watch unfinished jobs resume.

## Tests

```sh
mix test
```

85 tests, including a randomized stress test that throws successes, failures,
snoozes, cancels, crashes and delays at a queue and checks that every job
ends in exactly one terminal state without ever exceeding the concurrency limit,
random dependency graphs with random outcomes (each job runs only after all its
dependencies completed, and is cancelled exactly when one failed), and
`Jobforge.Cron.next/2` checked against a brute-force minute scan on 400 random
expressions.

Requires Elixir 1.15+ and Erlang/OTP 25+. No dependencies.

## License

MIT, see [LICENSE](LICENSE).
