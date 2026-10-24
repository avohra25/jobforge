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

54 tests, including a randomized stress test that throws successes, failures,
snoozes, cancels, crashes and delays at a queue and checks that every job
ends in exactly one terminal state without ever exceeding the concurrency limit.

Requires Elixir 1.15+ and Erlang/OTP 25+. No dependencies.

## License

MIT, see [LICENSE](LICENSE).
