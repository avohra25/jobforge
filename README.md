# jobforge

A small, dependency-free background job queue for Elixir, built on plain OTP.

> Work in progress: a demo task and fuller docs are coming.

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

## What's here so far

- `Jobforge.Job` - the job struct, validation, scheduling options and lifecycle helpers
- `Jobforge.Backoff` - exponential, linear, constant and custom retry delays with jitter
- `Jobforge.PQueue` - a persistent pairing-heap priority queue with FIFO tie-breaking
- `Jobforge.Store` - a behaviour for job storage, with `Store.Memory` (a map) and
  `Store.DETS` (a disk table that reloads every fully written job after a crash)
- `Jobforge.Worker` - the behaviour job modules implement, with `use` defaults and
  result normalisation (exceptions, throws and exits become errors)
- `Jobforge.RateLimiter` - a pure token bucket that reports how long to wait
- `Jobforge.Queue` - the GenServer that ties it together: priorities, delayed jobs,
  bounded concurrency, retries with backoff, per-attempt timeouts, snooze, cancel,
  unique keys, rate limiting, dead letters, events and crash recovery

## Running the tests

```sh
mix test
```

Requires Elixir 1.15+ and Erlang/OTP 25+. No dependencies.

## License

MIT
