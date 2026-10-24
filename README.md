# jobforge

A small, dependency-free background job queue for Elixir, built on plain OTP.

> Work in progress. The building blocks are in place; the queue server that
> ties them together lands in a follow-up commit.

## What's here so far

- `Jobforge.Job` - the job struct, validation, scheduling options and lifecycle helpers
- `Jobforge.Backoff` - exponential, linear, constant and custom retry delays with jitter
- `Jobforge.PQueue` - a persistent pairing-heap priority queue with FIFO tie-breaking
- `Jobforge.Store` - a behaviour for job storage, with `Store.Memory` (a map) and
  `Store.DETS` (a disk table that reloads every fully written job after a crash)
- `Jobforge.Worker` - the behaviour job modules implement, with `use` defaults and
  result normalisation (exceptions, throws and exits become errors)
- `Jobforge.RateLimiter` - a pure token bucket that reports how long to wait

## Running the tests

```sh
mix test
```

Requires Elixir 1.15+ and Erlang/OTP 25+. No dependencies.

## License

MIT
