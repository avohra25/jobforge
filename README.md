# jobforge

A small, dependency-free background job queue for Elixir, built on plain OTP.

> Work in progress. This first commit adds the core data structures; the queue
> server, persistence and rate limiting land in follow-up commits.

## What's here so far

- `Jobforge.Job` - the job struct, validation, scheduling options and lifecycle helpers
- `Jobforge.Backoff` - exponential, linear, constant and custom retry delays with jitter
- `Jobforge.PQueue` - a persistent pairing-heap priority queue with FIFO tie-breaking

## Running the tests

```sh
mix test
```

Requires Elixir 1.15+ and Erlang/OTP 25+. No dependencies.

## License

MIT
