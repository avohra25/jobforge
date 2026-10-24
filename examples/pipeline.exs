# A tiny two-step pipeline: fetch jobs enqueue parse jobs, persisted to disk.
#
#     mix run examples/pipeline.exs
#
# Run it, interrupt it with Ctrl-C twice partway through, and run it again:
# jobs that hadn't finished are reloaded from pipeline.dets and picked up.

defmodule Example.Fetch do
  use Jobforge.Worker, max_attempts: 3, priority: 1

  @impl true
  def perform(%Jobforge.Job{args: %{"url" => url}, meta: %{queue: q}}) do
    Process.sleep(200)
    body = "<html>#{String.duplicate("word ", :erlang.phash2(url, 50) + 1)}</html>"

    {:ok, _} =
      Jobforge.insert(q, Example.Parse, %{"url" => url, "body" => body},
        unique: {:parse, url},
        meta: %{queue: q}
      )

    :ok
  end
end

defmodule Example.Parse do
  use Jobforge.Worker, priority: 0

  @impl true
  def perform(%Jobforge.Job{args: %{"url" => url, "body" => body}}) do
    words = body |> String.split() |> length()
    IO.puts("#{url}: #{words} words")
    :ok
  end
end

path = Path.expand("pipeline.dets")

{:ok, q} =
  Jobforge.start_link(
    name: Example.Queue,
    concurrency: 2,
    store: {Jobforge.Store.DETS, path: path}
  )

if Jobforge.list(q) == [] do
  for i <- 1..10 do
    url = "https://example.com/page/#{i}"

    {:ok, _} =
      Jobforge.insert(q, Example.Fetch, %{"url" => url},
        unique: {:fetch, url},
        meta: %{queue: Example.Queue}
      )
  end
else
  IO.puts("resuming #{length(Jobforge.list(q))} unfinished jobs from #{path}")
end

:ok = Jobforge.drain(q, 60_000)
IO.inspect(Jobforge.stats(q), label: "stats")
Jobforge.stop(q)
File.rm(path)
