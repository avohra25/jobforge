defmodule Jobforge.PQueue do
  @moduledoc """
  A persistent (immutable) min-priority queue backed by a pairing heap.

  Keys are compared with Erlang term ordering, so integers, tuples such as
  `{priority, run_at}` and timestamps all work. Entries with equal keys are
  popped in insertion (FIFO) order, which keeps job dispatch fair.

  `push/3` and `peek/1` are O(1); `pop/1` is amortised O(log n).
  """

  defstruct heap: nil, size: 0, seq: 0

  @type key :: term()
  @type value :: term()
  @opaque t :: %__MODULE__{heap: term(), size: non_neg_integer(), seq: non_neg_integer()}

  @doc "Returns an empty queue."
  @spec new() :: t()
  def new, do: %__MODULE__{}

  @doc "Builds a queue from `{key, value}` pairs."
  @spec from_list([{key(), value()}]) :: t()
  def from_list(pairs), do: Enum.reduce(pairs, new(), fn {k, v}, q -> push(q, k, v) end)

  @doc "Inserts `value` under `key`."
  @spec push(t(), key(), value()) :: t()
  def push(%__MODULE__{heap: heap, size: size, seq: seq} = q, key, value) do
    %{q | heap: meld(heap, {{key, seq}, value, []}), size: size + 1, seq: seq + 1}
  end

  @doc "Returns the smallest entry without removing it."
  @spec peek(t()) :: {:ok, {key(), value()}} | :empty
  def peek(%__MODULE__{heap: nil}), do: :empty
  def peek(%__MODULE__{heap: {{key, _seq}, value, _children}}), do: {:ok, {key, value}}

  @doc "Removes and returns the smallest entry."
  @spec pop(t()) :: {:ok, {key(), value()}, t()} | :empty
  def pop(%__MODULE__{heap: nil}), do: :empty

  def pop(%__MODULE__{heap: {{key, _seq}, value, children}, size: size} = q) do
    {:ok, {key, value}, %{q | heap: merge_pairs(children), size: size - 1}}
  end

  @doc "Number of entries."
  @spec size(t()) :: non_neg_integer()
  def size(%__MODULE__{size: size}), do: size

  @doc "True when the queue has no entries."
  @spec empty?(t()) :: boolean()
  def empty?(%__MODULE__{size: size}), do: size == 0

  @doc "Drains the queue into a sorted list of `{key, value}` pairs."
  @spec to_list(t()) :: [{key(), value()}]
  def to_list(q), do: drain(q, [])

  defp drain(q, acc) do
    case pop(q) do
      :empty -> Enum.reverse(acc)
      {:ok, entry, rest} -> drain(rest, [entry | acc])
    end
  end

  defp meld(nil, h), do: h
  defp meld(h, nil), do: h

  defp meld({k1, v1, c1} = h1, {k2, v2, c2} = h2) do
    if k1 <= k2, do: {k1, v1, [h2 | c1]}, else: {k2, v2, [h1 | c2]}
  end

  # Two-pass pairing: meld adjacent pairs left to right, then fold right to left.
  defp merge_pairs(children), do: children |> pair_up([]) |> Enum.reduce(nil, &meld/2)

  defp pair_up([], acc), do: acc
  defp pair_up([h], acc), do: [h | acc]
  defp pair_up([a, b | rest], acc), do: pair_up(rest, [meld(a, b) | acc])
end
