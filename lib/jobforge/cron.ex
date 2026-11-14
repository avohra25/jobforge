defmodule Jobforge.Cron do
  @moduledoc """
  Cron expressions: parsing, matching and computing the next fire time.

      {:ok, cron} = Jobforge.Cron.parse("*/15 9-17 * * MON-FRI")
      Jobforge.Cron.next(cron, ~U[2026-11-13 17:50:00Z])
      #=> {:ok, ~U[2026-11-16 09:00:00Z]}

  Five fields (minute, hour, day of month, month, day of week) follow Vixie
  cron. An optional sixth, leading field gives seconds, so `"*/10 * * * * *"`
  fires every ten seconds. Each field accepts:

    * `*` - every value
    * `5`, `MON`, `JAN` - a single value (names are case-insensitive)
    * `1-5`, `MON-FRI` - an inclusive range
    * `*/15`, `10-40/10`, `7/5` - steps over all values, a range, or from a
      start value to the field's maximum
    * `1,15,30-35` - lists of any of the above

  Day of week runs 0-7 with both 0 and 7 meaning Sunday. As in Vixie cron,
  when both day of month and day of week are restricted (neither starts with
  `*`), a day matches if *either* matches: `"0 0 1 * MON"` fires on the 1st
  and on every Monday.

  Macros: `@yearly` (`@annually`), `@monthly`, `@weekly`, `@daily`
  (`@midnight`) and `@hourly`.

  All times are UTC. `next/2` takes a `DateTime` in `Etc/UTC`, a
  `NaiveDateTime` (read as UTC) or Unix milliseconds, and returns the same
  kind of value.
  """

  @enforce_keys [:source, :seconds, :minutes, :hours, :days, :months, :weekdays]
  defstruct [
    :source,
    :seconds,
    :minutes,
    :hours,
    :days,
    :months,
    :weekdays,
    dom_any: true,
    dow_any: true
  ]

  @type t :: %__MODULE__{
          source: String.t(),
          seconds: [0..59],
          minutes: [0..59],
          hours: [0..23],
          days: [1..31],
          months: [1..12],
          weekdays: [0..6],
          dom_any: boolean(),
          dow_any: boolean()
        }

  @macros %{
    "@yearly" => "0 0 1 1 *",
    "@annually" => "0 0 1 1 *",
    "@monthly" => "0 0 1 * *",
    "@weekly" => "0 0 * * 0",
    "@daily" => "0 0 * * *",
    "@midnight" => "0 0 * * *",
    "@hourly" => "0 * * * *"
  }

  @months ~w(jan feb mar apr may jun jul aug sep oct nov dec)
          |> Enum.with_index(1)
          |> Map.new()
  @weekdays ~w(sun mon tue wed thu fri sat) |> Enum.with_index() |> Map.new()

  # A schedule that matches nothing (e.g. "0 0 30 2 *") would loop forever;
  # leap days on a fixed weekday can be up to 28 years apart, so look that far.
  @horizon_years 29

  @doc "Parses an expression. Errors name the offending field."
  @spec parse(String.t()) :: {:ok, t()} | {:error, term()}
  def parse(expr) when is_binary(expr) do
    source = String.trim(expr)
    expanded = Map.get(@macros, String.downcase(source), source)

    fields =
      case String.split(expanded, ~r/\s+/, trim: true) do
        [_, _, _, _, _] = five -> {:ok, ["0" | five]}
        [_, _, _, _, _, _] = six -> {:ok, six}
        [] -> {:error, :empty}
        other -> {:error, {:field_count, length(other)}}
      end

    with {:ok, [s, mi, h, dom, mo, dow]} <- fields,
         {:ok, seconds} <- field(:second, s, 0, 59, %{}),
         {:ok, minutes} <- field(:minute, mi, 0, 59, %{}),
         {:ok, hours} <- field(:hour, h, 0, 23, %{}),
         {:ok, days} <- field(:day_of_month, dom, 1, 31, %{}),
         {:ok, months} <- field(:month, mo, 1, 12, @months),
         {:ok, weekdays} <- field(:day_of_week, dow, 0, 7, @weekdays) do
      {:ok,
       %__MODULE__{
         source: source,
         seconds: seconds,
         minutes: minutes,
         hours: hours,
         days: days,
         months: months,
         weekdays: weekdays |> Enum.map(&rem(&1, 7)) |> Enum.uniq() |> Enum.sort(),
         dom_any: String.starts_with?(dom, "*"),
         dow_any: String.starts_with?(dow, "*")
       }}
    end
  end

  def parse(other), do: {:error, {:not_a_string, other}}

  @doc "Like `parse/1` but raises `ArgumentError`."
  @spec parse!(String.t()) :: t()
  def parse!(expr) do
    case parse(expr) do
      {:ok, cron} -> cron
      {:error, reason} -> raise ArgumentError, "invalid cron #{inspect(expr)}: #{inspect(reason)}"
    end
  end

  @doc "True when the expression fires at `time` (sub-second parts are ignored)."
  @spec match?(t(), DateTime.t() | NaiveDateTime.t() | integer()) :: boolean()
  def match?(%__MODULE__{} = c, time) do
    t = to_naive(time)

    t.second in c.seconds and t.minute in c.minutes and t.hour in c.hours and
      day_match?(c, NaiveDateTime.to_date(t))
  end

  @doc """
  The first fire time strictly after `time`, or `{:error, :never}` when the
  expression can't match within #{@horizon_years} years (such as `"0 0 31 2 *"`).
  """
  @spec next(t(), DateTime.t() | NaiveDateTime.t() | integer()) ::
          {:ok, DateTime.t() | NaiveDateTime.t() | integer()} | {:error, :never}
  def next(%__MODULE__{} = c, time) do
    t = to_naive(time)
    start = t |> NaiveDateTime.truncate(:second) |> NaiveDateTime.add(1, :second)
    limit = Date.new!(t.year + @horizon_years, 12, 31)

    with {:ok, found} <- search(c, NaiveDateTime.to_date(start), start, limit) do
      {:ok, from_naive(found, time)}
    end
  end

  @doc "Lazily yields fire times after `time`, in order."
  @spec stream(t(), DateTime.t() | NaiveDateTime.t() | integer()) :: Enumerable.t()
  def stream(%__MODULE__{} = c, time) do
    Stream.unfold(time, fn t ->
      case next(c, t) do
        {:ok, n} -> {n, n}
        {:error, :never} -> nil
      end
    end)
  end

  ## Search

  # Walk day by day (skipping whole months that can't match), then look for
  # the first time of day on a matching day. `floor` is the earliest moment
  # allowed on the first day; later days start at midnight.
  defp search(c, date, floor, limit) do
    cond do
      Date.compare(date, limit) == :gt ->
        {:error, :never}

      date.month not in c.months ->
        search(c, first_of_next_month(date), nil, limit)

      not day_match?(c, date) ->
        search(c, Date.add(date, 1), nil, limit)

      true ->
        case time_on(c, date, floor) do
          {:ok, ndt} -> {:ok, ndt}
          :none -> search(c, Date.add(date, 1), nil, limit)
        end
    end
  end

  defp time_on(c, date, nil), do: first_time(c, date, 0, 0, 0)
  defp time_on(c, date, f), do: first_time(c, date, f.hour, f.minute, f.second)

  # Smallest (h, m, s) >= (h0, m0, s0) lexicographically with each part allowed.
  defp first_time(c, date, h0, m0, s0) do
    Enum.find_value(c.hours, :none, fn
      h when h < h0 ->
        nil

      h ->
        {mmin, smin} = if h == h0, do: {m0, s0}, else: {0, 0}

        Enum.find_value(c.minutes, fn
          m when m < mmin ->
            nil

          m ->
            floor_s = if m == mmin, do: smin, else: 0

            case Enum.find(c.seconds, &(&1 >= floor_s)) do
              nil -> nil
              s -> {:ok, NaiveDateTime.new!(date, Time.new!(h, m, s))}
            end
        end)
    end)
  end

  defp day_match?(c, date) do
    dom? = date.day in c.days
    dow? = rem(Date.day_of_week(date), 7) in c.weekdays

    cond do
      date.month not in c.months -> false
      c.dom_any or c.dow_any -> dom? and dow?
      true -> dom? or dow?
    end
  end

  defp first_of_next_month(%Date{year: y, month: 12}), do: Date.new!(y + 1, 1, 1)
  defp first_of_next_month(%Date{year: y, month: m}), do: Date.new!(y, m + 1, 1)

  ## Field parsing

  defp field(name, text, lo, hi, names) do
    text
    |> String.downcase()
    |> String.split(",")
    |> Enum.reduce_while({:ok, []}, fn part, {:ok, acc} ->
      case part(part, lo, hi, names) do
        {:ok, values} -> {:cont, {:ok, values ++ acc}}
        {:error, why} -> {:halt, {:error, {name, text, why}}}
      end
    end)
    |> case do
      {:ok, values} -> {:ok, values |> Enum.uniq() |> Enum.sort()}
      error -> error
    end
  end

  defp part(part, lo, hi, names) do
    {range, step} =
      case String.split(part, "/") do
        [r] -> {r, nil}
        [r, s] -> {r, s}
        _ -> {part, :bad}
      end

    with {:ok, step} <- step(step),
         {:ok, a, b} <- range(range, step, lo, hi, names) do
      {:ok, Enum.take_every(a..b//1, step || 1) |> Enum.to_list()}
    end
  end

  defp step(nil), do: {:ok, nil}
  defp step(:bad), do: {:error, :bad_step}

  defp step(text) do
    case Integer.parse(text) do
      {n, ""} when n > 0 -> {:ok, n}
      _ -> {:error, {:bad_step, text}}
    end
  end

  defp range("*", _step, lo, hi, _names), do: {:ok, lo, hi}

  defp range(text, step, lo, hi, names) do
    case String.split(text, "-") do
      [a] ->
        with {:ok, a} <- value(a, lo, hi, names) do
          # "7/5" means from 7 up to the maximum in steps of 5.
          if step, do: {:ok, a, hi}, else: {:ok, a, a}
        end

      [a, b] ->
        with {:ok, a} <- value(a, lo, hi, names),
             {:ok, b} <- value(b, lo, hi, names) do
          if a <= b, do: {:ok, a, b}, else: {:error, {:reversed_range, a, b}}
        end

      _ ->
        {:error, {:bad_range, text}}
    end
  end

  defp value(text, lo, hi, names) do
    case Map.fetch(names, text) do
      {:ok, v} ->
        {:ok, v}

      :error ->
        case Integer.parse(text) do
          {n, ""} when n >= lo and n <= hi -> {:ok, n}
          {n, ""} -> {:error, {:out_of_range, n, lo..hi}}
          _ -> {:error, {:bad_value, text}}
        end
    end
  end

  ## Time conversion

  defp to_naive(%DateTime{time_zone: "Etc/UTC"} = dt), do: DateTime.to_naive(dt)

  defp to_naive(%DateTime{} = dt),
    do: raise(ArgumentError, "Jobforge.Cron works in UTC, got #{dt.time_zone}")

  defp to_naive(%NaiveDateTime{} = ndt), do: ndt

  defp to_naive(ms) when is_integer(ms),
    do: ms |> DateTime.from_unix!(:millisecond) |> DateTime.to_naive()

  defp from_naive(ndt, %DateTime{}), do: DateTime.from_naive!(ndt, "Etc/UTC")
  defp from_naive(ndt, %NaiveDateTime{}), do: ndt

  defp from_naive(ndt, ms) when is_integer(ms),
    do: ndt |> DateTime.from_naive!("Etc/UTC") |> DateTime.to_unix(:millisecond)
end
