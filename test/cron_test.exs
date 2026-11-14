defmodule Jobforge.CronTest do
  use ExUnit.Case, async: true
  alias Jobforge.Cron

  defp next!(expr, from) do
    {:ok, t} = Cron.next(Cron.parse!(expr), from)
    t
  end

  describe "parse/1" do
    test "fields, ranges, steps, lists and names" do
      c = Cron.parse!("*/15 9-17 1,15 JAN-mar mon-FRI")
      assert c.seconds == [0]
      assert c.minutes == [0, 15, 30, 45]
      assert c.hours == Enum.to_list(9..17)
      assert c.days == [1, 15]
      assert c.months == [1, 2, 3]
      assert c.weekdays == [1, 2, 3, 4, 5]
      refute c.dom_any or c.dow_any
    end

    test "start/step runs to the field maximum, range/step stays inside the range" do
      assert Cron.parse!("7/20 * * * *").minutes == [7, 27, 47]
      assert Cron.parse!("10-40/15 * * * *").minutes == [10, 25, 40]
      assert Cron.parse!("0 */7 * * *").hours == [0, 7, 14, 21]
    end

    test "day of week 7 is Sunday and duplicates collapse" do
      assert Cron.parse!("0 0 * * 0,7,SUN").weekdays == [0]
      assert Cron.parse!("0 0 * * 5-7").weekdays == [0, 5, 6]
    end

    test "optional seconds field" do
      c = Cron.parse!("*/20 30 * * * *")
      assert c.seconds == [0, 20, 40]
      assert c.minutes == [30]
    end

    test "macros" do
      assert Cron.parse!("@hourly").minutes == [0]
      assert Cron.parse!("@HOURLY").hours == Enum.to_list(0..23)
      assert %{days: [1], months: [1]} = Cron.parse!("@annually")
      assert Cron.parse!("@weekly").weekdays == [0]
      assert Cron.parse!("  @daily ").source == "@daily"
    end

    test "errors name the field" do
      assert {:error, {:field_count, 4}} = Cron.parse("* * * *")
      assert {:error, :empty} = Cron.parse("   ")
      assert {:error, {:minute, "60", {:out_of_range, 60, 0..59}}} = Cron.parse("60 * * * *")
      assert {:error, {:hour, "5-2", {:reversed_range, 5, 2}}} = Cron.parse("0 5-2 * * *")
      assert {:error, {:month, "foo", {:bad_value, "foo"}}} = Cron.parse("0 0 1 foo *")
      assert {:error, {:day_of_month, "*/0", {:bad_step, "0"}}} = Cron.parse("0 0 */0 * *")
      assert {:error, {:day_of_week, "1/2/3", :bad_step}} = Cron.parse("0 0 * * 1/2/3")
      assert {:error, {:day_of_month, "0", _}} = Cron.parse("0 0 0 * *")
      assert {:error, {:not_a_string, 5}} = Cron.parse(5)
      assert_raise ArgumentError, ~r/invalid cron/, fn -> Cron.parse!("nope") end
    end
  end

  describe "next/2" do
    test "basic steps" do
      assert next!("*/15 * * * *", ~U[2026-11-13 10:07:31Z]) == ~U[2026-11-13 10:15:00Z]
      assert next!("*/15 * * * *", ~U[2026-11-13 10:15:00Z]) == ~U[2026-11-13 10:30:00Z]
      assert next!("0 9 * * *", ~U[2026-11-13 09:00:00.500Z]) == ~U[2026-11-14 09:00:00Z]
    end

    test "rolls over days, months and years" do
      assert next!("30 23 31 12 *", ~U[2026-12-31 23:30:00Z]) == ~U[2027-12-31 23:30:00Z]
      assert next!("0 0 31 * *", ~U[2026-04-15 00:00:00Z]) == ~U[2026-05-31 00:00:00Z]
      assert next!("*/15 9-17 * * MON-FRI", ~U[2026-11-13 17:50:00Z]) == ~U[2026-11-16 09:00:00Z]
    end

    test "leap days" do
      assert next!("0 12 29 2 *", ~U[2026-01-01 00:00:00Z]) == ~U[2028-02-29 12:00:00Z]
      # Vixie OR rule: the 29th of February or any Monday in February.
      assert next!("0 0 29 2 MON", ~U[2026-03-01 00:00:00Z]) == ~U[2027-02-01 00:00:00Z]
    end

    test "day of month and day of week combine with OR only when both are restricted" do
      # 1st of the month or any Monday.
      assert next!("0 0 1 * MON", ~U[2026-11-03 00:00:00Z]) == ~U[2026-11-09 00:00:00Z]
      assert next!("0 0 1 * MON", ~U[2026-11-24 00:00:00Z]) == ~U[2026-11-30 00:00:00Z]
      assert next!("0 0 1 * MON", ~U[2026-11-30 00:00:00Z]) == ~U[2026-12-01 00:00:00Z]
      # A star in either one makes it AND.
      assert next!("0 0 */1 * MON", ~U[2026-11-03 00:00:00Z]) == ~U[2026-11-09 00:00:00Z]
      assert next!("0 0 13 * *", ~U[2026-11-14 00:00:00Z]) == ~U[2026-12-13 00:00:00Z]
      assert next!("0 0 * * 5", ~U[2026-11-14 00:00:00Z]) == ~U[2026-11-20 00:00:00Z]
    end

    test "seconds" do
      assert next!("*/10 * * * * *", ~U[2026-11-13 10:00:05Z]) == ~U[2026-11-13 10:00:10Z]
      assert next!("5 0 * * * *", ~U[2026-11-13 10:59:59Z]) == ~U[2026-11-13 11:00:05Z]
    end

    test "impossible schedules return :never" do
      assert Cron.next(Cron.parse!("0 0 30 2 *"), ~U[2026-01-01 00:00:00Z]) == {:error, :never}

      assert Cron.next(Cron.parse!("0 0 31 4,6,9,11 *"), ~N[2026-01-01 00:00:00]) ==
               {:error, :never}
    end

    test "keeps the input's type" do
      c = Cron.parse!("@hourly")
      assert Cron.next(c, ~N[2026-11-13 10:20:00]) == {:ok, ~N[2026-11-13 11:00:00]}
      ms = DateTime.to_unix(~U[2026-11-13 10:20:00Z], :millisecond)
      assert Cron.next(c, ms) == {:ok, DateTime.to_unix(~U[2026-11-13 11:00:00Z], :millisecond)}

      assert_raise ArgumentError, fn ->
        Cron.next(
          c,
          DateTime.from_naive!(~N[2026-01-01 00:00:00], "Etc/UTC")
          |> Map.put(:time_zone, "Europe/Paris")
        )
      end
    end

    test "stream/2" do
      times = Cron.parse!("0 0 1 */4 *") |> Cron.stream(~U[2026-11-13 00:00:00Z]) |> Enum.take(3)

      assert times == [
               ~U[2027-01-01 00:00:00Z],
               ~U[2027-05-01 00:00:00Z],
               ~U[2027-09-01 00:00:00Z]
             ]

      assert Cron.parse!("0 0 30 2 *") |> Cron.stream(~U[2026-01-01 00:00:00Z]) |> Enum.to_list() ==
               []
    end
  end

  describe "against a brute-force reference" do
    # An independent matcher written straight from the field definitions.
    defp ref_match?(spec, ndt) do
      {mi, h, dom, mo, dow} = spec
      d = NaiveDateTime.to_date(ndt)
      wd = rem(Date.day_of_week(d), 7)
      dom_ok = d.day in dom.set
      dow_ok = wd in dow.set or (wd == 0 and 7 in dow.set)
      day_ok = if dom.star or dow.star, do: dom_ok and dow_ok, else: dom_ok or dow_ok

      ndt.second == 0 and ndt.minute in mi.set and ndt.hour in h.set and ndt.month in mo.set and
        day_ok
    end

    # Scans minute by minute, but skips days that can't match so years-long
    # gaps stay cheap.
    defp ref_next(spec, from) do
      start = from |> NaiveDateTime.truncate(:second) |> NaiveDateTime.add(1) |> ceil_minute()
      ref_scan(spec, start, NaiveDateTime.add(from, 30 * 366 * 86_400))
    end

    defp ceil_minute(%NaiveDateTime{second: 0} = t), do: t
    defp ceil_minute(t), do: NaiveDateTime.add(%{t | second: 0}, 60)

    defp ref_scan(spec, t, limit) do
      cond do
        NaiveDateTime.compare(t, limit) == :gt ->
          :never

        not ref_match?(spec, %{t | hour: hd(elem(spec, 1).set), minute: hd(elem(spec, 0).set)}) and
            not day_possible?(spec, t) ->
          next_day =
            t |> NaiveDateTime.to_date() |> Date.add(1) |> NaiveDateTime.new!(~T[00:00:00])

          ref_scan(spec, next_day, limit)

        ref_match?(spec, t) ->
          {:ok, t}

        true ->
          ref_scan(spec, NaiveDateTime.add(t, 60), limit)
      end
    end

    defp day_possible?(spec, t) do
      Enum.any?(elem(spec, 1).set, fn h ->
        Enum.any?(elem(spec, 0).set, &ref_match?(spec, %{t | hour: h, minute: &1}))
      end)
    end

    defp gen_field(lo, hi, names) do
      case :rand.uniform(6) do
        1 ->
          {"*", %{star: true, set: Enum.to_list(lo..hi)}}

        2 ->
          s = :rand.uniform(max(div(hi - lo, 2), 1))
          {"*/#{s}", %{star: true, set: Enum.take_every(lo..hi, s) |> Enum.to_list()}}

        3 ->
          [a, b] =
            Enum.sort([lo + :rand.uniform(hi - lo + 1) - 1, lo + :rand.uniform(hi - lo + 1) - 1])

          {"#{name(a, names)}-#{name(b, names)}", %{star: false, set: Enum.to_list(a..b)}}

        4 ->
          a = lo + :rand.uniform(hi - lo + 1) - 1
          s = :rand.uniform(5)
          {"#{a}/#{s}", %{star: false, set: Enum.take_every(a..hi//1, s) |> Enum.to_list()}}

        _ ->
          vals = for _ <- 1..:rand.uniform(3), do: lo + :rand.uniform(hi - lo + 1) - 1
          text = vals |> Enum.map(&name(&1, names)) |> Enum.join(",")
          {text, %{star: false, set: vals |> Enum.uniq() |> Enum.sort()}}
      end
    end

    defp name(v, names) do
      case names do
        [] -> v
        list -> if :rand.uniform(2) == 1 and v < length(list), do: Enum.at(list, v), else: v
      end
    end

    test "next/2 agrees with a minute-by-minute scan on random expressions" do
      :rand.seed(:exsss, {7, 11, 2026})
      months = [nil | ~w(JAN FEB MAR APR MAY JUN JUL AUG SEP OCT NOV DEC)]
      days = ~w(SUN MON TUE WED THU FRI SAT)

      for _ <- 1..400 do
        {mi_t, mi} = gen_field(0, 59, [])
        {h_t, h} = gen_field(0, 23, [])
        {dom_t, dom} = gen_field(1, 31, [])
        {mo_t, mo} = gen_field(1, 12, months)
        {dow_t, dow} = gen_field(0, 7, days)
        expr = Enum.join([mi_t, h_t, dom_t, mo_t, dow_t], " ")
        spec = {mi, h, dom, mo, dow}
        cron = Cron.parse!(expr)

        from =
          ~N[2024-01-01 00:00:00]
          |> NaiveDateTime.add(:rand.uniform(4 * 365 * 86_400), :second)

        expected =
          case ref_next(spec, from) do
            {:ok, t} -> {:ok, t}
            :never -> {:error, :never}
          end

        assert Cron.next(cron, from) == expected, "#{expr} from #{from}"

        with {:ok, t} <- expected do
          assert Cron.match?(cron, t)
          refute Cron.match?(cron, NaiveDateTime.add(t, 1))
        end
      end
    end
  end
end
