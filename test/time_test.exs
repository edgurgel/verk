defmodule Verk.TimeTest do
  use ExUnit.Case
  alias Verk.Time

  describe "shift/3" do
    test "moves datetime by indicated amount" do
      time1 = DateTime.from_unix!(10)
      time2 = DateTime.from_unix!(0)

      assert Time.shift(time1, -10, :second) == time2
    end
  end

  describe "after?/2" do
    test "returns true if first time is after the second" do
      time1 = DateTime.from_unix!(10)
      time2 = DateTime.from_unix!(0)

      assert Time.after?(time1, time2)
      refute Time.after?(time2, time1)
    end
  end

  describe "diff/3" do
    test "returns difference between DateTimes" do
      time1 = DateTime.from_unix!(10)
      time2 = DateTime.from_unix!(0)

      assert Time.diff(time2, time1, :second) == 10
      assert Time.diff(time2, time1, :microsecond) == 10_000_000
    end
  end
end
