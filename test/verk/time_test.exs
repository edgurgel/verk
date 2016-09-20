defmodule Verk.TimeTest do
  use ExUnit.Case
  alias Verk.Time

  test "shift/3 moves datetime by indicated amount" do
    time1 = DateTime.from_unix!(10)
    time2 = DateTime.from_unix!(0)

    assert Time.shift(time1, -10, :seconds) == time2
  end

  test "after?/2 returns true if first time is after the second" do
    time1 = DateTime.from_unix!(10)
    time2 = DateTime.from_unix!(0)

    assert Time.after?(time1, time2)
  end


end
