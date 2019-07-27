defmodule Verk.Redis.EntryIDTest do
  use ExUnit.Case
  alias Verk.Redis.EntryID
  doctest Verk.Redis.EntryID

  describe "parse/1" do
    test "parsing 0-0" do
      assert EntryID.parse("0-0") == %EntryID{time: 0, seq: 0}
    end

    test "parsing '-'" do
      assert EntryID.parse("-") == %EntryID{time: 0, seq: 0}
    end

    test "parsing 18446744073709551615-18446744073709551615" do
      assert EntryID.parse("18446744073709551615-18446744073709551615") == %EntryID{
               time: 18_446_744_073_709_551_615,
               seq: 18_446_744_073_709_551_615
             }
    end

    test "parsing '+'" do
      assert EntryID.parse("+") == %EntryID{
               time: 18_446_744_073_709_551_615,
               seq: 18_446_744_073_709_551_615
             }
    end
  end

  test "to_string/1" do
    assert to_string(%EntryID{time: 123, seq: 5}) == "123-5"
  end
end
