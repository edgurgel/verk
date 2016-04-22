defmodule Verk.DslTest do
  use ExUnit.Case
  import Verk.Dsl

  def incr(x) do
    case x do
      nil -> {:error, "Bad math"}
      _ -> {:ok, x + 1}
    end
  end

  def ok do
    :ok
  end

  test "bangify" do
    assert bangify(ok) == nil

    assert bangify(incr(2)) == 3

    assert_raise RuntimeError, "Bad math", fn ->
      bangify(incr(nil))
    end
  end
end
