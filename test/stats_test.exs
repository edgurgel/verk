defmodule Verk.StatsTest do
  use ExUnit.Case
  import Verk.Stats

  setup do
    { :ok, redis } = Application.fetch_env(:verk, :redis_url)
                       |> elem(1)
                       |> Redix.start_link
    Redix.command!(redis, ~w(DEL stat:processed stat:failed))
    { :ok, redis: redis }
  end

  test "total with no data", %{ redis: redis } do
    assert total(redis) == %{ processed: 0, failed: 0 }
  end

  test "total with data", %{ redis: redis } do
    Redix.command!(redis, ~w(MSET stat:processed 25 stat:failed 32))

    assert total(redis) == %{ processed: 25, failed: 32 }
  end
end
