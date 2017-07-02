defmodule Verk.StatsTest do
  use ExUnit.Case
  import Verk.Stats

  setup do
    { :ok, redis } = Confex.get_env(:verk, :redis_url)
                      |> Redix.start_link
    Redix.command!(redis, ~w(DEL stat:processed stat:failed))
    Redix.command!(redis, ~w(DEL stat:processed:default stat:failed:default))
    { :ok, redis: redis }
  end

  describe "total/1" do
    test "with no data", %{ redis: redis } do
      assert total(redis) == %{ processed: 0, failed: 0 }
    end

    test "with data", %{ redis: redis } do
      Redix.command!(redis, ~w(MSET stat:processed 25 stat:failed 32))

      assert total(redis) == %{ processed: 25, failed: 32 }
    end
  end

  describe "queue_total/2" do
    test "with no data", %{ redis: redis } do
      assert queue_total("default", redis) == %{total_processed: 0, total_failed: 0}
    end

    test "with data", %{ redis: redis } do
      Redix.command!(redis, ~w(MSET stat:processed:default 25 stat:failed:default 32))

      assert queue_total("default", redis) == %{total_processed: 25, total_failed: 32}
    end
  end
end
