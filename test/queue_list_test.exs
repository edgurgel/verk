defmodule Verk.QueueListTest do
  use ExUnit.Case
  alias Verk.QueueList

  @queue_list_key "verk:queues"

  setup do
    {:ok, redis} = Confex.get_env(:verk, :redis_url) |> Redix.start_link()

    Redix.command!(redis, ~w(DEL #{@queue_list_key}))

    {:ok, redis: redis}
  end

  describe "add/3" do
    test "add queue to set", %{redis: redis} do
      assert QueueList.add("queue_1", redis) == {:ok, 1}
      assert Redix.command!(redis, ["SMEMBERS", @queue_list_key]) == ["queue_1"]
    end
  end

  describe "all/3" do
    test "list queues inside set", %{redis: redis} do
      assert QueueList.all("", redis) == {:ok, []}

      Redix.command!(redis, ["SADD", @queue_list_key, "queue_1"])
      Redix.command!(redis, ["SADD", @queue_list_key, "queue_2"])
      {:ok, set} = QueueList.all("", redis)

      assert MapSet.equal?(MapSet.new(set), MapSet.new(["queue_1", "queue_2"]))
    end

    test "filter queues using prefix", %{redis: redis} do
      assert QueueList.all("queue_", redis) == {:ok, []}

      Redix.command!(redis, ["SADD", @queue_list_key, "queue_1"])
      Redix.command!(redis, ["SADD", @queue_list_key, "queue_2"])
      Redix.command!(redis, ["SADD", @queue_list_key, "default"])
      {:ok, set} = QueueList.all("queue_", redis)

      assert MapSet.equal?(MapSet.new(set), MapSet.new(["queue_1", "queue_2"]))
    end
  end
end
