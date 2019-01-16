defmodule Verk.NodeTest do
  use ExUnit.Case
  import Verk.Node

  @verk_nodes_key "verk_nodes"

  @node "123"
  @node_key "verk:node:123"
  @node_queues_key "verk:node:123:queues"

  setup do
    {:ok, redis} = :verk |> Confex.get_env(:redis_url) |> Redix.start_link()
    Redix.command!(redis, ["DEL", @verk_nodes_key, @node_queues_key])
    {:ok, %{redis: redis}}
  end

  defp register(%{redis: redis}), do: register(@node, 555, redis)

  describe "register/3" do
    test "add to verk_nodes", %{redis: redis} do
      assert register(@node, 555, redis) == :ok
      assert Redix.command!(redis, ["SMEMBERS", "verk_nodes"]) == ["123"]
      assert register(@node, 555, redis) == {:error, :node_id_already_running}
    end

    test "set verk:node: key", %{redis: redis} do
      assert register(@node, 555, redis) == :ok
      assert Redix.command!(redis, ["GET", @node_key]) == "alive"
    end

    test "expire verk:node: key", %{redis: redis} do
      assert register(@node, 555, redis) == :ok
      ttl = Redix.command!(redis, ["PTTL", @node_key])
      assert_in_delta ttl, 555, 5
    end
  end

  describe "deregister/2" do
    setup :register

    test "remove from verk_nodes", %{redis: redis} do
      assert Redix.command!(redis, ["SMEMBERS", "verk_nodes"]) == ["123"]
      assert deregister!(@node, redis) == :ok
      assert Redix.command!(redis, ["SMEMBERS", "verk_nodes"]) == []
    end

    test "remove verk:node: key", %{redis: redis} do
      assert Redix.command!(redis, ["GET", @node_key]) == "alive"
      assert deregister!(@node, redis) == :ok
      assert Redix.command!(redis, ["GET", @node_key]) == nil
    end

    test "remove verk:node::queues key", %{redis: redis} do
      assert Redix.command!(redis, ["SADD", @node_queues_key, "queue_1"]) == 1
      assert deregister!(@node, redis) == :ok
      assert Redix.command!(redis, ["GET", @node_queues_key]) == nil
    end
  end

  describe "members/3" do
    setup %{redis: redis} do
      register("node_1", 555, redis)
      register("node_2", 555, redis)
      register("node_3", 555, redis)
      register("node_4", 555, redis)
    end

    test "list verk nodes", %{redis: redis} do
      {:ok, nodes} = members(redis)
      nodes = MapSet.new(nodes)
      assert MapSet.equal?(nodes, MapSet.new(["node_1", "node_2", "node_3", "node_4"]))
    end
  end

  describe "ttl!/2" do
    setup :register

    test "return ttl from a verk node id", %{redis: redis} do
      assert_in_delta ttl!(@node, redis), 555, 5
    end
  end

  describe "expire_in/3" do
    test "resets expiration item", %{redis: redis} do
      assert {:ok, _} = expire_in(@node, 888, redis)
      assert_in_delta Redix.command!(redis, ["PTTL", @node_key]), 888, 5
    end
  end

  describe "queues!/2" do
    setup %{redis: redis} do
      add_queue!(@node, "queue_1", redis)
      add_queue!(@node, "queue_2", redis)
      add_queue!(@node, "queue_3", redis)
      add_queue!(@node, "queue_4", redis)
      :ok
    end

    test "list queues", %{redis: redis} do
      {:ok, queues} = queues!(@node, 0, redis)
      queues = MapSet.new(queues)
      assert MapSet.equal?(queues, MapSet.new(["queue_1", "queue_2", "queue_3", "queue_4"]))
    end
  end

  describe "add_queue!/3" do
    test "add queue to verk:node::queues", %{redis: redis} do
      queue_name = "default"
      assert add_queue!(@node, queue_name, redis)
      assert Redix.command!(redis, ["SMEMBERS", @node_queues_key]) == [queue_name]
    end
  end

  describe "remove_queue!/3" do
    test "remove queue from verk:node::queues", %{redis: redis} do
      queue_name = "default"
      assert Redix.command!(redis, ["SADD", @node_queues_key, queue_name]) == 1
      assert remove_queue!(@node, queue_name, redis)
      assert Redix.command!(redis, ["SMEMBERS", @node_queues_key]) == []
    end
  end
end
