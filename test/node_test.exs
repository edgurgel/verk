defmodule Verk.NodeTest do
  use ExUnit.Case
  import Verk.Node
  doctest Verk.Node

  @verk_nodes_key "verk_nodes"

  @node "123"
  @node_key "verk:node:123"
  @node_queues_key "verk:node:123:queues"

  setup do
    {:ok, redis} = :verk |> Confex.get_env(:redis_url) |> Redix.start_link()
    Redix.command!(redis, ["DEL", @verk_nodes_key, @node_queues_key])
    {:ok, %{redis: redis}}
  end

  defp register(%{redis: redis}) do
    register(redis, @node)
    :ok
  end

  defp register(redis, verk_node) do
    expire_in(verk_node, 555, redis)
    Redix.command!(redis, add_node_redis_command(verk_node))
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
      register(redis, "node_1")
      register(redis, "node_2")
      register(redis, "node_3")
      register(redis, "node_4")
      :ok
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
      Redix.command!(redis, add_queue_redis_command(@node, "queue_1"))
      Redix.command!(redis, add_queue_redis_command(@node, "queue_2"))
      Redix.command!(redis, add_queue_redis_command(@node, "queue_3"))
      Redix.command!(redis, add_queue_redis_command(@node, "queue_4"))
      :ok
    end

    test "list queues", %{redis: redis} do
      {:ok, queues} = queues!(@node, 0, redis)
      queues = MapSet.new(queues)
      assert MapSet.equal?(queues, MapSet.new(["queue_1", "queue_2", "queue_3", "queue_4"]))
    end
  end
end
