defmodule Verk.NodeTest do
  use ExUnit.Case, async: true
  import Verk.Node
  doctest Verk.Node

  @node "123"
  @node_key "verk:node:123"

  setup do
    {:ok, redis} = :verk |> Confex.get_env(:redis_url) |> Redix.start_link()
    Redix.command!(redis, ["DEL", @node_key])
    {:ok, %{redis: redis}}
  end

  defp register(redis, verk_node) do
    expire_in(verk_node, 555, redis)
  end

  describe "dead?/2" do
    test "return true if node is dead", %{redis: redis} do
      assert dead?(@node, redis) == {:ok, true}
    end

    test "return false if node is alive", %{redis: redis} do
      register(redis, @node)
      assert dead?(@node, redis) == {:ok, false}
    end
  end

  describe "expire_in/3" do
    test "resets expiration item", %{redis: redis} do
      assert {:ok, _} = expire_in(@node, 888, redis)
      assert_in_delta Redix.command!(redis, ["PTTL", @node_key]), 888, 50
    end
  end
end
