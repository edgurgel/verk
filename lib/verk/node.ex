defmodule Verk.Node do
  @moduledoc """
  Node data controller
  """

  @verk_nodes_key "verk_nodes"

  @spec register(String.t(), non_neg_integer, GenServer.t()) ::
          :ok | {:error, :verk_node_id_already_running}
  def register(verk_node_id, ttl, redis) do
    case Redix.pipeline!(redis, [
           ["SADD", @verk_nodes_key, verk_node_id],
           ["PSETEX", "verk:node:#{verk_node_id}", ttl, "alive"]
         ]) do
      [1, _] -> :ok
      _ -> {:error, :node_id_already_running}
    end
  end

  @spec deregister!(String.t(), GenServer.t()) :: :ok
  def deregister!(verk_node_id, redis) do
    Redix.pipeline!(redis, [
      ["DEL", verk_node_key(verk_node_id)],
      ["DEL", verk_node_queues_key(verk_node_id)],
      ["SREM", @verk_nodes_key, verk_node_id]
    ])

    :ok
  end

  @spec members(integer, non_neg_integer, GenServer.t()) ::
          {:ok, [String.t()]} | {:more, [String.t()], integer}
  def members(cursor \\ 0, count \\ 25, redis) do
    case Redix.command!(redis, ["SSCAN", @verk_nodes_key, cursor, "COUNT", count]) do
      ["0", verk_nodes] -> {:ok, verk_nodes}
      [cursor, verk_nodes] -> {:more, verk_nodes, cursor}
    end
  end

  @spec ttl!(String.t(), GenServer.t()) :: integer
  def ttl!(verk_node_id, redis) do
    Redix.command!(redis, ["PTTL", verk_node_key(verk_node_id)])
  end

  @spec expire_in!(String.t(), integer, GenServer.t()) :: integer
  def expire_in!(verk_node_id, ttl, redis) do
    Redix.command!(redis, ["PSETEX", verk_node_key(verk_node_id), ttl, "alive"])
  end

  @spec queues!(String.t(), integer, non_neg_integer, GenServer.t()) ::
          {:ok, [String.t()]} | {:more, [String.t()], integer}
  def queues!(verk_node_id, cursor \\ 0, count \\ 25, redis) do
    case Redix.command!(redis, [
           "SSCAN",
           verk_node_queues_key(verk_node_id),
           cursor,
           "COUNT",
           count
         ]) do
      ["0", queues] -> {:ok, queues}
      [cursor, queues] -> {:more, queues, cursor}
    end
  end

  def add_queue!(verk_node_id, queue, redis) do
    Redix.command!(redis, ["SADD", verk_node_queues_key(verk_node_id), queue])
  end

  def remove_queue!(verk_node_id, queue, redis) do
    Redix.command!(redis, ["SREM", verk_node_queues_key(verk_node_id), queue])
  end

  defp verk_node_key(verk_node_id), do: "verk:node:#{verk_node_id}"
  defp verk_node_queues_key(verk_node_id), do: "verk:node:#{verk_node_id}:queues"
end
