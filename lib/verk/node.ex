defmodule Verk.Node do
  @moduledoc """
  Node data controller.
  """

  @verk_nodes_key "verk_nodes"

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
          {:ok, [String.t()]} | {:more, [String.t()], integer} | {:error, term}
  def members(cursor \\ 0, count \\ 25, redis) do
    case Redix.command(redis, ["SSCAN", @verk_nodes_key, cursor, "COUNT", count]) do
      {:ok, ["0", verk_nodes]} -> {:ok, verk_nodes}
      {:ok, [cursor, verk_nodes]} -> {:more, verk_nodes, cursor}
      {:error, reason} -> {:error, reason}
    end
  end

  @spec ttl!(String.t(), GenServer.t()) :: integer
  def ttl!(verk_node_id, redis) do
    Redix.command!(redis, ["PTTL", verk_node_key(verk_node_id)])
  end

  @spec expire_in(String.t(), integer, GenServer.t()) :: {:ok, integer} | {:error, term}
  def expire_in(verk_node_id, ttl, redis) do
    Redix.command(redis, ["PSETEX", verk_node_key(verk_node_id), ttl, "alive"])
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

  @doc """
  Redis command to add a queue to the set of queues that a node is processing.

      iex> Verk.Node.add_queue_redis_command("123", "default")
      ["SADD", "verk:node:123:queues", "default"]

  """
  @spec add_queue_redis_command(String.t(), String.t()) :: [String.t()]
  def add_queue_redis_command(verk_node_id, queue) do
    ["SADD", verk_node_queues_key(verk_node_id), queue]
  end

  @doc """
  Redis command to add a queue to the set of queues that a node is processing.

      iex> Verk.Node.add_node_redis_command("123")
      ["SADD", "verk_nodes", "123"]

  """
  @spec add_node_redis_command(String.t()) :: [String.t()]
  def add_node_redis_command(verk_node_id) do
    ["SADD", @verk_nodes_key, verk_node_id]
  end

  defp verk_node_key(verk_node_id), do: "verk:node:#{verk_node_id}"
  defp verk_node_queues_key(verk_node_id), do: "verk:node:#{verk_node_id}:queues"
end
