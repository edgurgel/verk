defmodule Verk.Node do
  @moduledoc """
  Node data controller
  """

  @spec dead?(String.t(), GenServer.t()) :: {:ok, boolean} | {:error, term}
  def dead?(verk_node_id, redis) do
    case Redix.command(redis, ["EXISTS", verk_node_key(verk_node_id)]) do
      {:ok, 0} -> {:ok, true}
      {:ok, 1} -> {:ok, false}
      {:error, reason} -> {:error, reason}
    end
  end

  @spec expire_in(String.t(), integer, GenServer.t()) :: {:ok, integer} | {:error, term}
  def expire_in(verk_node_id, ttl, redis) do
    Redix.command(redis, ["PSETEX", verk_node_key(verk_node_id), ttl, "alive"])
  end

  defp verk_node_key(verk_node_id), do: "verk:node:#{verk_node_id}"
end
