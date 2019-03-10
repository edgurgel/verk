defmodule Verk.QueueList do
  @moduledoc """
  QueueList maintains a list of running queues
  """

  @queue_list_key "verk:queues"

  @spec all(binary, GenServer.server()) :: {:ok, [binary]} | {:error, term}
  def all(prefix \\ "", redis \\ Verk.Redis) do
    case Redix.command(redis, ["SMEMBERS", @queue_list_key]) do
      {:ok, queues} ->
        queues = Enum.filter(queues, &String.starts_with?(&1, prefix))
        {:ok, queues}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec add(binary, GenServer.server()) :: {:ok, non_neg_integer} | {:error, term}
  def add(queue, redis \\ Verk.Redis) do
    Redix.command(redis, ["SADD", @queue_list_key, queue])
  end
end
