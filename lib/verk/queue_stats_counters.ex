defmodule Verk.QueueStatsCounters do
  @moduledoc """
  This module is responsible for abstracting the logic of keeping counters for
  each queue.
  """

  @counters :queue_stats
  @ets_options [:ordered_set, :named_table, read_concurrency: true, keypos: 1]

  @doc """
  Initializes the ets tables for the queue stats.
  """
  @spec init :: :ok
  def init do
    :ets.new(@counters, @ets_options)
    :ok
  end

  @doc """
  Updates the counters according to the event that happened.
  """
  @spec register(:started | :finished | :failed, binary) :: integer
  def register(:started, queue), do: update_counters(queue, { 2, 1 })
  def register(:finished, queue), do: update_counters(queue, [{ 3, 1 }, { 2, -1 }])
  def register(:failed, queue), do: update_counters(queue, [{ 4, 1 }, { 2, -1 }])

  @doc """
  Saves processed and failed total counts to Redis.
  """
  @spec persist :: {:ok, [Redix.Protocol.redis_value]} | {:error, atom}
  def persist do
    {total_processed, total_failed} = Enum.reduce stats, {0, 0}, fn {queue, processed, failed}, {accum_processed, accum_failed} ->
      redis_commands = [
        incrby(queue, :processed, processed),
        incrby(queue, :failed, failed)
      ]
      case Redix.pipeline(Verk.Redis, redis_commands) do
        {:ok, [%Redix.Error{}, %Redix.Error{}]} ->
          {accum_processed, accum_failed}
        {:ok, [_, %Redix.Error{}]} ->
          update_counters(queue, { 3, -processed })
          {accum_processed + processed, accum_failed}
        {:ok, [%Redix.Error{}, _]} ->
          update_counters(queue, { 4, -failed })
          {accum_processed, accum_failed + failed}
        {:ok, [_, _]} ->
          update_counters(queue, [{ 3, -processed }, { 4, -failed }])
          {accum_processed + processed, accum_failed + failed}
        _error ->
          {accum_processed, accum_failed}
      end
    end

    redis_commands = [
      incrby(:total, :processed, total_processed),
      incrby(:total, :failed, total_failed)
    ]
    Redix.pipeline(Verk.Redis, redis_commands)
  end

  defp update_counters(queue, operations) do
    :ets.update_counter(@counters, queue, operations, { queue, 0, 0, 0 })
  end

  defp stats do
    :ets.select(@counters, [{{:"$1", :_, :"$2", :"$3"}, [], [{{:"$1", :"$2", :"$3"}}]}])
  end

  defp incrby(:total, attribute, increment), do: ["INCRBY", "stat:#{attribute}", increment]
  defp incrby(queue, attribute, increment), do: ["INCRBY", "stat:#{attribute}:#{queue}", increment]
end
