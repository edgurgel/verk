defmodule Verk.QueueStatsCounters do
  @moduledoc """
  This module is responsible for abstracting the logic of keeping counters for
  each queue.
  """

  @counters_table :queue_stats
  @ets_options [:ordered_set, :named_table, read_concurrency: true, keypos: 1]

  @doc """
  Initializes the ets tables for the queue stats.
  """
  @spec init :: :ok
  def init do
    :ets.new(@counters_table, @ets_options)
    :ok
  end

  @doc """
  It outputs the current stats about each queue and `total`
  """
  def all do
    :ets.select(@counters_table, [{ { :"$1", :"$2", :"$3", :"$4", :_, :_ }, [],
                           [{ { :"$1", :"$2", :"$3", :"$4" } }] }])
  end

  @doc """
  Updates the counters according to the event that happened.
  """
  @spec register(:started | :finished | :failed, binary) :: integer
  def register(:started, queue) do
    update = { 2, 1 }
    update_counters(queue, update)
    update_counters(:total, update)
  end
  def register(:finished, queue) do
    updates = [{ 3, 1 }, { 2, -1 }]
    update_counters(queue, updates)
    update_counters(:total, updates)
  end
  def register(:failed, queue) do
    updates = [{ 4, 1 }, { 2, -1 }]
    update_counters(queue, updates)
    update_counters(:total, updates)
  end

  @doc """
  Saves processed and failed total counts to Redis.
  """
  @spec persist :: :ok | { :error, term }
  def persist do
    cmds = Enum.reduce(counters, [], fn { queue, _started, processed, failed, last_processed, last_failed }, commands ->
      delta_processed = processed - last_processed
      delta_failed    = failed - last_failed
      :ets.update_counter(@counters_table, queue, [{ 5, delta_processed },
                                             { 6, delta_failed }])

      [incrby(queue, :processed, delta_processed) | [incrby(queue, :failed, delta_failed) | commands]]
    end)
    Enum.reject(cmds, &(&1 == nil)) |> flush_to_redis!
  end

  defp flush_to_redis!([]), do: :ok
  defp flush_to_redis!(cmds) do
    case Redix.pipeline(Verk.Redis, cmds) do
      { :ok, _ } -> :ok
      { :error, reason } -> { :error, reason }
    end
  end

  defp counters, do: :ets.tab2list(@counters_table)

  # started, finished, failed, last_started, last_failed
  defp update_counters(queue, operations) do
    :ets.update_counter(@counters_table, queue, operations, { queue, 0, 0, 0, 0, 0 })
  end

  defp incrby(_, _, 0), do: nil
  defp incrby(:total, attribute, increment) do
    ["INCRBY", "stat:#{attribute}", increment]
  end
  defp incrby(queue, attribute, increment) do
    ["INCRBY", "stat:#{attribute}:#{queue}", increment]
  end
end
