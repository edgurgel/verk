defmodule Verk.QueueStats do
  @moduledoc """
  This process will update an :ets table with the following information per queue

    * Current amount of running jobs
    * Amount of finished jobs
    * Amount of failed jobs

  It will persist to redis from time to time
  """
  use GenEvent
  require Logger
  alias Verk.QueueStatsCounters

  @persist_interval 10_000

  @doc """
  Lists the queues and their stats
  """
  @spec all :: Map.t
  def all do
    for {queue, running, finished, failed} <- QueueStatsCounters.all, is_binary(queue) do
      %{queue: queue, running_counter: running, finished_counter: finished, failed_counter: failed}
    end
  end

  @doc """
  Requests to reset started counter for a `queue`
  """
  @spec reset_started(binary) :: :ok
  def reset_started(queue) do
    GenEvent.call(Verk.EventManager, Verk.QueueStats, {:reset_started, to_string(queue)})
  end

  @doc false
  def init(_) do
    QueueStatsCounters.init
    Process.send_after(self, :persist_stats, @persist_interval)
    {:ok, nil}
  end

  @doc false
  def handle_event(%Verk.Events.JobStarted{job: job}, state) do
    QueueStatsCounters.register(:started, job.queue)
    {:ok, state}
  end

  def handle_event(%Verk.Events.JobFinished{job: job}, state) do
    QueueStatsCounters.register(:finished, job.queue)
    {:ok, state}
  end

  def handle_event(%Verk.Events.JobFailed{job: job}, state) do
    QueueStatsCounters.register(:failed, job.queue)
    {:ok, state}
  end

  @doc false
  def handle_call({:reset_started, queue}, state) do
    QueueStatsCounters.reset_started(queue)
    {:ok, :ok, state}
  end

  @doc false
  def handle_info(:persist_stats, state) do
    case QueueStatsCounters.persist do
      :ok -> :ok
      {:error, reason} ->
        Logger.error("QueueStats failed to persist stats to Redis. Reason: #{inspect reason}")
    end
    Process.send_after(self, :persist_stats, @persist_interval)
    {:ok, state}
  end
end
