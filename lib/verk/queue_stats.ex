defmodule Verk.QueueStats do
  @moduledoc """
  This process will update an :ets table with the following information per queue

    * Current amount of running jobs
    * Amount of finished jobs
    * Amount of failed jobs

  It will persist to redis from time to time
  """
  use GenStage
  require Logger
  alias Verk.QueueStatsCounters

  @persist_interval 10_000

  @doc false
  def start_link() do
    GenStage.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @doc """
  Lists the queues and their stats searching for a `prefix` if provided
  """
  @spec all(binary) :: Map.t
  def all(prefix \\ "") do
    for {queue, running, finished, failed} <- QueueStatsCounters.all(prefix), is_list(queue) do
      %{queue: to_string(queue), running_counter: running, finished_counter: finished, failed_counter: failed}
    end
  end

  @doc """
  Requests to reset started counter for a `queue`
  """
  @spec reset_started(binary) :: :ok
  def reset_started(queue) do
    GenStage.call(__MODULE__, {:reset_started, to_string(queue)})
  end

  @doc false
  def init(_) do
    QueueStatsCounters.init
    Process.send_after(self(), :persist_stats, @persist_interval)
    {:consumer, :ok, subscribe_to: [Verk.EventProducer]}
  end

  def handle_events(events, _from, state) do
    for event <- events, do: handle_event(event)
    {:noreply, [], state}
  end

  @doc false
  defp handle_event(%Verk.Events.JobStarted{job: job}) do
    QueueStatsCounters.register(:started, job.queue)
  end

  defp handle_event(%Verk.Events.JobFinished{job: job}) do
    QueueStatsCounters.register(:finished, job.queue)
  end

  defp handle_event(%Verk.Events.JobFailed{job: job}) do
    QueueStatsCounters.register(:failed, job.queue)
  end

  @doc false
  def handle_call({:reset_started, queue}, _from, state) do
    QueueStatsCounters.reset_started(queue)
    {:reply, :ok, [], state}
  end

  @doc false
  def handle_info(:persist_stats, state) do
    case QueueStatsCounters.persist do
      :ok -> :ok
      {:error, reason} ->
        Logger.error("QueueStats failed to persist stats to Redis. Reason: #{inspect reason}")
    end
    Process.send_after(self(), :persist_stats, @persist_interval)
    {:noreply, [], state}
  end

  @doc false
  def handle_info(_, state) do
    {:noreply, [], state}
  end
end
