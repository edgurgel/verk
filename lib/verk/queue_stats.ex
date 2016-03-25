defmodule Verk.QueueStats do
  @moduledoc """
  This process will update an :ets table with the following information per queue

    * Current amount of running jobs
    * Amount of finished jobs
    * Amount of failed jobs

  The saved tuples are of this form:
  { queue_name, running_jobs_counter, finished_jobs_counter, failed_jobs_counter }
  """
  use GenEvent
  alias Verk.QueueStatsCounters

  @persist_interval 10_000

  @doc false
  def init(_) do
    QueueStatsCounters.init
    Process.send_after(self(), :persist_stats, @persist_interval)
    { :ok, nil }
  end

  @doc false
  def handle_event(%Verk.Events.JobStarted{ job: job }, state) do
    QueueStatsCounters.register(:started, job.queue)
    { :ok, state }
  end

  def handle_event(%Verk.Events.JobFinished{ job: job }, state) do
    QueueStatsCounters.register(:finished, job.queue)
    { :ok, state }
  end

  def handle_event(%Verk.Events.JobFailed{ job: job }, state) do
    QueueStatsCounters.register(:failed, job.queue)
    { :ok, state }
  end

  @doc false
  def handle_info(:persist_stats, state) do
    QueueStatsCounters.persist
    {:noreply, state}
  end
end
