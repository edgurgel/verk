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

  @table :queue_stats

  @doc """
  Lists the queues and their stats
  """
  def all do
    for { queue, running, finished, failed } <- :ets.tab2list(@table) do
      %{ queue: queue, running_counter: running, finished_counter: finished, failed_counter: failed }
    end
  end

  @doc false
  def init(_) do
    :ets.new(@table, [:ordered_set, :named_table, read_concurrency: true, keypos: 1])
    { :ok, nil }
  end

  @doc false
  def handle_event(%Verk.Events.JobStarted{ job: job }, state) do
    :ets.update_counter(@table, job.queue, { 2, 1 }, default_tuple(job.queue))
    { :ok, state }
  end

  def handle_event(%Verk.Events.JobFinished{ job: job }, state) do
    :ets.update_counter(@table, job.queue, [{ 3, 1 }, { 2, -1 }], default_tuple(job.queue))
    { :ok, state }
  end

  def handle_event(%Verk.Events.JobFailed{ job: job }, state) do
    :ets.update_counter(@table, job.queue, [{ 4, 1 }, { 2, -1 }], default_tuple(job.queue))
    { :ok, state }
  end

  defp default_tuple(queue), do: { queue, 0, 0, 0 }
end
