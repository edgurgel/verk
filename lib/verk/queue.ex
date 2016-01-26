defmodule Verk.Queue do
  @moduledoc """
  This module interacts with a queue
  """
  alias Verk.Job

  @doc """
  Counts how many jobs are enqueued on a queue
  """
  @spec count(binary) :: integer
  def count(queue) do
    Redix.command!(Verk.Redis, ["LLEN", queue_name(queue)])
  end

  @doc """
  Clears the `queue`
  """
  @spec clear(binary) :: boolean
  def clear(queue) do
    Redix.command!(Verk.Redis, ["DEL", queue_name(queue)]) == 1
  end

  @doc """
  Lists enqueued jobs from `start` to `stop`
  """
  @spec range(binary, integer, integer) :: [Verk.Job.T]
  def range(queue, start \\ 0, stop \\ -1) do
    for job <- Redix.command!(Verk.Redis, ["LRANGE", queue_name(queue), start, stop]) do
      Job.decode!(job)
    end
  end

  @doc """
  Deletes the job from the queue
  """
  @spec delete_job(binary, %Job{} | binary) :: boolean
  def delete_job(queue, %Job{ original_json: original_json }) do
    Redix.command!(Verk.Redis, ["LREM", queue_name(queue), 1, original_json]) == 1
  end
  def delete_job(queue, original_json) do
    Redix.command!(Verk.Redis, ["LREM", queue_name(queue), 1, original_json]) == 1
  end

  defp queue_name(queue), do: "queue:#{queue}"
end
