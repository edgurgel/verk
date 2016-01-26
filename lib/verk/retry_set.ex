defmodule Verk.RetrySet do
  @moduledoc """
  This module interacts with the jobs on the retry set
  """
  alias Verk.Job

  @retry_key "retry"

  @doc """
  Counts how many jobs are inside the retry set
  """
  @spec count :: integer
  def count do
    Redix.command!(Verk.Redis, ["ZCARD", @retry_key])
  end

  @doc """
  Clears the retry set
  """
  @spec clear :: boolean
  def clear do
    Redix.command!(Verk.Redis, ["DEL", @retry_key]) == 1
  end

  @doc """
  Lists jobs from `start` to `stop`
  """
  @spec range(integer, integer) :: [Verk.Job.T]
  def range(start \\ 0, stop \\ -1) do
    for job <- Redix.command!(Verk.Redis, ["ZRANGE", @retry_key, start, stop]) do
      Job.decode!(job)
    end
  end

  @doc """
  Deletes the job from the retry set
  """
  @spec delete_job(%Job{} | binary) :: boolean
  def delete_job(%Job{ original_json: original_json }) do
    Redix.command!(Verk.Redis, ["ZREM", @retry_key, original_json]) == 1
  end
  def delete_job(original_json) do
    Redix.command!(Verk.Redis, ["ZREM", @retry_key, original_json]) == 1
  end
end
