defmodule Verk.SortedSet do
  @moduledoc """
  This module interacts with the jobs on a sorted set
  """
  alias Verk.Job

  @doc """
  Counts how many jobs are inside the sorted set
  """
  @spec count(String.t) :: integer
  def count(key) do
    Redix.command!(Verk.Redis, ["ZCARD", key])
  end

  @doc """
  Clears the sorted set
  """
  @spec clear(String.t) :: boolean
  def clear(key) do
    Redix.command!(Verk.Redis, ["DEL", key]) == 1
  end

  @doc """
  Lists jobs from `start` to `stop`
  """
  @spec range(String.t, integer, integer) :: [Verk.Job.T]
  def range(key, start \\ 0, stop \\ -1) do
    for job <- Redix.command!(Verk.Redis, ["ZRANGE", key, start, stop]) do
      Job.decode!(job)
    end
  end

  @doc """
  Deletes the job from the sorted set
  """
  @spec delete_job(String.t, %Job{} | binary) :: boolean
  def delete_job(key, %Job{ original_json: original_json }) do
    Redix.command!(Verk.Redis, ["ZREM", key, original_json]) == 1
  end
  def delete_job(key, original_json) do
    Redix.command!(Verk.Redis, ["ZREM", key, original_json]) == 1
  end
end
