defmodule Verk.SortedSet do
  @moduledoc """
  This module interacts with the jobs on a sorted set
  """
  alias Verk.Job

  @doc """
  Counts how many jobs are inside the sorted set
  """
  @spec count(String.t, GenServer.server) :: integer
  def count(key, redis) do
    Redix.command!(redis, ["ZCARD", key])
  end

  @doc """
  Clears the sorted set
  """
  @spec clear(String.t, GenServer.server) :: boolean
  def clear(key, redis) do
    Redix.command!(redis, ["DEL", key]) == 1
  end

  @doc """
  Lists jobs from `start` to `stop`
  """
  @spec range(String.t, integer, integer, GenServer.server) :: [Verk.Job.T]
  def range(key, start \\ 0, stop \\ -1, redis) do
    for job <- Redix.command!(redis, ["ZRANGE", key, start, stop]) do
      Job.decode!(job)
    end
  end

  @doc """
  Deletes the job from the sorted set
  """
  @spec delete_job(String.t, %Job{} | String.t, GenServer.server) :: boolean
  def delete_job(key, %Job{ original_json: original_json }, redis) do
    delete_job(key, original_json, redis)
  end
  def delete_job(key, original_json, redis) do
    Redix.command!(redis, ["ZREM", key, original_json]) == 1
  end
end
