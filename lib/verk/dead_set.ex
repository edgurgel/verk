defmodule Verk.DeadSet do
  @moduledoc """
  This module interacts with jobs in the dead set
  """
  alias Verk.SortedSet
  alias Verk.Job

  @max_jobs 100
  @timeout 60 * 60 * 24 * 7 # a week

  @dead_key "dead"

  @doc "Redis dead set key"
  def key, do: @dead_key

  @doc """
  Adds a `job` to the dead set ordering by `timestamp`

  Optionally a redis connection can be specified
  """
  def add(job, timestamp, redis \\ Verk.Redis) do
    case Redix.pipeline(redis, [["ZADD", @dead_key, timestamp, Poison.encode!(job)],
                                ["ZREMRANGEBYSCORE", @dead_key, "-inf", timestamp - @timeout],
                                ["ZREMRANGEBYRANK", @dead_key, 0, -@max_jobs]]) do
      { :ok, _ } -> :ok
      error -> error
    end
  end

  @doc """
  Counts how many jobs are inside the dead set
  """
  @spec count(GenServer.Server) :: integer
  def count(redis \\ Verk.Redis), do: SortedSet.count(@dead_key, redis)

  @doc """
  Clears the dead set
  """
  @spec clear(GenServer.server) :: boolean
  def clear(redis \\ Verk.Redis), do: SortedSet.clear(@dead_key, redis)

  @doc """
  List jobs from `start` to `stop`
  """
  @spec range(integer, integer, GenServer.server) :: [Verk.Job.T]
  def range(start \\ 0, stop \\ -1, redis \\ Verk.Redis) do
    SortedSet.range(@dead_key, start, stop, redis)
  end

  @doc """
  Delete the job from the dead set
  """
  @spec delete_job(%Job{} | String.t, GenServer.server) :: boolean
  def delete_job(original_json, redis \\ Verk.Redis)
  def delete_job(%Job{ original_json: original_json }, redis) do
    delete_job(original_json, redis)
  end
  def delete_job(original_json, redis), do: SortedSet.delete_job(@dead_key, original_json, redis)
end
