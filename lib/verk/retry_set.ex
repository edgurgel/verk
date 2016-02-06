defmodule Verk.RetrySet do
  @moduledoc """
  This module interacts with jobs in the retry set
  """
  alias Verk.SortedSet
  alias Verk.Job

  @retry_key "retry"

  def key, do: @retry_key

  @doc """
  Counts how many jobs are inside the retry set
  """
  @spec count(GenServer.Server) :: integer
  def count(redis \\ Verk.Redis), do: SortedSet.count(@retry_key, redis)

  @doc """
  Clears the retry set
  """
  @spec clear(GenServer.server) :: boolean
  def clear(redis \\ Verk.Redis), do: SortedSet.clear(@retry_key, redis)

  @doc """
  List jobs from `start` to `stop`
  """
  @spec range(integer, integer, GenServer.server) :: [Verk.Job.T]
  def range(start \\ 0, stop \\ -1, redis \\ Verk.Redis) do
    SortedSet.range(@retry_key, start, stop, redis)
  end

  @doc """
  Delete the job from the retry set
  """
  @spec delete_job(%Job{} | String.t, GenServer.server) :: boolean
  def delete_job(original_json, redis \\ Verk.Redis)
  def delete_job(%Job{ original_json: original_json }, redis) do
    delete_job(original_json, redis)
  end
  def delete_job(original_json, redis), do: SortedSet.delete_job(@retry_key, original_json, redis)
end
