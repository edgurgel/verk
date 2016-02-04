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
  @spec count :: integer
  def count, do: SortedSet.count(@retry_key)

  @doc """
  Clears the retry set
  """
  @spec clear :: boolean
  def clear, do: SortedSet.clear(@retry_key)

  @doc """
  List jobs from `start` to `stop`
  """
  @spec range(integer, integer) :: [Verk.Job.T]
  def range(start \\ 0, stop \\ -1), do: SortedSet.range(@retry_key, start, stop)

  @doc """
  Delete the job from the retry set
  """
  @spec delete_job(%Job{} | String.t) :: boolean
  def delete_job(%Job{ original_json: original_json }) do
    SortedSet.delete_job(@retry_key, original_json)
  end
  def delete_job(original_json), do: SortedSet.delete_job(@retry_key, original_json)
end
