defmodule Verk.RetrySet do
  @retry_key "retry"

  @doc """
  Count how many jobs are inside the retry set
  """
  @spec count(pid) :: integer
  def count(redis) do
    Redix.command!(redis, ["ZCARD", @retry_key])
  end

  @doc """
  List jobs from `start` to `stop`
  """
  @spec range(pid, integer, integer) :: [Verk.Job.T]
  def range(redis, start \\ 0, stop \\ -1) do
    for job <- Redix.command!(redis, ["ZRANGE", @retry_key, start, stop]) do
      Poison.decode!(job, as: Verk.Job)
    end
  end
end
