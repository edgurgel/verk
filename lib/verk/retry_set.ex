defmodule Verk.RetrySet do
  @retry_key "retry"

  @doc """
  Counts how many jobs are inside the retry set
  """
  @spec count :: integer
  def count do
    Redix.command!(Verk.Redis, ["ZCARD", @retry_key])
  end

  @doc """
  Lists jobs from `start` to `stop`
  """
  @spec range(integer, integer) :: [Verk.Job.T]
  def range(start \\ 0, stop \\ -1) do
    for job <- Redix.command!(Verk.Redis, ["ZRANGE", @retry_key, start, stop]) do
      Poison.decode!(job, as: Verk.Job)
    end
  end
end
