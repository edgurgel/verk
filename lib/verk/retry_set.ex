defmodule Verk.RetrySet do
  @moduledoc """
  This module interacts with jobs in the retry set
  """
  import Verk.Dsl
  alias Verk.SortedSet
  alias Verk.Job

  @retry_key "retry"

  @doc "Redis retry set key"
  def key, do: @retry_key

  @doc """
  Adds a `job` to the retry set ordering by `timestamp`

  Optionally a redis connection can be specified
  """
  @spec add(%Job{}, integer,  GenServer.server) :: :ok | {:error, Redix.Error.t}
  def add(job, failed_at, redis \\ Verk.Redis) do
    retry_at = calculate_retry_at(failed_at, job.retry_count)
    case Redix.command(redis, ["ZADD", @retry_key, to_string(retry_at), Poison.encode!(job)]) do
      {:ok, _} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  @doc """
  Adds a `job` to the retry set ordering by `timestamp`, raising if there's an error

  Optionally a redis connection can be specified
  """
  @spec add!(%Job{}, integer,  GenServer.server) :: nil
  def add!(job, failed_at, redis \\ Verk.Redis) do
    bangify(add(job, failed_at, redis))
  end

  defp calculate_retry_at(failed_at, retry_count) do
    delay = :math.pow(retry_count, 4) + 15 + (:random.uniform(30) * (retry_count + 1))
    failed_at + delay
  end

  @doc """
  Counts how many jobs are inside the retry set
  """
  @spec count(GenServer.Server) :: {:ok, integer} | {:error, Redix.Error.t}
  def count(redis \\ Verk.Redis), do: SortedSet.count(@retry_key, redis)

  @doc """
  Counts how many jobs are inside the retry set, raising if there's an error
  """
  @spec count!(GenServer.Server) :: integer
  def count!(redis \\ Verk.Redis), do: SortedSet.count!(@retry_key, redis)

  @doc """
  Clears the retry set
  """
  @spec clear(GenServer.server) :: :ok | {:error, RuntimeError.t | Redix.Error.t}
  def clear(redis \\ Verk.Redis), do: SortedSet.clear(@retry_key, redis)

  @doc """
  Clears the retry set, raising if there's an error
  """
  @spec clear!(GenServer.server) :: nil
  def clear!(redis \\ Verk.Redis), do: SortedSet.clear!(@retry_key, redis)

  @doc """
  List jobs from `start` to `stop`
  """
  @spec range(integer, integer, GenServer.server) :: {:ok, [Verk.Job.T]} | {:error, Redix.Error.t}
  def range(start \\ 0, stop \\ -1, redis \\ Verk.Redis) do
    SortedSet.range(@retry_key, start, stop, redis)
  end

  @doc """
  List jobs from `start` to `stop`, raising if there's an error
  """
  @spec range!(integer, integer, GenServer.server) :: [Verk.Job.T]
  def range!(start \\ 0, stop \\ -1, redis \\ Verk.Redis) do
    SortedSet.range!(@retry_key, start, stop, redis)
  end

  @doc """
  Delete the job from the retry set
  """
  @spec delete_job(%Job{} | String.t, GenServer.server) :: :ok | {:error, RuntimeError.t | Redix.Error.t}
  def delete_job(original_json, redis \\ Verk.Redis)
  def delete_job(%Job{original_json: original_json}, redis) do
    delete_job(original_json, redis)
  end
  def delete_job(original_json, redis), do: SortedSet.delete_job(@retry_key, original_json, redis)

  @doc """
  Delete the job from the retry set, raising if there's an error
  """
  @spec delete_job!(%Job{} | String.t, GenServer.server) :: nil
  def delete_job!(original_json, redis \\ Verk.Redis)
  def delete_job!(%Job{original_json: original_json}, redis) do
    delete_job!(original_json, redis)
  end
  def delete_job!(original_json, redis), do: SortedSet.delete_job!(@retry_key, original_json, redis)
end
