defmodule Verk.SortedSet do
  @moduledoc """
  This module interacts with the jobs on a sorted set
  """
  import Verk.Dsl
  alias Verk.Job

  @doc """
  Counts how many jobs are inside the sorted set
  """
  @spec count(String.t, GenServer.server) :: {:ok, integer} | {:error, Redix.Error.t}
  def count(key, redis) do
    Redix.command(redis, ["ZCARD", key])
  end


  @doc """
  Counts how many jobs are inside the sorted set, raising if there's an error
  """
  @spec count!(String.t, GenServer.server) :: integer
  def count!(key, redis) do
    bangify(count(key, redis))
  end

  @doc """
  Clears the sorted set
  """
  @spec clear(String.t, GenServer.server) :: :ok | {:error, RuntimeError.t | Redix.Error.t}
  def clear(key, redis) do
    case Redix.command(redis, ["DEL", key]) do
      {:ok, 0} -> {:error, %RuntimeError{message: ~s(Key "#{key}" not found.)}}
      {:ok, 1} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  @doc """
  Clears the sorted set, raising if there's an error
  """
  @spec clear!(String.t, GenServer.server) :: nil
  def clear!(key, redis) do
    bangify(clear(key, redis))
  end

  @doc """
  Lists jobs from `start` to `stop`
  """
  @spec range(String.t, integer, integer, GenServer.server) :: {:ok, [Verk.Job.T]} | {:error, Redix.Error.t}
  def range(key, start \\ 0, stop \\ -1, redis) do
    case Redix.command(redis, ["ZRANGE", key, start, stop]) do
      {:ok, jobs} -> {:ok, (for job <- jobs, do: Job.decode!(job))}
      {:error, error} -> {:error, error}
    end
  end

  @doc """
  Lists jobs from `start` to `stop`, raising if there's an error
  """
  @spec range!(String.t, integer, integer, GenServer.server) :: nil
  def range!(key, start \\ 0, stop \\ -1, redis) do
    bangify(range(key, start, stop, redis))
  end

  @doc """
  Deletes the job from the sorted set
  """
  @spec delete_job(String.t, %Job{} | String.t, GenServer.server) :: :ok | {:error, RuntimeError.t | Redix.Error.t}
  def delete_job(key, %Job{ original_json: original_json }, redis) do
    delete_job(key, original_json, redis)
  end

  def delete_job(key, original_json, redis) do
    case Redix.command(redis, ["ZREM", key, original_json]) do
      {:ok, 0} -> {:error, %RuntimeError{message: ~s(Key "#{key}" not found.)}}
      {:ok, 1} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  @doc """
  Deletes the job from the sorted set, raising if there's an error
  """
  @spec delete_job!(String.t, %Job{} | String.t, GenServer.server) :: nil
  def delete_job!(key, %Job{ original_json: original_json }, redis) do
    delete_job!(key, original_json, redis)
  end

  def delete_job!(key, original_json, redis) do
    bangify(delete_job(key, original_json, redis))
  end
end
