defmodule Verk.SortedSet do
  @moduledoc """
  This module interacts with the jobs on a sorted set
  """
  import Verk.Dsl
  alias Verk.Job

  @requeue_now_script Verk.Scripts.sha("requeue_job_now")

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

  It will return `{:ok, true}` if the sorted set was cleared and `{:ok, false}` otherwise

  An error tuple may be returned if Redis failed
  """
  @spec clear(String.t, GenServer.server) :: {:ok, boolean} | {:error, Redix.Error.t}
  def clear(key, redis) do
    case Redix.command(redis, ["DEL", key]) do
      {:ok, 0} -> {:ok, false}
      {:ok, 1} -> {:ok, true}
      {:error, error} -> {:error, error}
    end
  end

  @doc """
  Clears the sorted set, raising if there's an error

  It will return `true` if the sorted set was cleared and `false` otherwise
  """
  @spec clear!(String.t, GenServer.server) :: boolean
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
  Lists jobs from `start` to `stop` along with the item scores
  """
  @spec range_with_score(String.t, integer, integer, GenServer.server)
    :: {:ok, [{Verk.Job.T, integer}]} | {:error, Redix.Error.t}
  def range_with_score(key, start \\ 0, stop \\ -1, redis) do
    case Redix.command(redis, ["ZRANGE", key, start, stop, "WITHSCORES"]) do
      {:ok, jobs} ->
        # The Redis returned list alternates, [<job>, <job score>, ...].
        jobs_with_scores =
          jobs
          |> Enum.chunk(2)
          |> Enum.into([], fn [job, score] -> {Job.decode!(job), String.to_integer(score)} end)
        {:ok, jobs_with_scores}
      {:error, error} -> {:error, error}
    end
  end

  @doc """
  Lists jobs from `start` to `stop` along with the item scores, raising if there's an error
  """
  @spec range_with_score!(String.t, integer, integer, GenServer.server) :: nil
  def range_with_score!(key, start \\ 0, stop \\ -1, redis) do
    bangify(range_with_score(key, start, stop, redis))
  end

  @doc """
  Deletes the job from the sorted set

  It returns `{:ok, true}` if the job was found and deleted
  Otherwise it returns `{:ok, false}`

  An error tuple may be returned if Redis failed
  """
  @spec delete_job(String.t, %Job{} | String.t, GenServer.server) :: {:ok, boolean} | {:error, Redix.Error.t}
  def delete_job(key, %Job{original_json: original_json}, redis) do
    delete_job(key, original_json, redis)
  end

  def delete_job(key, original_json, redis) do
    case Redix.command(redis, ["ZREM", key, original_json]) do
      {:ok, 0} -> {:ok, false}
      {:ok, 1} -> {:ok, true}
      {:error, error} -> {:error, error}
    end
  end

  @doc """
  Deletes the job from the sorted set, raising if there's an error

  It returns `true` if the job was found and delete
  Otherwise it returns `false`
  """
  @spec delete_job!(String.t, %Job{} | String.t, GenServer.server) :: boolean
  def delete_job!(key, %Job{original_json: original_json}, redis) do
    delete_job!(key, original_json, redis)
  end

  def delete_job!(key, original_json, redis) do
    bangify(delete_job(key, original_json, redis))
  end

  @doc """
  Moves the job from the sorted set back to its original queue

  It returns `{:ok, true}` if the job was found and requeued
  Otherwise it returns `{:ok, false}`

  An error tuple may be returned if Redis failed
  """
  @spec requeue_job(String.t, %Job{} | String.t, GenServer.server) :: {:ok, boolean} | {:error, Redix.Error.t}
  def requeue_job(key, %Job{original_json: original_json}, redis) do
    requeue_job(key, original_json, redis)
  end

  def requeue_job(key, original_json, redis) do
    case Redix.command(redis, ["EVALSHA", @requeue_now_script, 1, key, original_json]) do
      {:ok, nil} -> {:ok, false}
      {:ok, _job} -> {:ok, true}
      {:error, %Redix.Error{message: message}} ->
        {:error, message}
      error ->
        {:error, error}
    end
  end

  @doc """
  Moves the job from the sorted set back to its original queue, raising if there's an error

  It returns `true` if the job was found and requeued
  Otherwise it returns `false`
  """
  @spec requeue_job!(String.t, %Job{} | String.t, GenServer.server) :: boolean
  def requeue_job!(key, %Job{original_json: original_json}, redis) do
    requeue_job!(key, original_json, redis)
  end

  def requeue_job!(key, original_json, redis) do
    bangify(requeue_job(key, original_json, redis))
  end
end
