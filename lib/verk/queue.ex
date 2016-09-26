defmodule Verk.Queue do
  @moduledoc """
  This module interacts with a queue
  """
  alias Verk.Job
  import Verk.Dsl

  @doc """
  Counts how many jobs are enqueued on a queue
  """
  @spec count(binary) :: {:ok, integer} |  {:error, atom | Redix.Error.t}
  def count(queue) do
    Redix.command(Verk.Redis, ["LLEN", queue_name(queue)])
  end

  @doc """
  Counts how many jobs are enqueued on a queue, raising if there's an error
  """
  @spec count!(binary) :: integer
  def count!(queue) do
    bangify(count(queue))
  end

  @doc """
  Clears the `queue`

  It will return `{:ok, true}` if the `queue` was cleared and `{:ok, false}` otherwise

  An error tuple may be returned if Redis failed
  """
  @spec clear(binary) :: {:ok, boolean} | {:error, Redix.Error.t}
  def clear(queue) do
    case Redix.command(Verk.Redis, ["DEL", queue_name(queue)]) do
      {:ok, 0} -> {:ok, false}
      {:ok, 1} -> {:ok, true}
      {:error, error} -> {:error, error}
    end
  end

  @doc """
  Clears the `queue`, raising if there's an error

  It will return `true` if the `queue` was cleared and `false` otherwise
  """
  @spec clear!(binary) :: boolean
  def clear!(queue) do
    bangify(clear(queue))
  end

  @doc """
  Lists enqueued jobs from `start` to `stop`
  """
  @spec range(binary, integer, integer) :: {:ok, [Verk.Job.T]} | {:error, Redix.Error.t}
  def range(queue, start \\ 0, stop \\ -1) do
    case Redix.command(Verk.Redis, ["LRANGE", queue_name(queue), start, stop]) do
      {:ok, jobs} -> {:ok, (for job <- jobs, do: Job.decode!(job))}
      {:error, error} -> {:error, error}
    end
  end

  @doc """
  Lists enqueued jobs from `start` to `stop`, raising if there's an error
  """
  @spec range!(binary, integer, integer) :: [Verk.Job.T]
  def range!(queue, start \\ 0, stop \\ -1) do
    bangify(range(queue, start, stop))
  end

  @doc """
  Deletes the job from the `queue`

  It returns `{:ok, true}` if the job was found and deleted
  Otherwise it returns `{:ok, false}`

  An error tuple may be returned if Redis failed
  """
  @spec delete_job(binary, %Job{} | binary) :: {:ok, boolean} | {:error, Redix.Error.t}
  def delete_job(queue, %Job{original_json: original_json}) do
    delete_job(queue, original_json)
  end

  def delete_job(queue, original_json) do
    case Redix.command(Verk.Redis, ["LREM", queue_name(queue), 1, original_json]) do
      {:ok, 0} -> {:ok, false}
      {:ok, 1} -> {:ok, true}
      {:error, error} -> {:error, error}
    end
  end

  @doc """
  Delete job from the `queue`, raising if there's an error

  It returns `true` if the job was found and delete
  Otherwise it returns `false`

  An error will be raised if Redis failed
  """
  @spec delete_job!(binary, %Job{} | binary) :: boolean
  def delete_job!(queue, %Job{original_json: original_json}) do
    delete_job!(queue, original_json)
  end

  def delete_job!(queue, original_json) do
    bangify(delete_job(queue, original_json))
  end

  defp queue_name(queue), do: "queue:#{queue}"
end
