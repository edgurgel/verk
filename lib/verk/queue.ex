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
  """
  @spec clear(binary) :: :ok | {:error, RuntimeError.t | Redix.Error.t}
  def clear(queue) do
    case Redix.command(Verk.Redis, ["DEL", queue_name(queue)]) do
      {:ok, 0} -> {:error, %RuntimeError{message: ~s(Queue "#{queue}" not found.)}}
      {:ok, 1} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  @doc """
  Clears the `queue`, raising if there's an error
  """
  @spec clear!(binary) :: nil
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
  Deletes the job from the queue
  """
  @spec delete_job(binary, %Job{} | binary) :: :ok | {:error, RuntimeError.t | Redix.Error.t}
  def delete_job(queue, %Job{ original_json: original_json }) do
    delete_job(queue, original_json)
  end

  def delete_job(queue, original_json) do
    case Redix.command(Verk.Redis, ["LREM", queue_name(queue), 1, original_json]) do
      {:ok, 0} -> {:error, %RuntimeError{message: ~s(Job not found in queue "#{queue}".)}}
      {:ok, 1} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  @doc """
  Delete job from the queue, raising if there's an error
  """
  @spec delete_job!(binary, %Job{} | binary) :: nil
  def delete_job!(queue, %Job{ original_json: original_json }) do
    delete_job!(queue, original_json)
  end

  def delete_job!(queue, original_json) do
    bangify(delete_job(queue, original_json))
  end

  defp queue_name(queue), do: "queue:#{queue}"
end
