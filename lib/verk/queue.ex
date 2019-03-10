defmodule Verk.Queue do
  @moduledoc """
  This module interacts with a queue
  """
  alias Verk.Job
  alias Verk.Redis.EntryID
  import Verk.Dsl

  @external_resource "#{:code.priv_dir(:verk)}/reenqueue_pending_job.lua"
  @reenqueue_pending_job_script_sha Verk.Scripts.sha("reenqueue_pending_job")

  @doc false
  def queue_name(queue) do
    "verk:queue:#{queue}"
  end

  @doc """
  Enqueue job
  """
  @spec enqueue(Job.t(), GenServer.server()) :: {:ok, binary} | {:error, atom | Redix.Error.t()}
  def enqueue(job, redis \\ Verk.Redis) do
    encoded_job = Job.encode!(job)
    Redix.command(redis, ["XADD", queue_name(job.queue), "*", "job", encoded_job])
  end

  @doc """
  Enqueue job, raising if there's an error
  """
  @spec enqueue!(Job.t(), GenServer.server()) :: binary
  def enqueue!(job, redis \\ Verk.Redis) do
    bangify(enqueue(job, redis))
  end

  @doc """
  Consume max of `count` jobs from `queue` identifying as `consumer_id`
  """
  @spec consume(binary, binary, binary, pos_integer, pos_integer, GenServer.server()) ::
          {:ok, term} | {:error, Redix.Error.t()}
  def consume(queue, consumer_id, last_id, count, timeout \\ 30_000, redis \\ Verk.Redis) do
    command = [
      "XREADGROUP",
      "GROUP",
      "verk",
      consumer_id,
      "COUNT",
      count,
      "BLOCK",
      timeout,
      "STREAMS",
      queue_name(queue),
      last_id
    ]

    case Redix.command(redis, command, timeout: timeout + 5000) do
      {:ok, [[_, jobs]]} -> {:ok, jobs}
      result -> result
    end
  end

  @doc """
  List node ids that have pending jobs
  """
  def pending_node_ids(queue, redis \\ Verk.Redis) do
    case Redix.command(redis, ["XPENDING", queue_name(queue), "verk"]) do
      {:ok, [_, _, _, nil]} -> {:ok, []}
      {:ok, [_, _, _, nodes]} -> {:ok, Enum.map(nodes, &List.first(&1))}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  List pending jobs from a queue being consumed by a node_id
  It returns a list of tuples like this: {job_id, idle_time}
  """
  @spec pending_job_ids(binary, binary, non_neg_integer, GenServer.server()) ::
          {:ok, [{binary, non_neg_integer}]} | {:error, atom | Redix.Error.t()}
  def pending_job_ids(queue, node_id, count, redis \\ Verk.Redis) do
    case Redix.command(redis, ["XPENDING", queue_name(queue), "verk", "-", "+", count, node_id]) do
      {:ok, result} ->
        {:ok, Enum.map(result, fn [job_id, _, idle_time, _] -> {job_id, idle_time} end)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  List pending jobs from a queue
  """
  @spec pending_jobs(binary, non_neg_integer, GenServer.server()) ::
          {:ok, [{binary, non_neg_integer}]} | {:error, atom | Redix.Error.t()}
  def pending_jobs(queue, start \\ "0-0", stop \\ "+", count \\ 25, redis \\ Verk.Redis) do
    queue_name = queue_name(queue)

    with {:ok, {xrange_min, xrange_max}} <- pending_range(queue_name, start, stop, redis),
         {:ok, entries_and_jobs} <-
           Redix.command(redis, [
             "XRANGE",
             queue_name,
             to_string(xrange_min),
             to_string(xrange_max),
             "COUNT",
             count
           ]) do
      decoded_jobs = decode_jobs(entries_and_jobs)
      {:ok, decoded_jobs}
    else
      {:ok, _} ->
        {:ok, []}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp pending_range(queue_name, start, stop, redis) do
    case Redix.command(redis, ["XPENDING", queue_name, "verk"]) do
      {:ok, [length, min_pending, max_pending, _]} when length > 0 ->
        min_pending = EntryID.parse(min_pending)
        max_pending = EntryID.parse(max_pending)
        start = EntryID.parse(start)
        stop = EntryID.parse(stop)

        xrange_min =
          min_pending
          |> max(start)
          |> min(max_pending)

        xrange_max =
          max_pending
          |> min(stop)
          |> max(min_pending)

        {:ok, {xrange_min, xrange_max}}

      {:ok, _} ->
        {:ok, []}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec reenqueue_pending_job(binary, binary, non_neg_integer, GenServer.server()) ::
          :ok | {:error, term}
  def reenqueue_pending_job(queue, job_id, idle_time, redis \\ Verk.Redis) do
    case Redix.command(redis, [
           "EVALSHA",
           @reenqueue_pending_job_script_sha,
           1,
           queue_name(queue),
           "verk",
           job_id,
           idle_time
         ]) do
      {:ok, _} ->
        :ok

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Counts how many jobs are enqueued
  """
  @spec count(binary) :: {:ok, integer} | {:error, atom | Redix.Error.t()}
  def count(queue) do
    commands = [
      ["MULTI"],
      ["XLEN", queue_name(queue)],
      ["XPENDING", queue_name(queue), "verk"],
      ["EXEC"]
    ]

    case Redix.pipeline(Verk.Redis, commands) do
      {:ok, [_, _, _, [total, [pending | _]]]} -> {:ok, total - pending}
      error -> error
    end
  end

  @doc """
  Counts how many jobs are enqueued on a queue, raising if there's an error
  """
  @spec count!(binary) :: integer
  def count!(queue) do
    bangify(count(queue))
  end

  @doc """
  Counts how many jobs are pending to be ack'd
  """
  @spec count_pending(binary) :: {:ok, integer} | {:error, atom | Redix.Error.t()}
  def count_pending(queue) do
    case Redix.command(Verk.Redis, ["XPENDING", queue_name(queue), "verk"]) do
      {:ok, [pending | _]} -> {:ok, pending}
      error -> error
    end
  end

  @doc """
  Counts how many jobs are pending to be ack'd, raising if there's an error
  """
  @spec count_pending!(binary) :: integer
  def count_pending!(queue) do
    bangify(count_pending(queue))
  end

  @doc """
  Clears the `queue`

  It will return `{:ok, true}` if the `queue` was cleared and `{:ok, false}` otherwise

  An error tuple may be returned if Redis failed
  """
  @spec clear(binary) :: {:ok, boolean} | {:error, Redix.Error.t()}
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
  Lists enqueued jobs from `from`
  """
  @spec range_from(binary, binary, non_neg_integer, GenServer.server()) ::
          {:ok, [Verk.Job.T]} | {:error, Redix.Error.t()}
  def range_from(queue, from \\ "0-0", count \\ 25, redis \\ Verk.Redis) do
    from = min_entry_id_unprocessed(queue, from)

    case Redix.command(redis, ["XRANGE", queue_name(queue), from, "+", "COUNT", count]) do
      {:ok, entries_and_jobs} ->
        decoded_jobs = decode_jobs(entries_and_jobs)
        {:ok, decoded_jobs}

      {:error, error} ->
        {:error, error}
    end
  end

  defp decode_jobs(entries_and_jobs) do
    for [entry_id, ["job", job]] <- entries_and_jobs, do: Job.decode!(job, entry_id)
  end

  @doc """
  Lists enqueued jobs from `from`
  """
  @spec range_to(binary, binary, non_neg_integer, GenServer.server()) ::
          {:ok, [Verk.Job.T]} | {:error, Redix.Error.t()}
  def range_to(queue, to \\ "+", count \\ 25, redis \\ Verk.Redis) do
    from = min_entry_id_unprocessed(queue, "0-0")

    case Redix.command(redis, ["XREVRANGE", queue_name(queue), to, from, "COUNT", count]) do
      {:ok, entries_and_jobs} ->
        decoded_jobs = decode_jobs(entries_and_jobs)
        {:ok, decoded_jobs}

      {:error, error} ->
        {:error, error}
    end
  end

  defp min_entry_id_unprocessed(queue, start) do
    entry_id =
      case Redix.command(Verk.Redis, ["XPENDING", queue_name(queue), "verk"]) do
        {:ok, [_, _, nil, _]} ->
          start = EntryID.parse(start)
          max(EntryID.min(), start)

        {:ok, [_, _, max_pending, _]} ->
          max_pending = EntryID.parse(max_pending)
          start = EntryID.parse(start)
          result = max(max_pending, start)
          EntryID.next(result)
      end

    to_string(entry_id)
  end

  @doc """
  Lists enqueued jobs from `from` raising if there's an error
  """
  @spec range_from!(binary, binary, non_neg_integer, GenServer.server()) :: [Verk.Job.T]
  def range_from!(queue, from \\ "0-0", count \\ 25, redis \\ Verk.Redis) do
    bangify(range_from(queue, from, count, redis))
  end

  @doc """
  Deletes the job from the `queue`

  It returns `{:ok, true}` if the job was found and deleted
  Otherwise it returns `{:ok, false}`

  An error tuple may be returned if Redis failed
  """
  @spec delete_job(binary, Job.t() | binary, GenServer.server()) ::
          {:ok, boolean} | {:error, Redix.Error.t()}
  def delete_job(queue, entry_id, redis \\ Verk.Redis)

  def delete_job(queue, entry_id, redis) when is_binary(entry_id) do
    case Redix.pipeline(redis, [
           ["XACK", queue_name(queue), "verk", entry_id],
           ["XDEL", queue_name(queue), entry_id]
         ]) do
      {:ok, [_, 1]} -> {:ok, true}
      {:ok, _} -> {:ok, false}
      {:error, error} -> {:error, error}
    end
  end

  def delete_job(queue, job, redis), do: delete_job(queue, job.item_id, redis)

  @doc """
  Delete job from the `queue`, raising if there's an error

  It returns `true` if the job was found and delete
  Otherwise it returns `false`

  An error will be raised if Redis failed
  """
  @spec delete_job!(binary, Job.t() | binary, GenServer.Sever) :: boolean
  def delete_job!(queue, job, redis \\ Verk.Redis) do
    bangify(delete_job(queue, job, redis))
  end
end
