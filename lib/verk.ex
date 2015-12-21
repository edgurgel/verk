defmodule Verk do
  @moduledoc """
  Verk is a job processing system that integrates well with Sidekiq jobs

  Each queue will have a pool of workers handled by `poolboy` that will process jobs.

  Verk has a retry mechanism similar to Sidekiq that keeps retrying the jobs with a reasonable backoff.

  It has an API that provides information about the queues
  """
  use Application
  alias Verk.Job

  @doc false
  def start(_type, _args), do: Verk.Supervisor.start_link

  @doc """
  Add a new `queue` with a pool of size `size` of workers
  """
  @spec add_queue(atom, pos_integer) :: Supervisor.on_start_child
  def add_queue(queue, size \\ 25) when is_atom(queue) and size > 0 do
    Verk.Supervisor.start_child(queue, size)
  end

  @doc """
  Remve `queue` from the list of queues that are being processed
  """
  @spec remove_queue(atom) :: :ok | { :error, :not_found }
  def remove_queue(queue) when is_atom(queue) do
    Verk.Supervisor.stop_child(queue)
  end

  @doc """
  Enqueues a Job to the specified queue returning the respective job id

  The job must have:
   * a valid `queue`
   * a list of `args` to perform
   * a module to perform (`class`)
   * a valid `jid`
  """
  @spec enqueue(pid, %Job{}) :: { :ok, binary } | { :error, term }
  def enqueue(_, job = %Job{ queue: nil }), do: { :error, { :missing_queue, job } }
  def enqueue(_, job = %Job{ class: nil }), do: { :error, { :missing_module, job } }
  def enqueue(_, job = %Job{ args: args }) when not is_list(args), do: { :error, { :missing_args, job } }
  def enqueue(redis, job = %Job{ jid: nil }) do
    <<part1::32, part2::32>> = :crypto.rand_bytes(8)
    jid = "#{part1}.#{part2}"
    enqueue(redis, %Job{ job | jid: jid })
  end
  def enqueue(redis, %Job{ jid: jid, queue: queue } = job) do
    case Redix.command(redis, ["LPUSH", "queue:#{queue}", Poison.encode!(job)]) do
      { :ok, _ } -> { :ok, jid }
      { :error, reason } -> { :error, reason }
    end
  end

  @doc """
  Similar to enqueue/2
  """
  @spec enqueue(%Job{}) :: { :ok, binary } | { :error, term }
  def enqueue(job) do
    redis_url = Application.get_env(:verk, :redis_url, "redis://127.0.0.1:6379")
    { :ok, redis } = Redix.start_link(redis_url)
    result = enqueue(redis, job)
    :ok = Redix.stop(redis)
    result
  end
end
