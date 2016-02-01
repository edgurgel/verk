defmodule Verk do
  @moduledoc """
  Verk is a job processing system that integrates well with Sidekiq jobs

  Each queue will have a pool of workers handled by `poolboy` that will process jobs.

  Verk has a retry mechanism similar to Sidekiq that keeps retrying the jobs with a reasonable backoff.

  It has an API that provides information about the queues
  """
  use Application
  alias Verk.Job
  alias Timex.Time
  alias Timex.DateTime
  alias Timex.Date

  @schedule_key "schedule"

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
  Remove `queue` from the list of queues that are being processed
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
  @spec enqueue(%Job{}) :: { :ok, binary } | { :error, term }
  def enqueue(job = %Job{ queue: nil }), do: { :error, { :missing_queue, job } }
  def enqueue(job = %Job{ class: nil }), do: { :error, { :missing_module, job } }
  def enqueue(job = %Job{ args: args }) when not is_list(args), do: { :error, { :missing_args, job } }
  def enqueue(job = %Job{ jid: nil }) do
    <<part1::32, part2::32>> = :crypto.rand_bytes(8)
    jid = "#{part1}.#{part2}"
    enqueue(%Job{ job | jid: jid })
  end
  def enqueue(%Job{ jid: jid, queue: queue } = job) do
    case Redix.command(Verk.Redis, ["LPUSH", "queue:#{queue}", Poison.encode!(job)]) do
      { :ok, _ } -> { :ok, jid }
      { :error, reason } -> { :error, reason }
    end
  end

  @doc """
  Schedules a Job to the specified queue returning the respective job id

  The job must have:
   * a valid `queue`
   * a list of `args` to perform
   * a module to perform (`class`)
   * a valid `jid`
  """
  @spec schedule(%Job{}, %DateTime{}) :: { :ok, binary } | { :error, term }
  def schedule(job = %Job{ queue: nil }, %DateTime{}), do: { :error, { :missing_queue, job } }
  def schedule(job = %Job{ class: nil }, %DateTime{}), do: { :error, { :missing_module, job } }
  def schedule(job = %Job{ args: args }, %DateTime{}) when not is_list(args), do: { :error, { :missing_args, job } }
  def schedule(job = %Job{ jid: nil }, perform_at = %DateTime{}) do
    <<part1::32, part2::32>> = :crypto.rand_bytes(8)
    jid = "#{part1}.#{part2}"
    schedule(%Job{ job | jid: jid }, perform_at)
  end
  def schedule(%Job{ jid: jid } = job, %DateTime{} = perform_at) do
    perform_at_secs = Date.to_secs(perform_at)

    if perform_at_secs < Time.now(:secs) do
      enqueue(job)
    else
      case Redix.command(Verk.Redis, ["ZADD", @schedule_key, perform_at_secs, Poison.encode!(job)]) do
        { :ok, _ } -> { :ok, jid }
        { :error, reason } -> { :error, reason }
      end
    end
  end
end
