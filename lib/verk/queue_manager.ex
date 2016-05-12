defmodule Verk.QueueManager do
  @moduledoc """
  QueueManager interacts with redis to dequeue jobs from the specified queue.
  """

  use GenServer
  require Logger
  alias Verk.RetrySet
  alias Verk.DeadSet

  @processing_key "processing"
  @default_stacktrace_size 5

  @external_resource "priv/lpop_rpush_src_dest.lua"
  @external_resource "priv/mrpop_lpush_src_dest.lua"
  @lpop_rpush_src_dest_script_sha Verk.Scripts.sha("lpop_rpush_src_dest")
  @mrpop_lpush_src_dest_script_sha Verk.Scripts.sha("mrpop_lpush_src_dest")

  @max_retry 25
  @max_dead 100
  @max_jobs 100

  defmodule State do
    @moduledoc false
    defstruct [:queue_name, :redis, :node_id]
  end

  @doc """
  Returns the atom that represents the QueueManager of the `queue`
  """
  @spec name(binary | atom) :: atom
  def name(queue) do
    String.to_atom("#{queue}.queue_manager")
  end

  @doc false
  def start_link(queue_manager_name, queue_name) do
    GenServer.start_link(__MODULE__, [queue_name], name: queue_manager_name)
  end

  @doc """
  Pop a job from the assigned queue and reply with it if not empty
  """
  def dequeue(queue_manager, n, timeout \\ 5000) do
    try do
      GenServer.call(queue_manager, {:dequeue, n}, timeout)
    catch
      :exit, {:timeout, _} -> :timeout
    end
  end

  @doc """
  Add job to be retried in the assigned queue
  """
  def retry(queue_manager, job, exception, stacktrace, timeout \\ 5000) do
    try do
      GenServer.call(queue_manager, {:retry, job, Timex.Time.now(:seconds), exception, stacktrace}, timeout)
    catch
      :exit, {:timeout, _} -> :timeout
    end
  end

  @doc """
  Acknowledge that a job was processed
  """
  def ack(queue_manager, job) do
    GenServer.cast(queue_manager, {:ack, job})
  end

  @doc """
  Enqueue inprogress jobs back to the queue
  """
  def enqueue_inprogress(queue_manager) do
    GenServer.call(queue_manager, :enqueue_inprogress)
  end

  @doc """
  Connect to redis
  """
  def init([queue_name]) do
    node_id = Application.get_env(:verk, :node_id, "1")
    {:ok, redis_url} = Application.fetch_env(:verk, :redis_url)
    {:ok, redis} = Redix.start_link(redis_url)
    Verk.Scripts.load(redis)

    state = %State{queue_name: queue_name, redis: redis, node_id: node_id}

    Logger.info "Queue Manager started for queue #{queue_name}"
    {:ok, state}
  end

  @doc false
  def handle_call(:enqueue_inprogress, _from, state) do
    in_progress_key = inprogress(state.queue_name, state.node_id)
    case Redix.command(state.redis, ["EVALSHA", @lpop_rpush_src_dest_script_sha, 2,
                                     in_progress_key, "queue:#{state.queue_name}"]) do
      {:ok, n} ->
        Logger.info("#{n} jobs readded to the queue #{state.queue_name} from inprogress list")
        {:reply, :ok, state}
      {:error, reason} ->
        Logger.error("Failed to add jobs back to queue #{state.queue_name} from inprogress. Error: #{inspect reason}")
        {:stop, :redis_failed, state}
    end
  end

  def handle_call({:dequeue, n}, _from, state) do
    case Redix.command(state.redis, ["EVALSHA", @mrpop_lpush_src_dest_script_sha, 2,  "queue:#{state.queue_name}",
                                     inprogress(state.queue_name, state.node_id), min(@max_jobs, n)]) do
      {:ok, []} ->
        {:reply, [], state}
      {:ok, jobs} ->
        {:reply, jobs, state}
      {:error, %Redix.Error{message: message}} ->
        Logger.error("Failed to fetch jobs: #{message}")
        {:stop, :redis_failed, :redis_failed, state}
      {:error, _} ->
        {:reply, :redis_failed, state}
    end
  end

  def handle_call({:retry, job, failed_at, exception, stacktrace}, _from, state) do
    retry_count = (job.retry_count || 0) + 1
    job         = build_retry_job(job, retry_count, failed_at, exception, stacktrace)

    if retry_count <= @max_retry do
      RetrySet.add!(job, failed_at, state.redis)
    else
      Logger.info("Max retries reached to job_id #{job.jid}, job: #{inspect job}")
      DeadSet.add!(job, failed_at, state.redis)
    end
    {:reply, :ok, state}
  end

  defp build_retry_job(job, retry_count, failed_at, exception, stacktrace) do
    job = %{job | error_backtrace: format_stacktrace(stacktrace),
                  error_message: Exception.message(exception),
                  retry_count: retry_count}
    if retry_count > 1 do
      # Set the retried_at if this job was already retried at least once
      %{job | retried_at: failed_at}
    else
      # Set the failed_at if this the first time the job failed
      %{job | failed_at: failed_at}
    end
  end

  @doc false
  def handle_cast({:ack, job}, state) do
    case Redix.command(state.redis, ["LREM", inprogress(state.queue_name, state.node_id), "-1", job.original_json]) do
      {:ok, 1} -> :ok
      _ -> Logger.error("Failed to acknowledge job #{inspect job}")
    end
    {:noreply, state}
  end

  defp inprogress(queue_name, node_id) do
    "inprogress:#{queue_name}:#{node_id}"
  end

  defp format_stacktrace(stacktrace) when is_list(stacktrace) do
    stacktrace_limit = Application.get_env(:verk, :failed_job_stacktrace_size, @default_stacktrace_size)
    Exception.format_stacktrace(Enum.slice(stacktrace, 0..(stacktrace_limit - 1)))
  end

  defp format_stacktrace(stacktrace), do: inspect(stacktrace)
end
