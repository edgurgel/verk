defmodule Verk.QueueManager do
  @moduledoc """
  QueueManager interacts with redis to dequeue jobs from the specified queue.
  """

  use GenServer
  require Logger

  @processing_key "processing"
  @retry_key "retry"

  @lpop_rpush_src_dest_script_sha Verk.Scripts.sha("lpop_rpush_src_dest")
  @mrpop_lpush_src_dest_script_sha Verk.Scripts.sha("mrpop_lpush_src_dest")

  @max_retry 25

  defmodule State do
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
  def start_link(name, queue_name) do
    GenServer.start_link(__MODULE__, [queue_name], name: name)
  end

  @doc """
  Pop a job from the assigned queue and reply with it if not empty
  """
  def dequeue(queue_manager, n, timeout \\ 5000) do
    try do
      GenServer.call(queue_manager, { :dequeue, n }, timeout)
    catch
      :exit, { :timeout, _ } -> :timeout
    end
  end

  @doc """
  Add job to be retried in the assigned queue
  """
  def retry(queue_manager, job, exception, timeout \\ 5000) do
    try do
      GenServer.call(queue_manager, { :retry, job, Timex.Time.now(:secs), exception }, timeout)
    catch
      :exit, { :timeout, _ } -> :timeout
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
    redis_url = Application.get_env(:verk, :redis_url, "redis://127.0.0.1:6379")
    { :ok, redis } = Redix.start_link(redis_url)
    Verk.Scripts.load(redis)

    state = %State{ queue_name: queue_name, redis: redis, node_id: node_id }

    Logger.info "Queue Manager started for queue #{queue_name}"
    { :ok, state }
  end

  @doc false
  def handle_call(:enqueue_inprogress, _from, state) do
    case Redix.command(state.redis, ["EVALSHA", @lpop_rpush_src_dest_script_sha, 2, inprogress(state.queue_name, state.node_id), "queue:#{state.queue_name}"]) do
      { :ok, n } -> Logger.info("#{n} jobs readded to the queue #{state.queue_name} from inprogress list")
        { :reply, :ok, state }
      { :error, reason } -> Logger.error("Failed to add jobs back to queue #{state.queue_name} from inprogress list. Error: #{inspect reason}")
        { :stop, :redis_failed, state }
    end
  end

  def handle_call({ :dequeue, n }, _from, state) do
    case Redix.command(state.redis, ["EVALSHA", @mrpop_lpush_src_dest_script_sha, 2,  "queue:#{state.queue_name}", inprogress(state.queue_name, state.node_id), n]) do
      { :ok, [] } ->
        { :reply, [], state }
      { :ok, jobs } ->
        jobs = for job <- jobs, do: Verk.Job.decode!(job)
        { :reply, jobs, state }
      { :error, %Redix.Error{message: message} } ->
        Logger.error("Failed to fetch jobs: #{message}")
        { :stop, :redis_failed, :redis_failed, state }
      { :error, _ } ->
        { :reply, :redis_failed, state }
    end
  end

  def handle_call({ :retry, job, failed_at, exception }, _from, state) do
    job = %{ job | failed_at: failed_at, error_message: Exception.message(exception) }
    retry_count = (job.retry_count || 0) + 1
    if retry_count <= @max_retry do
      job = %{ job | retry_count: retry_count }
      payload = Poison.encode!(job)
      retry_at = retry_at(failed_at, retry_count) |> to_string
      case Redix.command(state.redis, ["ZADD", @retry_key, retry_at, payload]) do
        { :ok, _ } -> :ok
        error -> Logger.error("Failed to add job_id #{job.jid} to retry. Error: #{inspect error}")
      end
    else
      Logger.error "Max retries reached to job_id #{job.jid}, job: #{inspect job}"
    end
    { :reply, :ok, state }
  end

  defp retry_at(failed_at, retry_count) do
    delay = :math.pow(retry_count, 4) + 15 + (:random.uniform(30) * (retry_count + 1))
    failed_at + delay
  end

  @doc false
  def handle_cast({:ack, job}, state) do
    case Redix.command(state.redis, ["LREM", inprogress(state.queue_name, state.node_id), "-1", job.original_json]) do
      { :ok, 1 } -> :ok
      _ -> Logger.error("Failed to acknowledge job #{inspect job}")
    end
    { :noreply, state }
  end

  defp inprogress(queue_name, node_id) do
    "inprogress:#{queue_name}:#{node_id}"
  end
end
