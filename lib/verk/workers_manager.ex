defmodule Verk.WorkersManager do
  @moduledoc """
  A WorkersManager assign jobs to workers from a pool (handled by poolboy) monitoring the job.

  It interacts with the related QueueManager to request jobs and to schedule jobs to be retried
  """

  use GenServer
  require Logger
  alias Verk.Events
  alias Verk.Job
  alias Verk.QueueManager
  alias Verk.Log

  @default_timeout 1000

  defmodule State do
    @moduledoc false
    defstruct [:queue_name, :pool_name, :queue_manager_name, :pool_size, :monitors, :timeout]
  end

  @doc """
  Returns the atom that represents the WorkersManager of the `queue`
  """
  @spec name(binary | atom) :: atom
  def name(queue) do
    String.to_atom("#{queue}.workers_manager")
  end

  @doc false
  def start_link(workers_manager_name, queue_name, queue_manager_name, pool_name, pool_size) do
    gen_server_args = [workers_manager_name, queue_name, queue_manager_name, pool_name, pool_size]
    GenServer.start_link(__MODULE__, gen_server_args, name: workers_manager_name)
  end

  @doc """
  List running jobs

  Example:
      [%{process: #PID<0.186.0>, job: %Verk.Job{...}, started_at: %Timex.DateTime{...}} ]
  """
  @spec running_jobs(binary | atom) :: Map.t
  def running_jobs(queue, limit \\ 100) do
    match_spec = [{{:"$1", :_, :"$2", :_, :"$3"}, [], [{{:"$1", :"$2", :"$3"}}]}]
    case :ets.select(name(queue), match_spec, limit) do
      :'$end_of_table' -> []
      {jobs, _continuation} ->
        for {pid, job, started_at} <- jobs do
          %{process: pid, job: job, started_at: started_at}
        end
    end
  end

  @process_info_keys [:current_stacktrace, :initial_call, :reductions, :status]
  @doc """
  List information about the process that is currently running a `job_id`
  """
  @spec inspect_worker(binary | atom, binary) :: {:ok, Map.t} | {:error, :not_found}
  def inspect_worker(queue, job_id) do
    case :ets.match(name(queue), {:'$1', job_id, :'$2', :_, :'$3'}) do
      [] ->
        {:error, :not_found}
      [[pid, job, started_at]] ->
        info = Process.info(pid, @process_info_keys)
        if info do
          {:ok, %{process: pid, job: job, started_at: started_at, info: info}}
        else
          {:error, :not_found}
        end
    end
  end

  @doc """
  Create a table to monitor workers saving data about the assigned queue/pool
  """
  def init([workers_manager_name, queue_name, queue_manager_name, pool_name, size]) do
    monitors = :ets.new(workers_manager_name, [:named_table, read_concurrency: true])
    timeout = Application.get_env(:verk, :workers_manager_timeout, @default_timeout)
    state = %State{queue_name: queue_name,
                  queue_manager_name: queue_manager_name,
                  pool_name: pool_name,
                  pool_size: size,
                  monitors: monitors,
                  timeout: timeout}

    Logger.info "Workers Manager started for queue #{queue_name}"

    send self, :enqueue_inprogress
    Verk.QueueStats.reset_started(queue_name)

    {:ok, state}
  end

  @doc false
  def handle_info(:enqueue_inprogress, state) do
    :ok = QueueManager.enqueue_inprogress(state.queue_manager_name)
    {:noreply, state, 0}
  end

  def handle_info(:timeout, state) do
    workers = free_workers(state.pool_name)
    if workers != 0 do
      case QueueManager.dequeue(state.queue_manager_name, workers) do
        jobs when is_list(jobs) ->
          for job <- jobs, do: job |> Job.decode! |> start_job(state)
        reason ->
          Logger.error("Failed to fetch a job. Reason: #{inspect reason}")
      end
      {:noreply, state, random_timeout(state.timeout)}
    else
      {:noreply, state}
    end
  end

  # Possible reasons to receive a :DOWN message:
  #   * :normal - The worker finished the job but :done message did not arrive
  #   * :failed - The worker failed the job but :failed message did not arrive
  #   * {reason, stack } - The worker crashed and it has a stacktrace
  #   * reason - The worker crashed and it doesn't have have a stacktrace
  def handle_info({:DOWN, mref, _, worker, :normal}, state) do
    case :ets.lookup(state.monitors, worker) do
      [{^worker, _job_id, job, ^mref, start_time}] ->
        succeed(job, start_time, worker, mref, state.monitors, state.queue_manager_name)
      _ -> Logger.warn "Worker finished but such worker was not monitored #{inspect(worker)}"
    end
    {:noreply, state, 0}
  end

  def handle_info({:DOWN, mref, _, worker, :failed}, state) do
    handle_down!(mref, worker, :failed, [], state)
    {:noreply, state, 0}
  end

  def handle_info({:DOWN, mref, _, worker, {reason, stack}}, state) do
    handle_down!(mref, worker, reason, stack, state)
    {:noreply, state, 0}
  end

  def handle_info({:DOWN, mref, _, worker, reason}, state) do
    handle_down!(mref, worker, reason, [], state)
    {:noreply, state, 0}
  end

  defp handle_down!(mref, worker, reason, stack, state) do
    Logger.debug "Worker got down, reason: #{inspect reason}, #{inspect([mref, worker])}"
    case :ets.lookup(state.monitors, worker) do
      [{^worker, _job_id, job, ^mref, start_time}] ->
        exception = RuntimeError.exception(inspect(reason))
        fail(job, start_time, worker, mref, state.monitors, state.queue_manager_name, exception, stack)
      error -> Logger.warn "Worker got down but it was not found, error: #{inspect error}"
    end
  end

  @doc false
  def handle_cast({:done, worker, job_id}, state) do
    case :ets.lookup(state.monitors, worker) do
      [{^worker, ^job_id, job, mref, start_time}] ->
        succeed(job, start_time, worker, mref, state.monitors, state.queue_manager_name)
      _ -> Logger.warn "#{job_id} finished but no worker was monitored"
    end
    {:noreply, state, 0}
  end

  def handle_cast({:failed, worker, job_id, exception, stacktrace}, state) do
    Logger.debug "Job failed reason: #{inspect exception}"
    case :ets.lookup(state.monitors, worker) do
      [{^worker, ^job_id, job, mref, start_time}] ->
        fail(job, start_time, worker, mref, state.monitors, state.queue_manager_name, exception, stacktrace)
      _ -> Logger.warn "#{job_id} failed but no worker was monitored"
    end
    {:noreply, state, 0}
  end

  defp succeed(job, start_time, worker, mref, monitors, queue_manager_name) do
    QueueManager.ack(queue_manager_name, job)
    Log.done(job, start_time, worker)
    demonitor!(monitors, worker, mref)
    notify!(%Events.JobFinished{job: job, finished_at: Timex.DateTime.now})
  end

  defp fail(job, start_time, worker, mref, monitors, queue_manager_name, exception, stacktrace) do
    Log.fail(job, start_time, worker)
    demonitor!(monitors, worker, mref)
    :ok = QueueManager.retry(queue_manager_name, job, exception, stacktrace)
    :ok = QueueManager.ack(queue_manager_name, job)
    notify!(%Events.JobFailed{job: job, failed_at: Timex.DateTime.now, exception: exception, stacktrace: stacktrace})
  end

  defp start_job(job, state) do
    case :poolboy.checkout(state.pool_name, false) do
      worker when is_pid(worker) ->
        monitor!(state.monitors, worker, job)
        Log.start(job, worker)
        Verk.Worker.perform_async(worker, self, job)
        notify!(%Events.JobStarted{job: job, started_at: Timex.DateTime.now})
      :full ->
        Verk.enqueue(job)
    end
  end

  defp demonitor!(monitors, worker, mref) do
    true = Process.demonitor(mref, [:flush])
    true = :ets.delete(monitors, worker)
  end

  defp monitor!(monitors, worker, job = %Job{jid: job_id}) do
    mref = Process.monitor(worker)
    now = Timex.DateTime.now
    true = :ets.insert(monitors, {worker, job_id, job, mref, now})
  end

  @doc false
  def terminate(reason, _state) do
    Logger.error "Manager terminating, reason: #{inspect reason}"
    :ok
  end

  defp notify!(event) do
    :ok = GenEvent.ack_notify(Verk.EventManager, event)
  end

  defp free_workers(pool_name) do
    {_, free, _, _} = :poolboy.status(pool_name)

    free
  end

  defp random_timeout(timeout) do
    round(timeout + (:rand.uniform * timeout))
  end
end
