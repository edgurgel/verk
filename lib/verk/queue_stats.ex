defmodule Verk.QueueStats do
  @moduledoc """
  This process will update an :ets table with the following information per queue

    * Current amount of running jobs
    * Amount of finished jobs
    * Amount of failed jobs

  It will persist to redis from time to time

  It also holds information about the current status of queus. They can be:
  * running
  * idle
  * pausing
  * paused
  """
  use GenStage
  require Logger
  alias Verk.QueueStatsCounters

  defmodule State do
    @moduledoc false
    defstruct queues: %{}
  end

  @persist_interval 10_000

  @doc false
  def start_link(_) do
    GenStage.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @doc """
  Lists the queues and their stats searching for a `prefix` if provided
  """
  @spec all(binary) :: Map.t()
  def all(prefix \\ "") do
    GenServer.call(__MODULE__, {:all, prefix})
  end

  defp status(queue, queues, running_counter) do
    status = queues[queue] || Verk.Manager.status(queue)

    if status == :running and running_counter == 0 do
      :idle
    else
      status
    end
  end

  @doc false
  def init(_) do
    QueueStatsCounters.init()
    Process.send_after(self(), :persist_stats, @persist_interval)
    {:consumer, %State{}, subscribe_to: [Verk.EventProducer]}
  end

  def handle_call({:all, prefix}, _from, state) do
    result =
      for {queue, running, finished, failed} <- QueueStatsCounters.all(prefix), is_list(queue) do
        queue = to_string(queue)

        %{
          queue: queue,
          status: status(queue, state.queues, running),
          running_counter: running,
          finished_counter: finished,
          failed_counter: failed
        }
      end

    queues =
      for %{queue: queue, status: status} <- result, into: state.queues, do: {queue, status}

    {:reply, result, [], %State{queues: queues}}
  end

  def handle_events(events, _from, state) do
    new_state =
      Enum.reduce(events, state, fn event, state ->
        handle_event(event, state)
      end)

    {:noreply, [], new_state}
  end

  @doc false
  defp handle_event(%Verk.Events.JobStarted{job: job}, state) do
    QueueStatsCounters.register(:started, job.queue)
    state
  end

  defp handle_event(%Verk.Events.JobFinished{job: job}, state) do
    QueueStatsCounters.register(:finished, job.queue)
    state
  end

  defp handle_event(%Verk.Events.JobFailed{job: job}, state) do
    QueueStatsCounters.register(:failed, job.queue)
    state
  end

  defp handle_event(%Verk.Events.QueueRunning{queue: queue}, state) do
    QueueStatsCounters.reset_started(queue)
    %{state | queues: Map.put(state.queues, to_string(queue), :running)}
  end

  defp handle_event(%Verk.Events.QueuePausing{queue: queue}, state) do
    %{state | queues: Map.put(state.queues, to_string(queue), :pausing)}
  end

  defp handle_event(%Verk.Events.QueuePaused{queue: queue}, state) do
    %{state | queues: Map.put(state.queues, to_string(queue), :paused)}
  end

  @doc false
  def handle_info(:persist_stats, state) do
    case QueueStatsCounters.persist() do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.error("QueueStats failed to persist stats to Redis. Reason: #{inspect(reason)}")
    end

    Process.send_after(self(), :persist_stats, @persist_interval)
    {:noreply, [], state}
  end

  @doc false
  def handle_info(_, state) do
    {:noreply, [], state}
  end
end
