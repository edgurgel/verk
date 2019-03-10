defmodule Verk.QueueConsumer do
  @moduledoc """
  QueueConsumer interacts with redis to dequeue jobs from the specified queue.
  """

  use GenServer
  require Logger
  alias Verk.Queue

  @max_jobs 100

  defmodule State do
    @moduledoc false
    defstruct [:queue, :workers_manager, :redis, :node_id, :demand, :last_id, :timeout]
  end

  def reset(queue_consumer, n, timeout) do
    GenServer.call(queue_consumer, {:reset, n}, timeout)
  end

  def ask(queue_consumer, n) do
    GenServer.cast(queue_consumer, {:ask, n})
  end

  @doc false
  def start_link(queue_name, workers_manager, timeout) do
    GenServer.start_link(__MODULE__, [queue_name, workers_manager, timeout])
  end

  def init([queue, workers_manager, timeout]) do
    node_id = Confex.fetch_env!(:verk, :local_node_id)
    {:ok, redis} = Redix.start_link(Confex.get_env(:verk, :redis_url))

    state = %State{
      queue: queue,
      workers_manager: workers_manager,
      redis: redis,
      node_id: node_id,
      timeout: timeout,
      demand: 0,
      last_id: 0
    }

    ensure_group_exists!(queue, redis)

    Logger.info("Queue Consumer started for queue #{queue}")
    {:ok, state}
  end

  defp ensure_group_exists!(queue, redis) do
    Redix.command(redis, ["XGROUP", "CREATE", Queue.queue_name(queue), "verk", 0, "MKSTREAM"])
  rescue
    _ -> :ok
  end

  def handle_call({:reset, new_demand}, _from, state) do
    if new_demand != 0, do: send(self(), :consume)
    {:reply, :ok, %{state | demand: new_demand}}
  end

  def handle_cast({:ask, 0}, state), do: {:noreply, state}

  def handle_cast({:ask, new_demand}, state) do
    if state.demand == 0, do: send(self(), :consume)
    {:noreply, %{state | demand: state.demand + new_demand}}
  end

  def handle_info(:consume, state = %State{demand: 0}), do: {:noreply, state}

  def handle_info(:consume, state = %State{last_id: ">"}) do
    case consume(state) do
      {:ok, nil} ->
        send(self(), :consume)
        {:noreply, state}

      {:ok, jobs} ->
        send(state.workers_manager, {:jobs, jobs})

        new_demand = state.demand - length(jobs)
        if new_demand > 0, do: send(self(), :consume)
        {:noreply, %{state | demand: new_demand}}

      result ->
        Logger.error("Error while consuming jobs. Result: #{inspect(result)}")
        send(self(), :consume)
        {:noreply, state}
    end
  end

  def handle_info(:consume, state) do
    case consume(state) do
      {:ok, nil} ->
        send(self(), :consume)
        {:noreply, state}

      {:ok, []} ->
        send(self(), :consume)
        {:noreply, %{state | last_id: ">"}}

      {:ok, jobs} ->
        send(state.workers_manager, {:jobs, jobs})

        [last_id, _] = List.last(jobs)

        new_demand = state.demand - Enum.count(jobs)
        if new_demand > 0, do: send(self(), :consume)
        {:noreply, %{state | last_id: last_id, demand: new_demand}}

      result ->
        Logger.error("Error while consuming jobs. Result: #{inspect(result)}")
        send(self(), :consume)
        {:noreply, state}
    end
  end

  defp consume(state) do
    Queue.consume(
      state.queue,
      state.node_id,
      state.last_id,
      min(state.demand, @max_jobs),
      state.timeout,
      state.redis
    )
  end
end
