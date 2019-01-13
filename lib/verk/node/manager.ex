defmodule Verk.Node.Manager do
  @moduledoc """
  NodeManager keeps track of the nodes that are working on the queues
  """

  use GenServer
  require Logger
  alias Verk.InProgressQueue

  @doc false
  def start_link, do: GenServer.start_link(__MODULE__, [], name: __MODULE__)

  @doc false
  def init(_) do
    local_verk_node_id = Application.fetch_env!(:verk, :local_node_id)
    frequency = Confex.get_env(:verk, :heartbeat, 30_000)

    Logger.info(
      "Node Manager started for node #{local_verk_node_id}. Heartbeat will run every #{frequency} milliseconds"
    )

    :ok = Verk.Node.register(local_verk_node_id, 2 * frequency, Verk.Redis)
    Process.send_after(self(), :heartbeat, frequency)
    {:ok, {local_verk_node_id, frequency}}
  end

  @doc false
  def handle_info(:heartbeat, state = {local_verk_node_id, frequency}) do
    faulty_nodes = find_faulty_nodes(local_verk_node_id)

    for verk_node_id <- faulty_nodes do
      Logger.warn("Verk Node #{verk_node_id} seems to be down. Restoring jobs!")

      cleanup_queues(verk_node_id)

      Verk.Node.deregister!(verk_node_id, Verk.Redis)
    end

    heartbeat!(local_verk_node_id, frequency)
    {:noreply, state}
  end

  defp cleanup_queues(verk_node_id, cursor \\ 0) do
    case Verk.Node.queues!(verk_node_id, cursor, Verk.Redis) do
      {:ok, queues} ->
        do_cleanup_queues(queues, verk_node_id)

      {:more, queues, cursor} ->
        do_cleanup_queues(queues, verk_node_id)
        cleanup_queues(verk_node_id, cursor)
    end
  end

  defp do_cleanup_queues(queues, verk_node_id) do
    Enum.each(queues, &enqueue_inprogress(verk_node_id, &1))
  end

  defp find_faulty_nodes(local_verk_node_id, cursor \\ 0) do
    case Verk.Node.members(cursor, Verk.Redis) do
      {:ok, verk_nodes} ->
        do_find_faulty_nodes(verk_nodes, local_verk_node_id)

      {:more, verk_nodes, cursor} ->
        do_find_faulty_nodes(verk_nodes, local_verk_node_id) ++
          find_faulty_nodes(local_verk_node_id, cursor)
    end
  end

  defp do_find_faulty_nodes(verk_nodes, local_verk_node_id) do
    Enum.filter(verk_nodes, fn verk_node_id ->
      verk_node_id != local_verk_node_id and Verk.Node.ttl!(verk_node_id, Verk.Redis) < 0
    end)
  end

  defp heartbeat!(local_verk_node_id, frequency) do
    Verk.Node.expire_in!(local_verk_node_id, 2 * frequency, Verk.Redis)
    Process.send_after(self(), :heartbeat, frequency)
  end

  defp enqueue_inprogress(node_id, queue) do
    case InProgressQueue.enqueue_in_progress(queue, node_id, Verk.Redis) do
      {:ok, [0, m]} ->
        Logger.info("Added #{m} jobs.")
        Logger.info("No more jobs to be added to the queue #{queue} from inprogress list.")
        :ok

      {:ok, [n, m]} ->
        Logger.info("Added #{m} jobs.")
        Logger.info("#{n} jobs still to be added to the queue #{queue} from inprogress list.")
        enqueue_inprogress(node_id, queue)

      {:error, reason} ->
        Logger.error(
          "Failed to add jobs back to queue #{queue} from inprogress. Error: #{inspect(reason)}"
        )

        throw(:error)
    end
  end
end
