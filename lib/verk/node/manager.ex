defmodule Verk.Node.Manager do
  @moduledoc """
  NodeManager keeps track of the nodes that are working on the queues
  """

  use GenServer
  require Logger

  @doc false
  def start_link(_), do: GenServer.start_link(__MODULE__, [], name: __MODULE__)

  @doc false
  def init(_) do
    local_verk_node_id = Application.fetch_env!(:verk, :local_node_id)
    frequency = Confex.get_env(:verk, :heartbeat, 30_000)

    Logger.info(
      "Node Manager started for node #{local_verk_node_id}. Heartbeat will run every #{frequency} milliseconds"
    )

    heartbeat(local_verk_node_id, frequency)

    Process.send_after(self(), :heartbeat, frequency)
    {:ok, {local_verk_node_id, frequency}}
  end

  @doc false
  def handle_info(:heartbeat, state = {local_verk_node_id, frequency}) do
    heartbeat(local_verk_node_id, frequency)

    Process.send_after(self(), :heartbeat, frequency)
    {:noreply, state}
  end

  defp heartbeat(local_verk_node_id, frequency) do
    case Verk.Node.expire_in(local_verk_node_id, 2 * frequency, Verk.Redis) do
      {:ok, _} ->
        :ok

      {:error, reason} ->
        Logger.error(
          "Failed to heartbeat node '#{local_verk_node_id}'. Reason: #{inspect(reason)}"
        )
    end
  end
end
