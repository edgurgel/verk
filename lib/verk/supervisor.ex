defmodule Verk.Supervisor do
  @moduledoc """
  Supervisor definition for Verk application. It consists of:
  * `Verk.ScheduleManager`
  * GenStage producer named `Verk.EventProducer`
  * GenStage consumer `Verk.QueueStats`
  * Redis connectionn named `Verk.Redis`
  * A `Verk.Queue.Supervisor` per queue
  """
  use Supervisor

  @doc """
  It starts the main supervisor
  """
  def start_link(_ \\ []) do
    Supervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  @doc false
  def init(_) do
    local_verk_node_id = verk_node_id()

    Application.put_env(:verk, :local_node_id, local_verk_node_id)

    Supervisor.init(children(), strategy: :one_for_one)
  end

  defp children do
    shutdown_timeout = Confex.get_env(:verk, :shutdown_timeout, 30_000)

    drainer =
      worker(
        Verk.QueuesDrainer,
        [shutdown_timeout],
        id: Verk.QueuesDrainer,
        shutdown: shutdown_timeout
      )

    [
      Verk.Redis,
      Verk.Node.Manager,
      Verk.EventProducer,
      Verk.QueueStats,
      Verk.ScheduleManager,
      Verk.Manager.Supervisor,
      drainer
    ]
  end

  defp verk_node_id do
    case Application.fetch_env(:verk, :local_node_id) do
      {:ok, local_verk_node_id} ->
        local_verk_node_id

      :error ->
        <<part1::32, part2::32>> = :crypto.strong_rand_bytes(8)
        "#{part1}#{part2}"
    end
  end
end
