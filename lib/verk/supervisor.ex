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
  def start_link do
    Supervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  defp generate_node_id do
    <<part1::32, part2::32>> = :crypto.strong_rand_bytes(8)
    "#{part1}#{part2}"
  end

  @doc false
  def init(_) do
    redis_url = Confex.get_env(:verk, :redis_url)
    shutdown_timeout = Confex.get_env(:verk, :shutdown_timeout, 30_000)
    verk_node_id = Confex.get_env(:verk, :node_id, generate_node_id())

    Application.put_env(:verk, :local_node_id, verk_node_id)

    redis = worker(Redix, [redis_url, [name: Verk.Redis]], id: Verk.Redis)
    node_manager = worker(Verk.Node.Manager, [], id: Verk.Node.Manager)
    event_producer = worker(Verk.EventProducer, [], id: Verk.EventProducer)
    queue_stats = worker(Verk.QueueStats, [], id: Verk.QueueStats)
    schedule_manager = worker(Verk.ScheduleManager, [], id: Verk.ScheduleManager)
    manager_sup = supervisor(Verk.Manager.Supervisor, [], id: Verk.Manager.Supervisor)

    drainer =
      worker(
        Verk.QueuesDrainer,
        [shutdown_timeout],
        id: Verk.QueuesDrainer,
        shutdown: shutdown_timeout
      )

    children = [
      redis,
      node_manager,
      event_producer,
      queue_stats,
      schedule_manager,
      manager_sup,
      drainer
    ]

    supervise(children, strategy: :one_for_one)
  end

  @doc false
  def start_child(queue, size \\ 25) when is_atom(queue) and size > 0 do
    Supervisor.start_child(__MODULE__, Verk.Queue.Supervisor.child_spec(queue, size))
  end

  @doc false
  def stop_child(queue) when is_atom(queue) do
    name = Verk.Queue.Supervisor.name(queue)

    case Supervisor.terminate_child(__MODULE__, name) do
      :ok -> Supervisor.delete_child(__MODULE__, name)
      error = {:error, :not_found} -> error
    end
  end
end
