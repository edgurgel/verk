defmodule Verk.Supervisor do
  use Supervisor

  @doc false
  def start_link do
    Supervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  @doc false
  def init(_) do
    queues = Application.get_env(:verk, :queues, [])
    children = for { queue, size } <- queues, do: queue_child(queue, size)
    children = [worker(Verk.ScheduleManager, [], id: :schedule_manager) | children]
    supervise(children, strategy: :one_for_one)
  end

  @doc false
  def start_child(queue, size \\ 25) when is_atom(queue) and size > 0 do
    Supervisor.start_child(__MODULE__, queue_child(queue, size))
  end

  @doc false
  def stop_child(queue) when is_atom(queue) do
    supervisor_name = supervisor_name(queue)
    case Supervisor.terminate_child(__MODULE__, supervisor_name) do
      :ok -> Supervisor.delete_child(__MODULE__, supervisor_name)
      error = { :error, :not_found } -> error
    end
  end

  defp queue_child(queue, size) when is_atom(queue) do
    supervisor(Verk.Queue.Supervisor, [queue, size], id: supervisor_name(queue))
  end

  defp supervisor_name(queue) do
    String.to_atom("#{queue}.supervisor")
  end
end
