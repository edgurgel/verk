defmodule Verk.Manager.Supervisor do
  @moduledoc false
  use Supervisor

  @doc false
  def start_link(_), do: Supervisor.start_link(__MODULE__, [], name: __MODULE__)

  @doc false
  def init(_) do
    queues = Confex.get_env(:verk, :queues, [])
    children = for {queue, size} <- queues, do: Verk.Queue.Supervisor.child_spec(queue, size)

    children = [worker(Verk.Manager, [queues], id: Verk.Manager) | children]

    supervise(children, strategy: :rest_for_one)
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
