defmodule Verk.Manager.Supervisor do
  @moduledoc false
  use Supervisor

  @doc false
  def start_link, do: Supervisor.start_link(__MODULE__, [], name: __MODULE__)

  @doc false
  def init(_) do
    queues = Confex.get_env(:verk, :queues, [])
    children = for {queue, size} <- queues, do: Verk.Queue.Supervisor.child_spec(queue, size)

    children = [worker(Verk.Manager, [queues], id: Verk.Manager) | children]

    supervise(children, strategy: :rest_for_one)
  end
end
