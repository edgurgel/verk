defmodule Verk.QueuesDrainer do
  @moduledoc """
  This process exists to pause queues once a `shutdown` is issued. Once all queues are paused the
  """

  defmodule Consumer do
    @moduledoc false
    use GenStage

    def start_link() do
      GenStage.start_link(__MODULE__, self())
    end

    def init(parent) do
      filter = fn event -> event.__struct__ == Verk.Events.QueuePaused end
      {:consumer, parent, subscribe_to: [{Verk.EventProducer, selector: filter}]}
    end

    def handle_events([event], _from, parent) do
      send(parent, {:paused, event.queue})
      {:noreply, [], parent}
    end
  end

  use GenServer
  require Logger

  def start_link(shutdown_timeout) do
    GenServer.start_link(__MODULE__, shutdown_timeout)
  end

  def init(shutdown_timeout) do
    Process.flag(:trap_exit, true)
    {:ok, shutdown_timeout}
  end

  def terminate(:shutdown, shutdown_timeout) do
    do_terminate(shutdown_timeout)
  end

  def terminate({:shutdown, _reason}, shutdown_timeout) do
    do_terminate(shutdown_timeout)
  end

  def terminate(:normal, shutdown_timeout) do
    do_terminate(shutdown_timeout)
  end

  def terminate(reason, shutdown_timeout) do
    Logger.info("Queues not being drainer as reason is not expected")
    :ok
  end

  defp do_terminate(shutdown_timeout) do
    Logger.warn("Pausing all queues. Max timeout: #{shutdown_timeout}")

    {:ok, _pid} = Consumer.start_link()

    queues =
      for {queue, _, :running} <- Verk.Manager.status(), into: MapSet.new() do
        Verk.pause_queue(queue)
        queue
      end

    n_queues = MapSet.size(queues)
    Logger.warn("Waiting for #{n_queues} queue(s)")

    for x <- 0..n_queues, x > 0 do
      receive do
        {:paused, queue} -> Logger.info("Queue #{queue} paused!")
      after
        shutdown_timeout ->
          Logger.error("Waited the maximum amount of time to pause queues.")
          throw(:shutdown_timeout)
      end
    end

    Logger.warn("All queues paused. Shutting down.")
    :ok
  end
end
