defmodule Verk.EventProducer do
  @moduledoc """
  A GenStage producer that broadcasts events to subscribed consumers.
  """
  use GenStage

  def start_link(_args \\ []) do
    GenStage.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  def stop, do: GenStage.stop(__MODULE__)

  def async_notify(event) do
    GenStage.cast(__MODULE__, {:notify, event})
  end

  def init(:ok) do
    {:producer, {:queue.new(), 0}, dispatcher: GenStage.BroadcastDispatcher}
  end

  def handle_cast({:notify, event}, {queue, demand}) do
    dispatch_events(:queue.in(event, queue), demand)
  end

  def handle_demand(incoming_demand, {queue, demand}) do
    dispatch_events(queue, incoming_demand + demand)
  end

  defp dispatch_events(queue, demand, events \\ []) do
    with d when d > 0 <- demand,
         {{:value, event}, queue} <- :queue.out(queue) do
      dispatch_events(queue, demand - 1, [event | events])
    else
      _ -> {:noreply, Enum.reverse(events), {queue, demand}}
    end
  end
end
