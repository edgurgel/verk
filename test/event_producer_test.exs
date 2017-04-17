defmodule Verk.EventProducerTest do
  use ExUnit.Case
  import Verk.EventProducer

  describe "init/1" do
    test "inits queue" do
      assert init(:ok) == {:producer, {:queue.new(), 0}, dispatcher: GenStage.BroadcastDispatcher}
    end
  end

  describe "handle_cast/2" do
    test "notify with demand" do
      event     = :e2
      queue     = :queue.in(:e1, :queue.new())

      assert {:noreply, [:e1], {new_queue, 0}} = handle_cast({:notify, event}, {queue, 1})
      assert :queue.to_list(new_queue) == [event]
    end

    test "notify with no demand" do
      event = :e3
      queue = :queue.from_list [:e1, :e2]

      assert handle_cast({:notify, event}, {queue, 0}) == {:noreply, [], {:queue.in(event, queue), 0}}
    end
  end

  describe "handle_demand/2" do
    test "with events" do
      queue = :queue.from_list [:e1, :e2, :e3]

      assert handle_demand(100, {queue, 0}) == {:noreply, [:e1, :e2, :e3], {:queue.new(), 97}}
    end

    test "with no events" do
      queue = :queue.new

      assert handle_demand(100, {queue, 0}) == {:noreply, [], {queue, 100}}
    end
  end
end
