defmodule Verk.QueuesDrainerTest do
  use ExUnit.Case
  alias Verk.EventProducer
  import Verk.QueuesDrainer
  import :meck

  setup_all do
    {:ok, pid} = EventProducer.start_link()
    on_exit fn ->
      if Process.alive?(pid), do: EventProducer.stop()
    end
    :ok
  end

  setup do
    new Verk, [:merge_expects]
    on_exit fn -> unload() end
    :ok
  end

  describe "terminate/2" do
    test "terminate shutdown" do
      queues = [{:running_queue, 25, :running}, {:paused_queue, 25, :paused}]
      expect(Verk, :pause_queue, [:running_queue], true)
      expect(Verk.Manager, :status, 0, queues)

      EventProducer.async_notify(%Verk.Events.QueuePaused{queue: :running_queue})

      assert terminate(:shutdown, 2000) == :ok
    end

    test "terminate normal" do
      queues = [{:running_queue, 25, :running}, {:paused_queue, 25, :paused}]
      expect(Verk, :pause_queue, [:running_queue], true)
      expect(Verk.Manager, :status, 0, queues)

      EventProducer.async_notify(%Verk.Events.QueuePaused{queue: :running_queue})

      assert terminate(:normal, 2000) == :ok
    end

    test "terminate shutdown timeout" do
      queues = [{:running_queue, 25, :running}, {:paused_queue, 25, :paused}]
      expect(Verk, :pause_queue, [:running_queue], true)
      expect(Verk.Manager, :status, 0, queues)

      assert catch_throw(terminate(:shutdown, 2000)) == :shutdown_timeout
    end
  end
end
