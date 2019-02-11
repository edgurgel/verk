defmodule Verk.QueuesDrainerTest do
  use ExUnit.Case, async: false
  alias Verk.EventProducer
  import Verk.QueuesDrainer
  import Mimic

  setup :verify_on_exit!

  setup do
    {:ok, pid} = EventProducer.start_link()

    on_exit(fn -> assert_down(pid) end)

    :ok
  end

  defp assert_down(pid) do
    ref = Process.monitor(pid)
    assert_receive {:DOWN, ^ref, _, _, _}
  end

  describe "terminate/2" do
    test "terminate shutdown" do
      queues = [{:running_queue, 25, :running}, {:paused_queue, 25, :paused}]
      expect(Verk, :pause_queue, fn :running_queue -> true end)
      expect(Verk.Manager, :status, fn -> queues end)

      EventProducer.async_notify(%Verk.Events.QueuePaused{queue: :running_queue})

      assert terminate(:shutdown, 2000) == :ok
    end

    test "terminate normal" do
      queues = [{:running_queue, 25, :running}, {:paused_queue, 25, :paused}]
      expect(Verk, :pause_queue, fn :running_queue -> true end)
      expect(Verk.Manager, :status, fn -> queues end)

      EventProducer.async_notify(%Verk.Events.QueuePaused{queue: :running_queue})

      assert terminate(:normal, 2000) == :ok
    end

    test "terminate shutdown timeout" do
      queues = [{:running_queue, 25, :running}, {:paused_queue, 25, :paused}]
      expect(Verk, :pause_queue, fn :running_queue -> true end)
      expect(Verk.Manager, :status, fn -> queues end)

      assert catch_throw(terminate(:shutdown, 2000)) == :shutdown_timeout
    end
  end
end
