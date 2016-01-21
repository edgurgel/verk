defmodule Verk.QueueStatsWatcherTest do
  use ExUnit.Case
  import Verk.QueueStatsWatcher

  test "init adds monitored handler" do
    { :ok, _pid } = GenEvent.start_link([name: Verk.EventManager])

    assert { :ok, _ref } = init([])
    assert { :monitors, process: { Verk.EventManager, _node } } = Process.info(self, :monitors)
    assert GenEvent.which_handlers(Verk.EventManager) == [Verk.QueueStats]
  end

  test "handle_info :gen_event_EXIT stops watcher" do
    assert handle_info({ :gen_event_EXIT, :handler, :normal}, :ref) == { :stop, :normal, :ref }
    assert handle_info({ :gen_event_EXIT, :handler, :shutdown}, :ref) == { :stop, :shutdown, :ref }
    assert handle_info({ :gen_event_EXIT, :handler, :crash}, :ref) == { :stop, :crash, :ref }
  end

  test "handle_info DOWN from manager stops watcher" do
    assert handle_info({ :DOWN, :ref, :process, :pid, :reason}, :ref) == { :stop, :reason, :ref }
  end
end
