defmodule Verk.SupervisorTest do
  use ExUnit.Case

  describe "init/1" do
    test "defines tree" do
      {:ok, {_, children}} = Verk.Supervisor.init([])
      [redix, producer, stats, schedule_manager, default] = children

      assert {Verk.Redis, _, _, _, :worker, [Redix]} = redix
      assert {Verk.EventProducer, _, _, _, :worker, [Verk.EventProducer]} = producer
      assert {Verk.QueueStats, _, _, _, :worker, [Verk.QueueStats]} = stats
      assert {Verk.ScheduleManager, _, _, _, :worker, [Verk.ScheduleManager]} = schedule_manager
      assert {:"default.supervisor", _, _, _, :supervisor, [Verk.Queue.Supervisor]} = default
    end
  end
end
