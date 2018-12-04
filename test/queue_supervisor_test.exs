defmodule Verk.QueueSupervisorTest do
  use ExUnit.Case
  import Verk.Queue.Supervisor

  describe "init/1" do
    test "defines tree" do
      {:ok, {_, children}} = init([:default, 25])
      [queue_manager, pool, workers_manager] = children

      assert {:"default.queue_manager", _, _, _, :worker, [Verk.QueueManager]} = queue_manager
      assert {:"default.pool", _, _, _, :worker, [:poolboy]} = pool

      assert {:"default.workers_manager", _, _, _, :worker, [Verk.WorkersManager]} =
               workers_manager
    end
  end

  describe "name/1" do
    test "returns supervisor name" do
      assert name("default") == :"default.supervisor"
    end
  end

  describe "child_spec/2" do
    test "returns supervisor spec" do
      assert {:"default.supervisor", {Verk.Queue.Supervisor, :start_link, [:default, 25]}, _, _,
              :supervisor, [Verk.Queue.Supervisor]} = child_spec(:default, 25)
    end
  end
end
