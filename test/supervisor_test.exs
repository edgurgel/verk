defmodule Verk.SupervisorTest do
  use ExUnit.Case
  import Verk.Supervisor

  setup do
    Application.delete_env(:verk, :local_node_id)
    Application.delete_env(:verk, :generate_node_id)
    Application.put_env(:verk, :node_id, "123")

    on_exit(fn ->
      Application.delete_env(:verk, :node_id)
      Application.delete_env(:verk, :local_node_id)
      Application.delete_env(:verk, :generate_node_id)
    end)

    :ok
  end

  describe "init/1" do
    test "defines tree" do
      {:ok, {_, children}} = init([])
      [redix, producer, stats, schedule_manager, manager_sup, drainer] = children

      assert {Verk.Redis, _, _, _, :worker, [Redix]} = redix
      assert {Verk.EventProducer, _, _, _, :worker, [Verk.EventProducer]} = producer
      assert {Verk.QueueStats, _, _, _, :worker, [Verk.QueueStats]} = stats
      assert {Verk.ScheduleManager, _, _, _, :worker, [Verk.ScheduleManager]} = schedule_manager

      assert {Verk.Manager.Supervisor, _, _, _, :supervisor, [Verk.Manager.Supervisor]} =
               manager_sup

      assert {Verk.QueuesDrainer, _, _, _, :worker, [Verk.QueuesDrainer]} = drainer
    end

    test "defines local_node_id randomly if generate_node_id is true" do
      Application.put_env(:verk, :generate_node_id, true)
      assert {:ok, _} = init([])
      assert Application.get_env(:verk, :local_node_id) != "123"
    end

    test "defines local_node_id as node_id if generate_node_id is false" do
      Application.put_env(:verk, :generate_node_id, false)
      assert {:ok, _} = init([])
      assert Application.get_env(:verk, :local_node_id) == "123"
    end

    test "does nothing if node_id if local_node_id is already defined" do
      Application.put_env(:verk, :generate_node_id, true)
      Application.put_env(:verk, :local_node_id, "456")
      assert {:ok, _} = init([])
      assert Application.get_env(:verk, :local_node_id) == "456"
    end

    test "defines tree with node manager if generate_node_id is true" do
      Application.put_env(:verk, :generate_node_id, true)

      {:ok, {_, children}} = init([])
      [redix, node_manager, producer, stats, schedule_manager, manager_sup, drainer] = children

      assert {Verk.Redis, _, _, _, :worker, [Redix]} = redix
      assert {Verk.Node.Manager, _, _, _, :worker, [Verk.Node.Manager]} = node_manager
      assert {Verk.EventProducer, _, _, _, :worker, [Verk.EventProducer]} = producer
      assert {Verk.QueueStats, _, _, _, :worker, [Verk.QueueStats]} = stats
      assert {Verk.ScheduleManager, _, _, _, :worker, [Verk.ScheduleManager]} = schedule_manager

      assert {Verk.Manager.Supervisor, _, _, _, :supervisor, [Verk.Manager.Supervisor]} =
               manager_sup

      assert {Verk.QueuesDrainer, _, _, _, :worker, [Verk.QueuesDrainer]} = drainer
    end
  end
end
