defmodule Verk.SupervisorTest do
  use ExUnit.Case
  import :meck
  import Verk.Supervisor

  setup do
    new(Supervisor)
    Application.delete_env(:verk, :local_node_id)
    Application.delete_env(:verk, :generate_node_id)
    Application.put_env(:verk, :node_id, "123")

    on_exit(fn ->
      unload()
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

  describe "start_child/2" do
    test "add a new queue" do
      queue = :test_queue

      child =
        {:"test_queue.supervisor", {Verk.Queue.Supervisor, :start_link, [:test_queue, 30]},
         :permanent, :infinity, :supervisor, [Verk.Queue.Supervisor]}

      expect(Supervisor, :start_child, [Verk.Supervisor, child], :ok)

      assert start_child(queue, 30) == :ok

      assert validate(Supervisor)
    end
  end

  describe "stop_child/1" do
    test "a queue successfully" do
      queue = :test_queue

      expect(Supervisor, :terminate_child, [Verk.Supervisor, :"test_queue.supervisor"], :ok)
      expect(Supervisor, :delete_child, [Verk.Supervisor, :"test_queue.supervisor"], :ok)

      assert stop_child(queue) == :ok

      assert validate(Supervisor)
    end

    test "a queue unsuccessfully terminating child" do
      queue = :test_queue

      expect(
        Supervisor,
        :terminate_child,
        [Verk.Supervisor, :"test_queue.supervisor"],
        {:error, :not_found}
      )

      assert stop_child(queue) == {:error, :not_found}

      assert validate(Supervisor)
    end

    test "a queue unsuccessfully deleting child" do
      queue = :test_queue

      expect(Supervisor, :terminate_child, [Verk.Supervisor, :"test_queue.supervisor"], :ok)

      expect(
        Supervisor,
        :delete_child,
        [Verk.Supervisor, :"test_queue.supervisor"],
        {:error, :not_found}
      )

      assert stop_child(queue) == {:error, :not_found}

      assert validate(Supervisor)
    end
  end
end
