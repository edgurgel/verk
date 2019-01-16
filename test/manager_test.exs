defmodule Verk.ManagerTest do
  use ExUnit.Case
  import :meck
  import Verk.Manager

  @node_id 123

  setup_all do
    Application.put_env(:verk, :local_node_id, @node_id)

    on_exit(fn ->
      Application.delete_env(:verk, :local_node_id)
    end)
  end

  setup do
    new(Verk.Node, [:merge_expects])
    on_exit(fn -> unload() end)
    :ok
  end

  defp init_table(queues) do
    :ets.new(:verk_manager, [:ordered_set, :named_table, :public, read_concurrency: true])
    for {queue, size, status} <- queues, do: :ets.insert_new(:verk_manager, {queue, size, status})
  end

  describe "init/1" do
    test "creates an ETS table with queues" do
      queues = [default: 25, low_priority: 10]
      expect(Verk.Node, :add_queue!, [@node_id, :default, Verk.Redis], :ok)
      expect(Verk.Node, :add_queue!, [@node_id, :low_priority, Verk.Redis], :ok)
      init(queues)

      assert :ets.tab2list(:verk_manager) == [
               {:default, 25, :running},
               {:low_priority, 10, :running}
             ]
    end
  end

  describe "status/0" do
    test "returns running queues" do
      queues = [{:default, 25, :running}, {:low_priority, 10, :running}]
      init_table(queues)
      assert status() == [{:default, 25, :running}, {:low_priority, 10, :running}]
    end
  end

  describe "status/1" do
    test "returns status of a queue" do
      queues = [{:default, 25, :running}, {:low_priority, 10, :paused}]
      init_table(queues)
      assert status(:default) == :running
      assert status(:low_priority) == :paused
    end
  end

  describe "pause/1" do
    test "pauses queue if queue exists" do
      queues = [{:default, 25, :running}, {:low_priority, 10, :running}]
      init_table(queues)

      queue = :default

      expect(Verk.WorkersManager, :pause, [queue], :ok)

      assert pause(queue) == true

      assert :ets.tab2list(:verk_manager) == [
               {:default, 25, :paused},
               {:low_priority, 10, :running}
             ]

      assert validate(Verk.WorkersManager)
    end

    test "does nothing if queue does not exist" do
      queues = [{:default, 25, :running}, {:low_priority, 10, :running}]
      init_table(queues)

      queue = :no_queue

      assert pause(queue) == false

      assert :ets.tab2list(:verk_manager) == [
               {:default, 25, :running},
               {:low_priority, 10, :running}
             ]
    end
  end

  describe "resume/1" do
    test "resume queue if queue exists" do
      queues = [{:default, 25, :paused}, {:low_priority, 10, :running}]
      init_table(queues)

      queue = :default

      expect(Verk.WorkersManager, :resume, [queue], :ok)

      assert resume(queue) == true

      assert :ets.tab2list(:verk_manager) == [
               {:default, 25, :running},
               {:low_priority, 10, :running}
             ]

      assert validate(Verk.WorkersManager)
    end

    test "does nothing if queue does not exist" do
      queues = [{:default, 25, :paused}, {:low_priority, 10, :running}]
      init_table(queues)

      queue = :no_queue

      assert pause(queue) == false

      assert :ets.tab2list(:verk_manager) == [
               {:default, 25, :paused},
               {:low_priority, 10, :running}
             ]
    end
  end

  describe "add/2" do
    test "adds queue to supervisor if not already there" do
      init_table([])

      expect(Verk.Supervisor, :start_child, [:default, 25], {:ok, :child})
      expect(Verk.Node, :add_queue!, [@node_id, :default, Verk.Redis], :ok)

      assert add(:default, 25) == {:ok, :child}
      assert :ets.tab2list(:verk_manager) == [{:default, 25, :running}]
      assert validate(Verk.Supervisor)
    end
  end

  describe "remove/1" do
    test "removes queue from supervisor if queue is running" do
      queues = [{:default, 25, :paused}, {:low_priority, 10, :running}]
      init_table(queues)

      expect(Verk.Supervisor, :stop_child, [:default], :ok)
      expect(Verk.Node, :remove_queue!, [@node_id, :default, Verk.Redis], :ok)

      assert remove(:default) == :ok
      assert :ets.tab2list(:verk_manager) == [{:low_priority, 10, :running}]
      assert validate(Verk.Supervisor)
    end

    test "does nothing if queue is not running" do
      queues = [{:default, 25, :paused}]
      init_table(queues)

      expect(Verk.Supervisor, :stop_child, [:default], {:error, :not_found})
      expect(Verk.Node, :remove_queue!, [@node_id, :default, Verk.Redis], :ok)

      assert remove(:default) == {:error, :not_found}
      assert validate(Verk.Supervisor)
    end
  end
end
