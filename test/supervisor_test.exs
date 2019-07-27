defmodule Verk.SupervisorTest do
  use ExUnit.Case
  import Verk.Supervisor

  setup do
    Application.delete_env(:verk, :local_node_id)
    Application.put_env(:verk, :node_id, "123")
    Application.put_env(:verk, :redis_pool_size, 3)

    on_exit(fn ->
      Application.delete_env(:verk, :node_id)
      Application.delete_env(:verk, :local_node_id)
      Application.delete_env(:verk, :redis_pool_size)
    end)

    :ok
  end

  describe "init/1" do
    test "defines local_node_id randomly" do
      assert {:ok, _} = init([])
      assert Application.get_env(:verk, :local_node_id) != "123"
    end

    test "does nothing if node_id if local_node_id is already defined" do
      Application.put_env(:verk, :local_node_id, "456")
      assert {:ok, _} = init([])
      assert Application.get_env(:verk, :local_node_id) == "456"
    end

    test "defines tree" do
      {:ok, {_, children}} = init([])
      [redix, node_manager, producer, stats, schedule_manager, manager_sup, drainer] = children

      assert redix == Verk.Redis.child_spec([])
      assert %{id: Verk.Node.Manager} = node_manager
      assert %{id: Verk.EventProducer} = producer
      assert %{id: Verk.QueueStats} = stats
      assert %{id: Verk.ScheduleManager} = schedule_manager

      assert %{id: Verk.Manager.Supervisor, type: :supervisor} = manager_sup

      assert {Verk.QueuesDrainer, _, _, _, :worker, [Verk.QueuesDrainer]} = drainer
    end
  end
end
