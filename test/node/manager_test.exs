defmodule Verk.Node.ManagerTest do
  use ExUnit.Case
  alias Verk.InProgressQueue
  import Verk.Node.Manager
  import Mimic

  setup :verify_on_exit!

  @verk_node_id "node-manager-test"
  @frequency 10
  @expiration 2 * @frequency

  setup do
    Application.put_env(:verk, :local_node_id, @verk_node_id)
    Application.put_env(:verk, :heartbeat, @frequency)

    on_exit(fn ->
      Application.delete_env(:verk, :local_node_id)
      Application.delete_env(:verk, :heartbeat)
    end)

    :ok
  end

  describe "init/1" do
    test "registers local verk node id" do
      expect(Verk.Node, :expire_in, fn @verk_node_id, @expiration, Verk.Redis -> {:ok, 1} end)
      expect(Verk.Scripts, :load, fn _ -> :ok end)

      assert init([]) == {:ok, {@verk_node_id, @frequency}}
      assert_receive :heartbeat
    end
  end

  describe "handle_info/2" do
    test "heartbeat when only one node" do
      state = {@verk_node_id, @frequency}
      expect(Verk.Node, :members, fn 0, Verk.Redis -> {:ok, [@verk_node_id]} end)
      expect(Verk.Node, :expire_in, fn @verk_node_id, @expiration, Verk.Redis -> {:ok, 1} end)
      assert handle_info(:heartbeat, state) == {:noreply, state}
      assert_receive :heartbeat
    end

    test "heartbeat when more than 1 node - alive" do
      alive_node_id = "alive-node"
      state = {@verk_node_id, @frequency}
      expect(Verk.Node, :members, fn 0, Verk.Redis -> {:ok, [@verk_node_id, alive_node_id]} end)
      expect(Verk.Node, :ttl!, fn ^alive_node_id, Verk.Redis -> 500 end)
      expect(Verk.Node, :expire_in, fn @verk_node_id, @expiration, Verk.Redis -> {:ok, 1} end)
      assert handle_info(:heartbeat, state) == {:noreply, state}
      assert_receive :heartbeat
    end

    test "heartbeat when more than 1 node - dead" do
      dead_node_id = "dead-node"
      state = {@verk_node_id, @frequency}

      expect(Verk.Node, :members, fn 0, Verk.Redis -> {:more, [dead_node_id], 123} end)
      expect(Verk.Node, :members, fn 123, Verk.Redis -> {:ok, [@verk_node_id]} end)
      expect(Verk.Node, :ttl!, fn ^dead_node_id, Verk.Redis -> -2 end)
      expect(Verk.Node, :expire_in, fn @verk_node_id, @expiration, Verk.Redis -> {:ok, 1} end)
      expect(Verk.Node, :queues!, fn "dead-node", 0, Verk.Redis -> {:more, ["queue_1"], 123} end)
      expect(Verk.Node, :queues!, fn "dead-node", 123, Verk.Redis -> {:ok, ["queue_2"]} end)

      expect(InProgressQueue, :enqueue_in_progress, fn "queue_1", ^dead_node_id, Verk.Redis ->
        {:ok, [3, 5]}
      end)

      expect(InProgressQueue, :enqueue_in_progress, fn "queue_1", ^dead_node_id, Verk.Redis ->
        {:ok, [0, 3]}
      end)

      expect(InProgressQueue, :enqueue_in_progress, fn "queue_2", ^dead_node_id, Verk.Redis ->
        {:ok, [0, 1]}
      end)

      expect(Verk.Node, :deregister!, fn ^dead_node_id, Verk.Redis -> :ok end)

      assert handle_info(:heartbeat, state) == {:noreply, state}
      assert_receive :heartbeat
    end
  end

  describe "terminate/2" do
    test "non-shutdown reason" do
      state = {@verk_node_id, @frequency}
      assert terminate(:normal, state) == :ok
    end

    test "deregisters itself" do
      state = {@verk_node_id, @frequency}

      expect(Verk.Node, :queues!, fn @verk_node_id, 0, Verk.Redis -> {:more, ["queue_1"], 123} end)

      expect(Verk.Node, :queues!, fn @verk_node_id, 123, Verk.Redis -> {:ok, ["queue_2"]} end)

      expect(InProgressQueue, :enqueue_in_progress, fn "queue_1", @verk_node_id, Verk.Redis ->
        {:ok, [3, 5]}
      end)

      expect(InProgressQueue, :enqueue_in_progress, fn "queue_1", @verk_node_id, Verk.Redis ->
        {:ok, [0, 3]}
      end)

      expect(InProgressQueue, :enqueue_in_progress, fn "queue_2", @verk_node_id, Verk.Redis ->
        {:ok, [0, 1]}
      end)

      expect(Verk.Node, :deregister!, fn @verk_node_id, Verk.Redis -> :ok end)

      assert terminate(:shutdown, state) == :ok
    end
  end
end
