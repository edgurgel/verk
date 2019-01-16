defmodule Verk.Node.ManagerTest do
  use ExUnit.Case
  alias Verk.InProgressQueue
  import Verk.Node.Manager
  import :meck

  @verk_node_id "node-manager-test"
  @frequency 10

  setup do
    new([Verk.Node, InProgressQueue], [:merge_expects])
    Application.put_env(:verk, :local_node_id, @verk_node_id)
    Application.put_env(:verk, :heartbeat, @frequency)

    on_exit(fn ->
      Application.delete_env(:verk, :local_node_id)
      Application.delete_env(:verk, :heartbeat)
      unload()
    end)

    :ok
  end

  describe "init/1" do
    test "registers local verk node id" do
      expect(Verk.Node, :register, [@verk_node_id, 2 * @frequency, Verk.Redis], :ok)
      expect(Verk.Scripts, :load, 1, :ok)

      assert init([]) == {:ok, {@verk_node_id, @frequency}}
      assert_receive :heartbeat
    end
  end

  describe "handle_info/2" do
    test "heartbeat when only one node" do
      state = {@verk_node_id, @frequency}
      expect(Verk.Node, :members, [0, Verk.Redis], {:ok, [@verk_node_id]})
      expect(Verk.Node, :expire_in, [@verk_node_id, 2 * @frequency, Verk.Redis], {:ok, 1})
      assert handle_info(:heartbeat, state) == {:noreply, state}
      assert_receive :heartbeat
    end

    test "heartbeat when more than 1 node - alive" do
      alive_node_id = "alive-node"
      state = {@verk_node_id, @frequency}
      expect(Verk.Node, :members, [0, Verk.Redis], {:ok, [@verk_node_id, alive_node_id]})
      expect(Verk.Node, :ttl!, [alive_node_id, Verk.Redis], 500)
      expect(Verk.Node, :expire_in, [@verk_node_id, 2 * @frequency, Verk.Redis], {:ok, 1})
      assert handle_info(:heartbeat, state) == {:noreply, state}
      assert_receive :heartbeat
    end

    test "heartbeat when more than 1 node - dead" do
      dead_node_id = "dead-node"
      state = {@verk_node_id, @frequency}

      expect(Verk.Node, :members, [0, Verk.Redis], {:more, [@verk_node_id], 123})
      expect(Verk.Node, :members, [123, Verk.Redis], {:ok, [dead_node_id]})
      expect(Verk.Node, :ttl!, [dead_node_id, Verk.Redis], -2)
      expect(Verk.Node, :expire_in, [@verk_node_id, 2 * @frequency, Verk.Redis], {:ok, 1})
      expect(Verk.Node, :queues!, ["dead-node", 0, Verk.Redis], {:more, ["queue_1"], 123})
      expect(Verk.Node, :queues!, ["dead-node", 123, Verk.Redis], {:ok, ["queue_2"]})

      expect(
        InProgressQueue,
        :enqueue_in_progress,
        ["queue_1", dead_node_id, Verk.Redis],
        seq([{:ok, [3, 5]}, {:ok, [0, 3]}])
      )

      expect(
        InProgressQueue,
        :enqueue_in_progress,
        ["queue_2", dead_node_id, Verk.Redis],
        {:ok, [0, 1]}
      )

      expect(Verk.Node, :deregister!, [dead_node_id, Verk.Redis], :ok)

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

      expect(Verk.Node, :queues!, [@verk_node_id, 0, Verk.Redis], {:more, ["queue_1"], 123})
      expect(Verk.Node, :queues!, [@verk_node_id, 123, Verk.Redis], {:ok, ["queue_2"]})

      expect(
        InProgressQueue,
        :enqueue_in_progress,
        ["queue_1", @verk_node_id, Verk.Redis],
        seq([{:ok, [3, 5]}, {:ok, [0, 3]}])
      )

      expect(
        InProgressQueue,
        :enqueue_in_progress,
        ["queue_2", @verk_node_id, Verk.Redis],
        {:ok, [0, 1]}
      )

      expect(Verk.Node, :deregister!, [@verk_node_id, Verk.Redis], :ok)

      assert terminate(:shutdown, state) == :ok
    end
  end
end
