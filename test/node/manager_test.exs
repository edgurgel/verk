defmodule Verk.Node.ManagerTest do
  use ExUnit.Case
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

      assert init([]) == {:ok, {@verk_node_id, @frequency}}
      assert_receive :heartbeat
    end
  end

  describe "handle_info/2" do
    test "heartbeat" do
      state = {@verk_node_id, @frequency}
      expect(Verk.Node, :expire_in, fn @verk_node_id, @expiration, Verk.Redis -> {:ok, 1} end)
      assert handle_info(:heartbeat, state) == {:noreply, state}
      assert_receive :heartbeat
    end
  end
end
