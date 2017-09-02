defmodule Verk.ManagerTest do
  use ExUnit.Case
  import :meck
  import Verk.Manager

  setup do
    new Verk.Supervisor
    on_exit fn -> unload() end
    :ok
  end

  describe "init/1" do
    test "creates an ETS table with queues" do
      queues = [default: 25, low_priority: 10]
      init(queues)
      assert :ets.tab2list(:verk_manager) == queues
    end
  end

  describe "status/0" do
    test "returns running queues" do
      queues = [default: 25, low_priority: 10]
      init(queues)
      assert status() == queues
    end
  end

  describe "add/2" do
    test "adds queue to supervisor if not already there" do
      init([])

      expect(Verk.Supervisor, :start_child, [:default, 25], {:ok, :child})

      assert add(:default, 25) == {:ok, :child}
      assert :ets.tab2list(:verk_manager) == [default: 25]
      assert validate Verk.Supervisor
    end
  end

  describe "remove/1" do
    test "removes queue from supervisor if queue is running" do
      queues = [default: 25]
      init(queues)

      expect(Verk.Supervisor, :stop_child, [:default], :ok)

      assert remove(:default) == :ok
      assert validate Verk.Supervisor
    end

    test "does nothing if queue is not running" do
      queues = [default: 25]
      init(queues)

      expect(Verk.Supervisor, :stop_child, [:default], {:error, :not_found})

      assert remove(:default) == {:error, :not_found}
      assert validate Verk.Supervisor
    end
  end
end
