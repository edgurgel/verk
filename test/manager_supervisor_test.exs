defmodule Verk.ManagerSupervisorTest do
  use ExUnit.Case
  import :meck
  import Verk.Manager.Supervisor

  setup do
    new(Supervisor)

    on_exit(fn -> unload() end)

    :ok
  end

  describe "init/1" do
    test "defines tree" do
      {:ok, {_, children}} = init([])
      [manager, default] = children

      assert {Verk.Manager, _, _, _, :worker, [Verk.Manager]} = manager
      assert {:"default.supervisor", _, _, _, :supervisor, [Verk.Queue.Supervisor]} = default
    end
  end

  describe "start_child/2" do
    test "add a new queue" do
      queue = :test_queue

      child =
        {:"test_queue.supervisor", {Verk.Queue.Supervisor, :start_link, [:test_queue, 30]},
         :permanent, :infinity, :supervisor, [Verk.Queue.Supervisor]}

      expect(Supervisor, :start_child, [Verk.Manager.Supervisor, child], :ok)

      assert start_child(queue, 30) == :ok

      assert validate(Supervisor)
    end
  end

  describe "stop_child/1" do
    test "a queue successfully" do
      queue = :test_queue

      expect(
        Supervisor,
        :terminate_child,
        [Verk.Manager.Supervisor, :"test_queue.supervisor"],
        :ok
      )

      expect(Supervisor, :delete_child, [Verk.Manager.Supervisor, :"test_queue.supervisor"], :ok)

      assert stop_child(queue) == :ok

      assert validate(Supervisor)
    end

    test "a queue unsuccessfully terminating child" do
      queue = :test_queue

      expect(
        Supervisor,
        :terminate_child,
        [Verk.Manager.Supervisor, :"test_queue.supervisor"],
        {:error, :not_found}
      )

      assert stop_child(queue) == {:error, :not_found}

      assert validate(Supervisor)
    end

    test "a queue unsuccessfully deleting child" do
      queue = :test_queue

      expect(
        Supervisor,
        :terminate_child,
        [Verk.Manager.Supervisor, :"test_queue.supervisor"],
        :ok
      )

      expect(
        Supervisor,
        :delete_child,
        [Verk.Manager.Supervisor, :"test_queue.supervisor"],
        {:error, :not_found}
      )

      assert stop_child(queue) == {:error, :not_found}

      assert validate(Supervisor)
    end
  end
end
