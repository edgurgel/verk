defmodule Verk.ManagerSupervisorTest do
  use ExUnit.Case
  import Verk.Manager.Supervisor

  describe "init/1" do
    test "defines tree" do
      {:ok, {_, children}} = init([])
      [manager, default] = children

      assert {Verk.Manager, _, _, _, :worker, [Verk.Manager]} = manager
      assert {:"default.supervisor", _, _, _, :supervisor, [Verk.Queue.Supervisor]} = default
    end
  end
end
