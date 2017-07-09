defmodule Verk.QueueTest do
  use ExUnit.Case
  import Verk.Queue

  @queue     "default"
  @queue_key "queue:default"

  setup do
    { :ok, pid } = Confex.get_env(:verk, :redis_url)
                    |> Redix.start_link([name: Verk.Redis])
    Redix.command!(pid, ~w(DEL #{@queue_key}))
    on_exit fn ->
      ref = Process.monitor(pid)
      assert_receive {:DOWN, ^ref, _, _, _}
    end
    :ok
  end

  describe "count/1" do
    test "empty queue" do
      assert count(@queue) == {:ok, 0}
    end

    test "non-empty queue" do
      Redix.command!(Verk.Redis, ~w(LPUSH #{@queue_key} 1 2 3))

      assert count(@queue) == {:ok, 3}
    end
  end

  describe "count!/1" do
    test "non-empty queue" do
      Redix.command!(Verk.Redis, ~w(LPUSH #{@queue_key} 1 2 3))

      assert count!(@queue) == 3
    end

    test "empty queue" do
      assert count!(@queue) == 0
    end
  end

  describe "clear/1" do
    test "clear queue" do
      assert clear(@queue) == {:ok, false}

      Redix.command!(Verk.Redis, ~w(LPUSH #{@queue_key} 1 2 3))

      assert clear(@queue) == {:ok, true}

      assert Redix.command!(Verk.Redis, ~w(GET #{@queue_key})) == nil
    end
  end

  describe "clear!/1" do
    test "clear!" do
      assert clear!(@queue) == false

      Redix.command!(Verk.Redis, ~w(LPUSH #{@queue_key} 1 2 3))

      assert clear!(@queue) == true

      assert Redix.command!(Verk.Redis, ~w(GET #{@queue_key})) == nil
    end
  end

  describe "range/1" do
    test "with items" do
      job = %Verk.Job{class: "Class", args: []}
      json = Poison.encode!(job)
      Redix.command!(Verk.Redis, ~w(LPUSH #{@queue_key} #{json}))

      assert range(@queue) == {:ok, [%{ job | original_json: json }]}
    end

    test "with no items" do
      assert range(@queue) == {:ok, []}
    end
  end


  describe "range!/1" do
    test "with items" do
      job = %Verk.Job{class: "Class", args: []}
      json = Poison.encode!(job)
      Redix.command!(Verk.Redis, ~w(LPUSH #{@queue_key} #{json}))

      assert range!(@queue) == [%{ job | original_json: json }]
    end

    test "with no items" do
      assert range!(@queue) == []
    end
  end

  describe "delete_job/2" do
    test "no job inside the queue" do
      assert delete_job(@queue, %Verk.Job{}) == {:ok, false}
    end

    test "job with original_json" do
      job = %Verk.Job{class: "Class", args: []}
      json = Poison.encode!(job)

      Redix.command!(Verk.Redis, ~w(LPUSH #{@queue_key} #{json}))

      job = %{ job | original_json: json}

      assert delete_job(@queue, job) == {:ok, true}
    end

    test "job with no original_json" do
      json = %Verk.Job{class: "Class", args: []} |> Poison.encode!

      Redix.command!(Verk.Redis, ~w(LPUSH #{@queue_key} #{json}))

      assert delete_job(@queue, json) == {:ok, true}
    end
  end

  describe "delete_job!/2" do
    test "no job inside the queue" do
      assert delete_job!(@queue, %Verk.Job{}) == false
    end

    test "job with original_json" do
      job = %Verk.Job{class: "Class", args: []}
      json = Poison.encode!(job)

      Redix.command!(Verk.Redis, ~w(LPUSH #{@queue_key} #{json}))

      job = %{ job | original_json: json}

      assert delete_job!(@queue, job) == true
    end

    test "job with no original_json" do
      json = %Verk.Job{class: "Class", args: []} |> Poison.encode!

      Redix.command!(Verk.Redis, ~w(LPUSH #{@queue_key} #{json}))

      assert delete_job!(@queue, json) == true
    end
  end
end
