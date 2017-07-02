defmodule Verk.DeadSetTest do
  use ExUnit.Case
  alias Verk.SortedSet
  import Verk.DeadSet
  import :meck

  setup do
    { :ok, redis } = Confex.get_env(:verk, :redis_url) |> Redix.start_link
    Redix.command!(redis, ~w(DEL dead))
    on_exit fn -> unload() end
    { :ok, %{ redis: redis } }
  end

  describe "add/3" do
    test "adds a job", %{ redis: redis } do
      job = %Verk.Job{ retry_count: 1 }
      payload = Poison.encode!(job)
      failed_at = 1

      assert add(job, failed_at, redis) == :ok

      assert Redix.command!(redis, ["ZRANGE", key(), 0, -1, "WITHSCORES"]) == [payload, to_string(failed_at)]
    end

    test "replaces old jobs", %{ redis: redis } do
      job1 = %Verk.Job{ retry_count: 1 }
      payload1 = Poison.encode!(job1)
      job2 = %Verk.Job{ retry_count: 2 }
      payload2 = Poison.encode!(job2)
      failed_at = 60 * 60 * 24 * 7

      assert add(job1, failed_at + 1, redis) == :ok
      assert add(job2, failed_at + 2, redis) == :ok

      assert Redix.command!(redis, ["ZRANGE", key(), 0, 2, "WITHSCORES"])
        == [payload1, to_string(failed_at + 1), payload2, to_string(failed_at + 2)]
    end
  end

  describe "add!/3" do
    test "add!/3", %{ redis: redis } do
      job = %Verk.Job{ retry_count: 1 }
      payload = Poison.encode!(job)
      failed_at = 1

      assert add!(job, failed_at, redis) == nil

      assert Redix.command!(redis, ["ZRANGE", key(), 0, -1, "WITHSCORES"]) == [payload, to_string(failed_at)]
    end

    test "replaces old jobs", %{ redis: redis } do
      job1 = %Verk.Job{ retry_count: 1 }
      payload1 = Poison.encode!(job1)
      job2 = %Verk.Job{ retry_count: 2 }
      payload2 = Poison.encode!(job2)
      failed_at = 60 * 60 * 24 * 7

      assert add!(job1, failed_at + 1, redis) == nil
      assert add!(job2, failed_at + 2, redis) == nil

      assert Redix.command!(redis, ["ZRANGE", key(), 0, 2, "WITHSCORES"])
        == [payload1, to_string(failed_at + 1), payload2, to_string(failed_at + 2)]
    end
  end

  describe "count/0" do
    test "count" do
      expect(SortedSet, :count, [key(), Verk.Redis], {:ok, 1})

      assert count() == {:ok, 1}
      assert validate SortedSet
    end
  end

  describe "count!/0" do
    test "count!" do
      expect(SortedSet, :count!, [key(), Verk.Redis], 1)

      assert count!() == 1
      assert validate SortedSet
    end
  end

  describe "clear/0" do
    test "clear" do
      expect(SortedSet, :clear, [key(), Verk.Redis], :ok)

      assert clear() == :ok
      assert validate SortedSet
    end
  end

  describe "clear!/0" do
    test "clear!" do
      expect(SortedSet, :clear!, [key(), Verk.Redis], nil)

      assert clear!() == nil
      assert validate SortedSet
    end
  end

  describe "range/0" do
    test "range" do
      job = %Verk.Job{class: "Class", args: []}
      json = Poison.encode!(job)

      expect(SortedSet, :range, [key(), 0, -1, Verk.Redis], {:ok, [%{ job | original_json: json }]})

      assert range() == {:ok, [%{ job | original_json: json }]}
      assert validate SortedSet
    end
  end

  describe "range/2" do
    test "respects start and stop" do
      job = %Verk.Job{class: "Class", args: []}
      json = Poison.encode!(job)

      expect(SortedSet, :range, [key(), 1, 2, Verk.Redis], {:ok, [%{ job | original_json: json }]})

      assert range(1, 2) == {:ok, [%{ job | original_json: json }]}
      assert validate SortedSet
    end
  end

  describe "range!/0" do
    test "range" do
      job = %Verk.Job{class: "Class", args: []}
      json = Poison.encode!(job)

      expect(SortedSet, :range!, [key(), 0, -1, Verk.Redis], [%{ job | original_json: json }])

      assert range!() == [%{ job | original_json: json }]
      assert validate SortedSet
    end
  end

  describe "range!/2" do
    test "respects start and stop" do
      job = %Verk.Job{class: "Class", args: []}
      json = Poison.encode!(job)

      expect(SortedSet, :range!, [key(), 1, 2, Verk.Redis], [%{ job | original_json: json }])

      assert range!(1, 2) == [%{ job | original_json: json }]
      assert validate SortedSet
    end
  end

  describe "delete_job/1" do
    test "job with original_json" do
      job = %Verk.Job{class: "Class", args: []}
      json = Poison.encode!(job)
      job = %{ job | original_json: json }

      expect(SortedSet, :delete_job, [key(), json, Verk.Redis], :ok)

      assert delete_job(job) == :ok
      assert validate SortedSet
    end

    test "with original_json" do
      json = %Verk.Job{class: "Class", args: []} |> Poison.encode!
      expect(SortedSet, :delete_job, [key(), json, Verk.Redis], :ok)

      assert delete_job(json) == :ok
      assert validate SortedSet
    end
  end

  describe "delete_job!" do
    test "job with original_json" do
      job = %Verk.Job{class: "Class", args: []}
      json = Poison.encode!(job)
      job = %{ job | original_json: json }

      expect(SortedSet, :delete_job!, [key(), json, Verk.Redis], nil)

      assert delete_job!(job) == nil
      assert validate SortedSet
    end

    test "with original_json" do
      json = %Verk.Job{class: "Class", args: []} |> Poison.encode!
      expect(SortedSet, :delete_job!, [key(), json, Verk.Redis], :nil)

      assert delete_job!(json) == nil
      assert validate SortedSet
    end
  end
end
