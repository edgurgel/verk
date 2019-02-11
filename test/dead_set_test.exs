defmodule Verk.DeadSetTest do
  use ExUnit.Case, async: true
  alias Verk.SortedSet
  import Verk.DeadSet
  import Mimic

  setup :verify_on_exit!

  @key key()

  setup do
    {:ok, redis} = Confex.get_env(:verk, :redis_url) |> Redix.start_link()
    Redix.command!(redis, ~w(DEL dead))
    {:ok, %{redis: redis}}
  end

  describe "add/3" do
    test "adds a job", %{redis: redis} do
      job = %Verk.Job{retry_count: 1}
      payload = Verk.Job.encode!(job)
      failed_at = 1

      assert add(job, failed_at, redis) == :ok

      assert Redix.command!(redis, ["ZRANGE", @key, 0, -1, "WITHSCORES"]) == [
               payload,
               to_string(failed_at)
             ]
    end

    test "replaces old jobs", %{redis: redis} do
      job1 = %Verk.Job{retry_count: 1}
      payload1 = Verk.Job.encode!(job1)
      job2 = %Verk.Job{retry_count: 2}
      payload2 = Verk.Job.encode!(job2)
      failed_at = 60 * 60 * 24 * 7

      assert add(job1, failed_at + 1, redis) == :ok
      assert add(job2, failed_at + 2, redis) == :ok

      assert Redix.command!(redis, ["ZRANGE", @key, 0, 2, "WITHSCORES"]) ==
               [payload1, to_string(failed_at + 1), payload2, to_string(failed_at + 2)]
    end

    test "allows setting max_dead_jobs size, limiting queue size", %{redis: redis} do
      Application.put_env(:verk, :max_dead_jobs, 1)

      job1 = %Verk.Job{retry_count: 1}
      _payload1 = Verk.Job.encode!(job1)
      job2 = %Verk.Job{retry_count: 2}
      payload2 = Verk.Job.encode!(job2)
      failed_at = 60 * 60 * 24 * 7

      assert add(job1, failed_at + 1, redis) == :ok
      assert add(job2, failed_at + 2, redis) == :ok

      assert Redix.command!(redis, ["ZRANGE", @key, 0, 2, "WITHSCORES"]) ==
               [payload2, to_string(failed_at + 2)]

      Application.put_env(:verk, :max_dead_jobs, 100)
    end
  end

  describe "add!/3" do
    test "add!/3", %{redis: redis} do
      job = %Verk.Job{retry_count: 1}
      payload = Verk.Job.encode!(job)
      failed_at = 1

      assert add!(job, failed_at, redis) == nil

      assert Redix.command!(redis, ["ZRANGE", @key, 0, -1, "WITHSCORES"]) == [
               payload,
               to_string(failed_at)
             ]
    end

    test "replaces old jobs", %{redis: redis} do
      job1 = %Verk.Job{retry_count: 1}
      payload1 = Verk.Job.encode!(job1)
      job2 = %Verk.Job{retry_count: 2}
      payload2 = Verk.Job.encode!(job2)
      failed_at = 60 * 60 * 24 * 7

      assert add!(job1, failed_at + 1, redis) == nil
      assert add!(job2, failed_at + 2, redis) == nil

      assert Redix.command!(redis, ["ZRANGE", @key, 0, 2, "WITHSCORES"]) ==
               [payload1, to_string(failed_at + 1), payload2, to_string(failed_at + 2)]
    end
  end

  describe "count/0" do
    test "count" do
      expect(SortedSet, :count, fn @key, Verk.Redis -> {:ok, 1} end)

      assert count() == {:ok, 1}
    end
  end

  describe "count!/0" do
    test "count!" do
      expect(SortedSet, :count!, fn @key, Verk.Redis -> 1 end)

      assert count!() == 1
    end
  end

  describe "clear/0" do
    test "clear" do
      expect(SortedSet, :clear, fn @key, Verk.Redis -> :ok end)

      assert clear() == :ok
    end
  end

  describe "clear!/0" do
    test "clear!" do
      expect(SortedSet, :clear!, fn @key, Verk.Redis -> nil end)

      assert clear!() == nil
    end
  end

  describe "range/0" do
    test "range" do
      job = %Verk.Job{class: "Class", args: []}
      json = Verk.Job.encode!(job)

      expect(SortedSet, :range, fn @key, 0, -1, Verk.Redis ->
        {:ok, [%{job | original_json: json}]}
      end)

      assert range() == {:ok, [%{job | original_json: json}]}
    end
  end

  describe "range/2" do
    test "respects start and stop" do
      job = %Verk.Job{class: "Class", args: []}
      json = Verk.Job.encode!(job)

      expect(SortedSet, :range, fn @key, 1, 2, Verk.Redis ->
        {:ok, [%{job | original_json: json}]}
      end)

      assert range(1, 2) == {:ok, [%{job | original_json: json}]}
    end
  end

  describe "range!/0" do
    test "range" do
      job = %Verk.Job{class: "Class", args: []}
      json = Verk.Job.encode!(job)

      expect(SortedSet, :range!, fn @key, 0, -1, Verk.Redis -> [%{job | original_json: json}] end)

      assert range!() == [%{job | original_json: json}]
    end
  end

  describe "range!/2" do
    test "respects start and stop" do
      job = %Verk.Job{class: "Class", args: []}
      json = Verk.Job.encode!(job)

      expect(SortedSet, :range!, fn @key, 1, 2, Verk.Redis -> [%{job | original_json: json}] end)

      assert range!(1, 2) == [%{job | original_json: json}]
    end
  end

  describe "delete_job/1" do
    test "job with original_json" do
      job = %Verk.Job{class: "Class", args: []}
      json = Verk.Job.encode!(job)
      job = %{job | original_json: json}

      expect(SortedSet, :delete_job, fn @key, ^json, Verk.Redis -> :ok end)

      assert delete_job(job) == :ok
    end

    test "with original_json" do
      json = %Verk.Job{class: "Class", args: []} |> Verk.Job.encode!()
      expect(SortedSet, :delete_job, fn @key, ^json, Verk.Redis -> :ok end)

      assert delete_job(json) == :ok
    end
  end

  describe "delete_job!" do
    test "job with original_json" do
      job = %Verk.Job{class: "Class", args: []}
      json = Verk.Job.encode!(job)
      job = %{job | original_json: json}

      expect(SortedSet, :delete_job!, fn @key, ^json, Verk.Redis -> nil end)

      assert delete_job!(job) == nil
    end

    test "with original_json" do
      json = %Verk.Job{class: "Class", args: []} |> Verk.Job.encode!()
      expect(SortedSet, :delete_job!, fn @key, ^json, Verk.Redis -> nil end)

      assert delete_job!(json) == nil
    end
  end
end
