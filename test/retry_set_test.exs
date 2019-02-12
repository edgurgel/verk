defmodule Verk.RetrySetTest do
  use ExUnit.Case, async: true
  alias Verk.SortedSet
  import Verk.RetrySet
  import Mimic

  setup :verify_on_exit!

  defmodule DummyJob do
    def retry_at(_failed_at, _retry_count) do
      4
    end
  end

  setup do
    :rand.seed(:exs1024, {123, 123_534, 345_345})
    :ok
  end

  @key key()

  describe "add/3" do
    test "adds a job" do
      job = %Verk.Job{retry_count: 1}
      failed_at = 1
      retry_at = "29.0"
      expect(Verk.Job, :encode!, fn ^job -> :payload end)
      expect(Redix, :command, fn :redis, ["ZADD", "retry", ^retry_at, :payload] -> {:ok, 1} end)

      assert add(job, failed_at, :redis) == :ok
    end

    test "allows custom retry_at" do
      job = %Verk.Job{class: "Verk.RetrySetTest.DummyJob", retry_count: 1}
      failed_at = 1
      retry_at = "4"
      expect(Verk.Job, :encode!, fn ^job -> :payload end)
      expect(Redix, :command, fn :redis, ["ZADD", "retry", ^retry_at, :payload] -> {:ok, 1} end)

      assert add(job, failed_at, :redis) == :ok
    end

    test "custom retry_at but module doesn't exist" do
      job = %Verk.Job{class: "Verk.NoModule", retry_count: 1}
      failed_at = 1
      retry_at = "29.0"
      expect(Verk.Job, :encode!, fn ^job -> :payload end)
      expect(Redix, :command, fn :redis, ["ZADD", "retry", ^retry_at, :payload] -> {:ok, 1} end)

      assert add(job, failed_at, :redis) == :ok
    end
  end

  describe "add!/3" do
    test "add!" do
      job = %Verk.Job{retry_count: 1}
      failed_at = 1
      retry_at = "29.0"
      expect(Verk.Job, :encode!, fn ^job -> :payload end)
      expect(Redix, :command, fn :redis, ["ZADD", "retry", ^retry_at, :payload] -> {:ok, 1} end)

      assert add!(job, failed_at, :redis) == nil
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
      expect(SortedSet, :clear, fn @key, Verk.Redis -> {:ok, false} end)

      assert clear() == {:ok, false}
    end
  end

  describe "clear!/0" do
    test "clear!" do
      expect(SortedSet, :clear!, fn @key, Verk.Redis -> true end)

      assert clear!() == true
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
    test "range with start and stop" do
      job = %Verk.Job{class: "Class", args: []}
      json = Verk.Job.encode!(job)

      expect(SortedSet, :range, fn @key, 1, 2, Verk.Redis ->
        {:ok, [%{job | original_json: json}]}
      end)

      assert range(1, 2) == {:ok, [%{job | original_json: json}]}
    end
  end

  describe "range!/0" do
    test "range!" do
      job = %Verk.Job{class: "Class", args: []}
      json = Verk.Job.encode!(job)

      expect(SortedSet, :range!, fn @key, 0, -1, Verk.Redis -> [%{job | original_json: json}] end)

      assert range!() == [%{job | original_json: json}]
    end
  end

  describe "range_with_score/0" do
    test "range_with_score" do
      job = %Verk.Job{class: "Class", args: []}
      json = Verk.Job.encode!(job)

      expect(SortedSet, :range_with_score, fn @key, 0, -1, Verk.Redis ->
        {:ok, [{%{job | original_json: json}, 45}]}
      end)

      assert range_with_score() == {:ok, [{%{job | original_json: json}, 45}]}
    end
  end

  describe "range_with_score/2" do
    test "range_with_score with start and stop" do
      job = %Verk.Job{class: "Class", args: []}
      json = Verk.Job.encode!(job)

      expect(SortedSet, :range_with_score, fn @key, 1, 2, Verk.Redis ->
        {:ok, [{%{job | original_json: json}, 45}]}
      end)

      assert range_with_score(1, 2) == {:ok, [{%{job | original_json: json}, 45}]}
    end
  end

  describe "range_with_score!/0" do
    test "range_with_score!" do
      job = %Verk.Job{class: "Class", args: []}
      json = Verk.Job.encode!(job)

      expect(SortedSet, :range_with_score!, fn @key, 0, -1, Verk.Redis ->
        [{%{job | original_json: json}, 45}]
      end)

      assert range_with_score!() == [{%{job | original_json: json}, 45}]
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

    test "job with no original_json" do
      json = %Verk.Job{class: "Class", args: []} |> Verk.Job.encode!()
      expect(SortedSet, :delete_job, fn @key, ^json, Verk.Redis -> {:ok, true} end)

      assert delete_job(json) == {:ok, true}
    end
  end

  describe "delete_job!/1" do
    test "job with original_json" do
      job = %Verk.Job{class: "Class", args: []}
      json = Verk.Job.encode!(job)
      job = %{job | original_json: json}

      expect(SortedSet, :delete_job!, fn @key, ^json, Verk.Redis -> true end)

      assert delete_job!(job) == true
    end

    test "job with no original_json" do
      json = %Verk.Job{class: "Class", args: []} |> Verk.Job.encode!()
      expect(SortedSet, :delete_job!, fn @key, ^json, Verk.Redis -> true end)

      assert delete_job!(json) == true
    end
  end

  describe "requeue_job/1" do
    test "job with original_json" do
      job = %Verk.Job{class: "Class", args: []}
      json = Verk.Job.encode!(job)
      job = %{job | original_json: json}

      expect(SortedSet, :requeue_job, fn @key, ^json, Verk.Redis -> :ok end)

      assert requeue_job(job) == :ok
    end

    test "job with no original_json" do
      json = %Verk.Job{class: "Class", args: []} |> Verk.Job.encode!()
      expect(SortedSet, :requeue_job, fn @key, ^json, Verk.Redis -> {:ok, true} end)

      assert requeue_job(json) == {:ok, true}
    end
  end

  describe "requeue_job!/1" do
    test "job with original_json" do
      job = %Verk.Job{class: "Class", args: []}
      json = Verk.Job.encode!(job)
      job = %{job | original_json: json}

      expect(SortedSet, :requeue_job!, fn @key, ^json, Verk.Redis -> true end)

      assert requeue_job!(job) == true
    end

    test "job with no original_json" do
      json = %Verk.Job{class: "Class", args: []} |> Verk.Job.encode!()
      expect(SortedSet, :requeue_job!, fn @key, ^json, Verk.Redis -> true end)

      assert requeue_job!(json) == true
    end
  end
end
