defmodule VerkTest do
  use ExUnit.Case
  import :meck
  import Verk
  alias Verk.{Time, Manager}

  setup do
    on_exit(fn -> unload() end)
    :ok
  end

  describe "add_queue/2" do
    test "returns the child spec" do
      queue = :test_queue

      expect(Manager, :add, [:test_queue, 30], {:ok, :child})

      assert add_queue(queue, 30) == {:ok, :child}

      assert validate(Manager)
    end
  end

  describe "remove_queue/1" do
    test "returns true if successfully removed" do
      queue = :test_queue

      expect(Manager, :remove, [:test_queue], :ok)

      assert remove_queue(queue) == :ok

      assert validate(Manager)
    end

    test "returns false if failed to remove" do
      queue = :test_queue

      expect(Manager, :remove, [:test_queue], {:error, :not_found})

      assert remove_queue(queue) == {:error, :not_found}

      assert validate(Manager)
    end
  end

  describe "enqueue/2" do
    test "a job with a jid and a queue passing redis connection" do
      job = %Verk.Job{
        queue: "test_queue",
        jid: "job_id",
        class: "TestWorker",
        args: [],
        max_retry_count: 1
      }

      now = Time.now()
      encoded_job = "encoded_job"
      expected_job = %Verk.Job{job | enqueued_at: now |> DateTime.to_unix()}

      expect(Time, :now, [], now)
      expect(Verk.Job, :encode!, [expected_job], encoded_job)
      expect(Redix, :command, [Verk.Redis, ["LPUSH", "queue:test_queue", encoded_job]], {:ok, :_})

      assert enqueue(job, Verk.Redis) == {:ok, "job_id"}

      assert validate([Verk.Job, Redix, Time])
    end

    test "a job with a jid and a queue passing no redis connection" do
      job = %Verk.Job{
        queue: "test_queue",
        jid: "job_id",
        class: "TestWorker",
        args: [],
        max_retry_count: 1
      }

      now = Time.now()
      encoded_job = "encoded_job"
      expected_job = %Verk.Job{job | enqueued_at: now |> DateTime.to_unix()}

      expect(Time, :now, [], now)
      expect(Verk.Job, :encode!, [expected_job], encoded_job)
      expect(Redix, :command, [Verk.Redis, ["LPUSH", "queue:test_queue", encoded_job]], {:ok, :_})

      assert enqueue(job) == {:ok, "job_id"}

      assert validate([Verk.Job, Redix, Time])
    end

    test "a job with a jid and a queue" do
      job = %Verk.Job{
        queue: "test_queue",
        jid: "job_id",
        class: "TestWorker",
        args: [],
        max_retry_count: 1
      }

      now = Time.now()
      encoded_job = "encoded_job"
      expected_job = %Verk.Job{job | enqueued_at: now |> DateTime.to_unix()}

      expect(Time, :now, [], now)
      expect(Verk.Job, :encode!, [expected_job], encoded_job)
      expect(Redix, :command, [Verk.Redis, ["LPUSH", "queue:test_queue", encoded_job]], {:ok, :_})

      assert enqueue(job) == {:ok, "job_id"}

      assert validate([Verk.Job, Redix, Time])
    end

    test "a job without a jid" do
      job = %Verk.Job{queue: "test_queue", class: "TestWorker", args: [], jid: nil}
      encoded_job = "encoded_job"

      expect(Verk.Job, :encode!, 1, encoded_job)
      expect(Redix, :command, [Verk.Redis, ["LPUSH", "queue:test_queue", encoded_job]], {:ok, :_})

      {:ok, jid} = enqueue(job)

      assert is_binary(jid)
    end

    test "a job without a max_retry_count" do
      Application.put_env(:verk, :max_retry_count, 100)
      job = %Verk.Job{queue: "test_queue", class: "TestWorker"}
      encoded_job = "encoded_job"

      expect(Verk.Job, :encode!, 1, encoded_job)
      expect(Redix, :command, [Verk.Redis, ["LPUSH", "queue:test_queue", encoded_job]], {:ok, :_})

      {:ok, jid} = enqueue(job)
      %Verk.Job{max_retry_count: max_retry_count} = capture(:first, Verk.Job, :encode!, [:_], 1)

      assert max_retry_count == 100
      assert is_binary(jid)
      Application.put_env(:verk, :max_retry_count, 25)
    end

    test "a job without a queue" do
      job = %Verk.Job{queue: nil, jid: "job_id"}

      assert enqueue(job) == {:error, {:missing_queue, job}}
    end

    test "a job with non-list args" do
      job = %Verk.Job{queue: "queue", jid: "job_id", class: "TestWorker", args: 123}

      assert enqueue(job) == {:error, {:missing_args, job}}
    end

    test "a job with no module to perform" do
      job = %Verk.Job{queue: "queue", jid: "job_id", args: [123], class: nil}

      assert enqueue(job) == {:error, {:missing_module, job}}
    end

    test "a job with non-integer max_retry_count" do
      job = %Verk.Job{
        queue: "queue",
        jid: "job_id",
        class: "TestWorker",
        args: [123],
        max_retry_count: "30"
      }

      assert enqueue(job) == {:error, {:invalid_max_retry_count, job}}
    end
  end

  describe "schedule/2" do
    test "a job with a jid, a queue and a perform_in" do
      now = Time.now()
      perform_at = Time.shift(now, 100)
      job = %Verk.Job{queue: "test_queue", jid: "job_id", class: "TestWorker", args: []}
      encoded_job = "encoded_job"
      expect(Verk.Job, :encode!, [job], encoded_job)
      perform_at_secs = DateTime.to_unix(perform_at)

      expect(
        Redix,
        :command,
        [Verk.Redis, ["ZADD", "schedule", perform_at_secs, encoded_job]],
        {:ok, :_}
      )

      assert schedule(job, perform_at) == {:ok, "job_id"}

      assert validate([Verk.Job, Redix])
    end

    test "a job with a jid, a queue and a perform_in passing a redis connection" do
      now = Time.now()
      perform_at = Time.shift(now, 100, :seconds)
      job = %Verk.Job{queue: "test_queue", jid: "job_id", class: "TestWorker", args: []}
      encoded_job = "encoded_job"
      expect(Verk.Job, :encode!, [job], encoded_job)
      perform_at_secs = DateTime.to_unix(perform_at, :seconds)

      expect(
        Redix,
        :command,
        [Verk.Redis, ["ZADD", "schedule", perform_at_secs, encoded_job]],
        {:ok, :_}
      )

      assert schedule(job, perform_at, Verk.Redis) == {:ok, "job_id"}

      assert validate([Verk.Job, Redix])
    end
  end
end
