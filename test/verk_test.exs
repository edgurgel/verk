defmodule VerkTest do
  use ExUnit.Case
  import :meck
  import Verk
  alias Verk.Time

  setup do
    on_exit fn -> unload() end
    :ok
  end

  describe "add_queue/2" do
    test "add a new queue" do
      queue = :test_queue

      child = { :"test_queue.supervisor", { Verk.Queue.Supervisor, :start_link, [:test_queue, 30] }, :permanent, :infinity, :supervisor, [ Verk.Queue.Supervisor ] }
      expect(Supervisor, :start_child, [Verk.Supervisor, child], :ok)

      assert add_queue(queue, 30) == :ok

      assert validate Supervisor
    end
  end

  describe "remove_queue/1" do
    test "a queue successfully" do
      queue = :test_queue

      expect(Supervisor, :terminate_child, [Verk.Supervisor, :"test_queue.supervisor"], :ok)
      expect(Supervisor, :delete_child, [Verk.Supervisor, :"test_queue.supervisor"], :ok)

      assert remove_queue(queue) == :ok

      assert validate Supervisor
    end

    test "a queue unsuccessfully terminating child" do
      queue = :test_queue

      expect(Supervisor, :terminate_child, [Verk.Supervisor, :"test_queue.supervisor"], { :error, :not_found })

      assert remove_queue(queue) == { :error, :not_found }

      assert validate Supervisor
    end

    test "a queue unsuccessfully deleting child" do
      queue = :test_queue

      expect(Supervisor, :terminate_child, [Verk.Supervisor, :"test_queue.supervisor"], :ok)
      expect(Supervisor, :delete_child, [Verk.Supervisor, :"test_queue.supervisor"], { :error, :not_found })

      assert remove_queue(queue) == { :error, :not_found }

      assert validate Supervisor
    end
  end

  describe "enqueue/2" do
    test "a job with a jid and a queue passing redis connection" do
      job = %Verk.Job{ queue: "test_queue", jid: "job_id", class: "TestWorker",
        args: [], max_retry_count: 1 }
      now = Time.now
      encoded_job = "encoded_job"
      expected_job = %Verk.Job{ job | enqueued_at: now |> DateTime.to_unix }

      expect(Time, :now, [], now)
      expect(Poison, :encode!, [expected_job], encoded_job)
      expect(Redix, :command, [Verk.Redis, ["LPUSH", "queue:test_queue", encoded_job]], { :ok, :_ })

      assert enqueue(job, Verk.Redis) == { :ok, "job_id" }

      assert validate [Poison, Redix, Time]
    end

    test "a job with a jid and a queue passing no redis connection" do
      job = %Verk.Job{ queue: "test_queue", jid: "job_id", class: "TestWorker",
        args: [], max_retry_count: 1 }
      now = Time.now
      encoded_job = "encoded_job"
      expected_job = %Verk.Job{ job | enqueued_at: now |> DateTime.to_unix }

      expect(Time, :now, [], now)
      expect(Poison, :encode!, [expected_job], encoded_job)
      expect(Redix, :command, [Verk.Redis, ["LPUSH", "queue:test_queue", encoded_job]], { :ok, :_ })

      assert enqueue(job) == { :ok, "job_id" }

      assert validate [Poison, Redix, Time]
    end

    test "a job with a jid and a queue" do
      job = %Verk.Job{ queue: "test_queue", jid: "job_id", class: "TestWorker",
        args: [], max_retry_count: 1 }
      now = Time.now
      encoded_job = "encoded_job"
      expected_job = %Verk.Job{ job | enqueued_at: now |> DateTime.to_unix }

      expect(Time, :now, [], now)
      expect(Poison, :encode!, [expected_job], encoded_job)
      expect(Redix, :command, [Verk.Redis, ["LPUSH", "queue:test_queue", encoded_job]], { :ok, :_ })

      assert enqueue(job) == { :ok, "job_id" }

      assert validate [Poison, Redix, Time]
    end

    test "a job without a jid" do
      job = %Verk.Job{ queue: "test_queue", class: "TestWorker", args: [], jid: nil }
      encoded_job = "encoded_job"

      expect(Poison, :encode!, 1, encoded_job)
      expect(Redix, :command, [Verk.Redis, ["LPUSH", "queue:test_queue", encoded_job]], { :ok, :_ })

      { :ok, jid } = enqueue(job)

      assert is_binary(jid)
    end

    test "a job without a queue" do
      job = %Verk.Job{ queue: nil, jid: "job_id" }

      assert enqueue(job) == { :error, { :missing_queue, job } }
    end

    test "a job with non-list args" do
      job = %Verk.Job{ queue: "queue", jid: "job_id", class: "TestWorker", args: 123 }

      assert enqueue(job) == { :error, { :missing_args, job } }
    end

    test "a job with no module to perform" do
      job = %Verk.Job{ queue: "queue", jid: "job_id", args: [123], class: nil }

      assert enqueue(job) == { :error, { :missing_module, job } }
    end

    test "a job with non-integer max_retry_count" do
      job = %Verk.Job{ queue: "queue", jid: "job_id", class: "TestWorker",
        args: [123], max_retry_count: "30" }

      assert enqueue(job) == { :error, { :invalid_max_retry_count, job } }
    end
  end

  describe "schedule/2" do
    test "a job with a jid, a queue and a perform_in" do
      now = Time.now
      perform_at = Time.shift(now, 100)
      job = %Verk.Job{ queue: "test_queue", jid: "job_id", class: "TestWorker", args: [] }
      encoded_job = "encoded_job"
      expect(Poison, :encode!, [job], encoded_job)
      perform_at_secs = DateTime.to_unix(perform_at)
      expect(Redix, :command, [Verk.Redis, ["ZADD", "schedule", perform_at_secs, encoded_job]], { :ok, :_ })

      assert schedule(job, perform_at) == { :ok, "job_id" }

      assert validate [Poison, Redix]
    end

    test "a job with a jid, a queue and a perform_in passing a redis connection" do
      now = Time.now
      perform_at = Time.shift(now, 100, :seconds)
      job = %Verk.Job{ queue: "test_queue", jid: "job_id", class: "TestWorker", args: [] }
      encoded_job = "encoded_job"
      expect(Poison, :encode!, [job], encoded_job)
      perform_at_secs = DateTime.to_unix(perform_at, :seconds)
      expect(Redix, :command, [Verk.Redis, ["ZADD", "schedule", perform_at_secs, encoded_job]], { :ok, :_ })

      assert schedule(job, perform_at, Verk.Redis) == { :ok, "job_id" }

      assert validate [Poison, Redix]
    end
  end
end
