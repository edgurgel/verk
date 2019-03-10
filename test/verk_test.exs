defmodule VerkTest do
  use ExUnit.Case, async: true
  import Mimic
  import Verk
  alias Verk.{Time, Manager, Queue}

  setup :verify_on_exit!

  describe "add_queue/2" do
    test "returns the child spec" do
      queue = :test_queue

      expect(Manager, :add, fn :test_queue, 30 -> {:ok, :child} end)

      assert add_queue(queue, 30) == {:ok, :child}
    end
  end

  describe "remove_queue/1" do
    test "returns true if successfully removed" do
      queue = :test_queue

      expect(Manager, :remove, fn :test_queue -> :ok end)

      assert remove_queue(queue) == :ok
    end

    test "returns false if failed to remove" do
      queue = :test_queue

      expect(Manager, :remove, fn :test_queue -> {:error, :not_found} end)

      assert remove_queue(queue) == {:error, :not_found}
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
      expected_job = %Verk.Job{job | enqueued_at: now |> DateTime.to_unix()}

      expect(Time, :now, fn -> now end)

      expect(Queue, :enqueue, fn ^expected_job, Verk.Redis -> {:ok, "job_id"} end)

      assert enqueue(job, Verk.Redis) == {:ok, "job_id"}
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
      expected_job = %Verk.Job{job | enqueued_at: now |> DateTime.to_unix()}

      expect(Time, :now, fn -> now end)

      expect(Queue, :enqueue, fn ^expected_job, Verk.Redis -> {:ok, "job_id"} end)

      assert enqueue(job) == {:ok, "job_id"}
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
      expected_job = %Verk.Job{job | enqueued_at: now |> DateTime.to_unix()}

      expect(Time, :now, fn -> now end)
      expect(Queue, :enqueue, fn ^expected_job, Verk.Redis -> {:ok, "job_id"} end)

      assert enqueue(job) == {:ok, "job_id"}
    end

    test "a job without a jid" do
      job = %Verk.Job{queue: "test_queue", class: "TestWorker", args: [], jid: nil}

      expect(Queue, :enqueue, fn enqueued_job, Verk.Redis ->
        assert job.queue == enqueued_job.queue
        assert job.class == enqueued_job.class
        assert job.args == enqueued_job.args
        {:ok, enqueued_job.jid}
      end)

      {:ok, jid} = enqueue(job)

      assert is_binary(jid)
    end

    test "a job without a max_retry_count" do
      Application.put_env(:verk, :max_retry_count, 100)
      job = %Verk.Job{queue: "test_queue", class: "TestWorker"}

      expect(Queue, :enqueue, fn enqueued_job, Verk.Redis ->
        assert job.queue == enqueued_job.queue
        assert job.class == enqueued_job.class
        assert job.args == enqueued_job.args
        assert enqueued_job.max_retry_count == 100
        assert is_binary(enqueued_job.jid)
        {:ok, enqueued_job.jid}
      end)

      {:ok, _jid} = enqueue(job)

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
      expect(Verk.Job, :encode!, fn ^job -> encoded_job end)
      perform_at_secs = DateTime.to_unix(perform_at)

      expect(Redix, :command, fn Verk.Redis,
                                 ["ZADD", "schedule", ^perform_at_secs, ^encoded_job] ->
        {:ok, :_}
      end)

      assert schedule(job, perform_at) == {:ok, "job_id"}
    end

    test "a job with a jid, a queue and a perform_in passing a redis connection" do
      now = Time.now()
      perform_at = Time.shift(now, 100, :second)
      job = %Verk.Job{queue: "test_queue", jid: "job_id", class: "TestWorker", args: []}
      encoded_job = "encoded_job"
      expect(Verk.Job, :encode!, fn ^job -> encoded_job end)
      perform_at_secs = DateTime.to_unix(perform_at, :second)

      expect(Redix, :command, fn Verk.Redis,
                                 ["ZADD", "schedule", ^perform_at_secs, ^encoded_job] ->
        {:ok, :_}
      end)

      assert schedule(job, perform_at, Verk.Redis) == {:ok, "job_id"}
    end
  end
end
