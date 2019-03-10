defmodule Verk.QueueManagerTest do
  use ExUnit.Case, async: true
  import Verk.QueueManager
  import Mimic
  alias Verk.{Job, Queue, RetrySet, DeadSet, Redis, Node}

  setup :verify_on_exit!

  @node_id "1"

  setup do
    Application.put_env(:verk, :local_node_id, @node_id)
    stub(Redis, :random, fn -> :redis end)

    on_exit(fn ->
      Application.delete_env(:verk, :local_node_id)
      Application.delete_env(:verk, :generate_node_id)
    end)

    :ok
  end

  describe "name/1" do
    test "returns queue manager name" do
      assert name("queue_name") == :"queue_name.queue_manager"
      assert name(:queue_name) == :"queue_name.queue_manager"
    end
  end

  describe "init/1" do
    test "sets up state" do
      command = ["XGROUP", "CREATE", "verk:queue:queue_name", "verk", 0, "MKSTREAM"]
      expect(Verk.Scripts, :load, fn _ -> :ok end)
      expect(Redix, :command, fn _redis, ^command -> :ok end)
      assert init(["queue_name"]) == {:ok, "queue_name"}
    end
  end

  describe "handle_call/3 retry" do
    test "retry on a job with retry count 0" do
      failed_at = 100

      job = %Job{
        retry_count: 1,
        failed_at: failed_at,
        error_backtrace: "\n",
        error_message: "reasons"
      }

      expect(RetrySet, :add!, fn ^job, ^failed_at, :redis -> "payload" end)

      state = "queue_name"
      job = %Job{retry_count: 0}
      exception = RuntimeError.exception("reasons")

      assert handle_call({:retry, job, failed_at, exception, []}, :from, state) ==
               {:reply, :ok, state}
    end

    test "retry on a job with no retry_count" do
      failed_at = 100

      job = %Job{
        retry_count: 1,
        failed_at: failed_at,
        error_backtrace: "\n",
        error_message: "reasons"
      }

      expect(RetrySet, :add!, fn ^job, ^failed_at, :redis -> :ok end)

      state = "queue_name"
      job = %Job{retry_count: nil}
      exception = RuntimeError.exception("reasons")

      assert handle_call({:retry, job, failed_at, exception, []}, :from, state) ==
               {:reply, :ok, state}
    end

    test "retry on a job with retry_count less than max_retry_count" do
      failed_at = 100

      job = %Job{
        retry_count: 2,
        retried_at: failed_at,
        error_backtrace: "\n",
        error_message: "reasons",
        max_retry_count: 4
      }

      expect(RetrySet, :add!, fn ^job, ^failed_at, :redis -> :ok end)

      state = "queue_name"
      job = %Job{retry_count: 1, max_retry_count: 4}
      exception = RuntimeError.exception("reasons")

      assert handle_call({:retry, job, failed_at, exception, []}, :from, state) ==
               {:reply, :ok, state}
    end

    test "retry on a job failing to add to retry set" do
      failed_at = 100

      job = %Job{
        retry_count: 1,
        failed_at: failed_at,
        error_backtrace: "\n",
        error_message: "reasons"
      }

      expect(RetrySet, :add!, fn ^job, ^failed_at, :redis ->
        raise %Redix.Error{message: "fail"}
      end)

      state = "queue_name"
      job = %Job{retry_count: 0}
      exception = RuntimeError.exception("reasons")

      assert_raise Redix.Error, fn ->
        handle_call({:retry, job, failed_at, exception, []}, :from, state) == {:reply, :ok, state}
      end
    end

    test "retry on a job with nil max_retry_count and retry_count greater than default" do
      failed_at = 100

      job = %Job{
        retry_count: Job.default_max_retry_count() + 1,
        max_retry_count: nil,
        error_backtrace: "\n",
        retried_at: failed_at,
        error_message: "reasons"
      }

      expect(DeadSet, :add!, fn ^job, ^failed_at, :redis -> :ok end)

      state = "queue_name"
      job = %Job{retry_count: Job.default_max_retry_count(), max_retry_count: nil}
      exception = RuntimeError.exception("reasons")

      assert handle_call({:retry, job, failed_at, exception, []}, :from, state) ==
               {:reply, :ok, state}
    end

    test "retry on a job failing too many times and failing to add!" do
      failed_at = 100

      job = %Job{
        retry_count: 26,
        error_backtrace: "\n",
        retried_at: failed_at,
        error_message: "reasons"
      }

      expect(DeadSet, :add!, fn ^job, ^failed_at, :redis ->
        raise %Redix.Error{message: "fail"}
      end)

      state = "queue_name"
      job = %Job{retry_count: Job.default_max_retry_count()}
      exception = RuntimeError.exception("reasons")

      assert_raise Redix.Error, fn ->
        handle_call({:retry, job, failed_at, exception, []}, :from, state) == {:reply, :ok, state}
      end
    end

    test "retry on a job with non-enum stacktrace" do
      failed_at = 100

      stacktrace = {GenServer, :call, [:process, "1"]}

      job = %Job{
        retry_count: 1,
        failed_at: failed_at,
        error_backtrace: inspect(stacktrace),
        error_message: "reasons"
      }

      expect(RetrySet, :add!, fn ^job, ^failed_at, :redis -> "payload" end)

      state = "queue_name"
      job = %Job{retry_count: 0}
      exception = RuntimeError.exception("reasons")

      assert handle_call({:retry, job, failed_at, exception, stacktrace}, :from, state) ==
               {:reply, :ok, state}
    end

    test "retry limit stacktrace size" do
      failed_at = 100

      stacktrace = Enum.map(1..7, fn x -> {:mod, :fun, x, [file: "src/file.erl", line: x]} end)
      stacktrace_snippet = Enum.slice(stacktrace, 0..4)

      job = %Job{
        retry_count: 1,
        failed_at: failed_at,
        error_backtrace: Exception.format_stacktrace(stacktrace_snippet),
        error_message: "reasons"
      }

      expect(RetrySet, :add!, fn ^job, ^failed_at, :redis -> "payload" end)

      state = "queue_name"
      job = %Job{retry_count: 0}
      exception = RuntimeError.exception("reasons")

      assert handle_call({:retry, job, failed_at, exception, stacktrace}, :from, state) ==
               {:reply, :ok, state}
    end
  end

  describe "handle_cast/2 ack" do
    test "ack job" do
      job_item_id = "item_id"

      expect(Queue, :delete_job, fn "test_queue", ^job_item_id, :redis -> {:ok, [1, 1]} end)

      state = "test_queue"

      assert handle_cast({:ack, job_item_id}, state) == {:noreply, state}
    end
  end

  describe "handle_cast/2 malformed" do
    test "malformed job" do
      job_item_id = "item_id"

      expect(Queue, :delete_job, fn "test_queue", ^job_item_id, :redis -> {:ok, [1, 1]} end)

      state = "test_queue"

      assert handle_cast({:malformed, job_item_id}, state) == {:noreply, state}
    end
  end

  describe "handle_info/2 reenqueue_dead_jobs" do
    test "reenqueue jobs if dead nodes and jobs exist" do
      queue = "queue_name"
      jobs = [{"job_id", 123}]

      expect(Queue, :pending_node_ids, fn ^queue, _redis -> {:ok, ["dead_node", "alive_node"]} end)

      expect(Node, :dead?, fn "dead_node", _redis -> {:ok, true} end)
      expect(Node, :dead?, fn "alive_node", _redis -> {:ok, false} end)
      expect(Queue, :pending_job_ids, fn ^queue, "dead_node", 25, _redis -> {:ok, jobs} end)
      expect(Queue, :reenqueue_pending_job, fn ^queue, "job_id", 123, _redis -> :ok end)

      assert handle_info(:reenqueue_dead_jobs, queue) == {:noreply, queue}
    end

    test "reenqueue jobs if dead nodes do not exist" do
      queue = "queue_name"
      expect(Queue, :pending_node_ids, fn ^queue, _redis -> {:ok, ["alive_node"]} end)
      expect(Node, :dead?, fn "alive_node", _redis -> {:ok, false} end)
      reject(&Queue.pending_job_ids/4)
      reject(&Queue.reenqueue_pending_job/4)

      assert handle_info(:reenqueue_dead_jobs, queue) == {:noreply, queue}
    end
  end
end
