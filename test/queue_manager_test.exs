defmodule Verk.QueueManagerTest do
  use ExUnit.Case
  import Verk.QueueManager
  import :meck
  alias Verk.{QueueManager.State, Job, RetrySet, DeadSet}

  @mrpop_script Verk.Scripts.sha("mrpop_lpush_src_dest")

  setup do
    on_exit fn -> unload() end
    :ok
  end

  describe "name/1" do
    test "returns queue manager name" do
      assert name("queue_name") == :"queue_name.queue_manager"
      assert name(:queue_name) == :"queue_name.queue_manager"
    end
  end

  describe "init/1" do
    test "sets up redis connection" do
      redis_url = Confex.get_env(:verk, :redis_url)
      node_id = Confex.get_env(:verk, :node_id, "1")

      expect(Redix, :start_link, [redis_url], {:ok, :redis })
      expect(Verk.Scripts, :load, [:redis], :ok)

      assert init(["queue_name"]) == { :ok, %State{ node_id: node_id, queue_name: "queue_name", redis: :redis } }

      assert validate [Redix, Verk.Scripts]
    end
  end

  describe "handle_call/3 enqueue_inprogress" do
    @script Verk.Scripts.sha("lpop_rpush_src_dest")

    test "with more jobs to enqueue" do
      expect(Redix, :command, [:redis, ["EVALSHA", @script, 2, "inprogress:test_queue:test_node", "queue:test_queue", 1000]], { :ok, [42, 2] })

      state = %State{ queue_name: "test_queue", redis: :redis, node_id: "test_node" }

      assert handle_call(:enqueue_inprogress, :from, state) == { :reply, :more, state }

      assert validate Redix
    end

    test "with no more jobs to enqueue" do
      expect(Redix, :command, [:redis, ["EVALSHA", @script, 2, "inprogress:test_queue:test_node", "queue:test_queue", 1000]], { :ok, [0, 0] })

      state = %State{ queue_name: "test_queue", redis: :redis, node_id: "test_node" }

      assert handle_call(:enqueue_inprogress, :from, state) == { :reply, :ok, state }

      assert validate Redix
    end

    test "when redis fails" do
      expect(Redix, :command, [:redis, ["EVALSHA", @script, 2, "inprogress:test_queue:test_node", "queue:test_queue", 1000]], { :error, :reason })

      state = %State{ queue_name: "test_queue", redis: :redis, node_id: "test_node" }

      assert handle_call(:enqueue_inprogress, :from, state) == { :stop, :redis_failed, state }

      assert validate Redix
    end
  end

  describe "handle_call/3 dequeue" do
    test "dequeue with an empty queue" do
      expect(Redix, :command, [:redis, ["EVALSHA", @mrpop_script, 2, "queue:test_queue", "inprogress:test_queue:test_node", 3]], { :ok, [] })

      state = %State{ queue_name: "test_queue", redis: :redis, node_id: "test_node" }

      assert handle_call({ :dequeue, 3 }, :from, state) == { :reply, [], state }

      assert validate Redix
    end

    test "dequeue with a non empty queue" do
      expect(Redix, :command, [:redis, ["EVALSHA", @mrpop_script, 2, "queue:test_queue", "inprogress:test_queue:test_node", 3]], { :ok, ["job"] })

      state = %State{ queue_name: "test_queue", redis: :redis, node_id: "test_node" }

      assert handle_call({ :dequeue, 3 }, :from, state) == { :reply, ["job"], state }
      assert validate [Redix]
    end

    test "dequeue with a non empty queue and more than max_jobs" do
      expect(Redix, :command, [:redis, ["EVALSHA", @mrpop_script, 2, "queue:test_queue", "inprogress:test_queue:test_node", 100]], { :ok, ["job"] })

      state = %State{ queue_name: "test_queue", redis: :redis, node_id: "test_node" }

      assert handle_call({ :dequeue, 500 }, :from, state) == { :reply, ["job"], state }
      assert validate [Redix]
    end

    test "dequeue and redis failed" do
      expect(Redix, :command, 2, { :error, :reason })

      state = %State{ queue_name: "test_queue", redis: :redis }

      assert handle_call({ :dequeue, 3 }, :from, state) == { :reply, :redis_failed, state }

      assert validate Redix
    end

    test "dequeue and redis failed to evaluate the script" do
      expect(Redix, :command, 2, { :error, %Redix.Error{message: "a message" } })

      state = %State{ queue_name: "test_queue", redis: :redis }

      assert handle_call({ :dequeue, 3 }, :from, state) == { :stop, :redis_failed, :redis_failed, state }

      assert validate Redix
    end

    test "dequeue and timeout" do
      pid = spawn_link(fn -> :timer.sleep(5000) end)
      assert dequeue(pid, 1, 1) == :timeout
    end
  end

  describe "handle_call/3 retry" do
    test "retry on a job with retry count 0" do
      failed_at = 100

      job = %Job{ retry_count: 1, failed_at: failed_at, error_backtrace: "\n", error_message: "reasons" }
      expect(RetrySet, :add!, [job, failed_at, :redis], "payload")

      state = %State{ redis: :redis }
      job = %Job{ retry_count: 0 }
      exception = RuntimeError.exception("reasons")

      assert handle_call({ :retry, job, failed_at, exception, [] }, :from, state) == { :reply, :ok, state }

      assert validate RetrySet
    end

    test "retry on a job with no retry_count" do
      failed_at = 100

      job = %Job{ retry_count: 1, failed_at: failed_at, error_backtrace: "\n", error_message: "reasons" }
      expect(RetrySet, :add!, [job, failed_at, :redis], :ok)

      state = %State{ redis: :redis }
      job = %Job{ retry_count: nil }
      exception = RuntimeError.exception("reasons")

      assert handle_call({ :retry, job, failed_at, exception, [] }, :from, state) == { :reply, :ok, state }

      assert validate RetrySet
    end

    test "retry on a job with retry_count less than max_retry_count" do
      failed_at = 100

      job = %Job{ retry_count: 2, retried_at: failed_at, error_backtrace: "\n",
        error_message: "reasons", max_retry_count: 4}
      expect(RetrySet, :add!, [job, failed_at, :redis], :ok)

      state = %State{ redis: :redis }
      job = %Job{ retry_count: 1, max_retry_count: 4 }
      exception = RuntimeError.exception("reasons")

      assert handle_call({ :retry, job, failed_at, exception, [] }, :from, state) == { :reply, :ok, state }

      assert validate RetrySet
    end

    test "retry on a job failing to add to retry set" do
      failed_at = 100

      job = %Job{ retry_count: 1, failed_at: failed_at, error_backtrace: "\n", error_message: "reasons" }
      expect(RetrySet, :add!, fn ^job, ^failed_at, :redis -> exception(:error, %Redix.Error{message: "fail"}) end)

      state = %State{ redis: :redis }
      job = %Job{ retry_count: 0 }
      exception = RuntimeError.exception("reasons")

      assert_raise Redix.Error, fn ->
        handle_call({ :retry, job, failed_at, exception, [] }, :from, state) == { :reply, :ok, state }
      end

      assert validate RetrySet
    end

    test "retry on a job with nil max_retry_count and retry_count greater than default" do
      failed_at = 100
      job = %Job{ retry_count: Job.default_max_retry_count() + 1, max_retry_count: nil,
        error_backtrace: "\n", retried_at: failed_at, error_message: "reasons" }
      expect(DeadSet, :add!, [job, failed_at, :redis], :ok)

      state = %State{ redis: :redis }
      job = %Job{ retry_count: Job.default_max_retry_count(), max_retry_count: nil}
      exception = RuntimeError.exception("reasons")

      assert handle_call({ :retry, job, failed_at, exception, [] }, :from, state) == { :reply, :ok, state }

      assert validate DeadSet
    end

    test "retry on a job failing too many times and failing to add!" do
      failed_at = 100
      job = %Job{ retry_count: 26, error_backtrace: "\n", retried_at: failed_at, error_message: "reasons" }
      expect(DeadSet, :add!, fn ^job, ^failed_at, :redis -> exception(:error, %Redix.Error{message: "fail"}) end)

      state = %State{ redis: :redis }
      job = %Job{ retry_count: Job.default_max_retry_count() }
      exception = RuntimeError.exception("reasons")

      assert_raise Redix.Error, fn ->
        handle_call({ :retry, job, failed_at, exception, [] }, :from, state) == { :reply, :ok, state }
      end

      assert validate DeadSet
    end

    test "retry on a job with non-enum stacktrace" do
      failed_at = 100

      stacktrace = {GenServer, :call, [:process, "1"]}
      job = %Job{ retry_count: 1, failed_at: failed_at, error_backtrace: inspect(stacktrace), error_message: "reasons" }
      expect(RetrySet, :add!, [job, failed_at, :redis], "payload")

      state = %State{ redis: :redis }
      job = %Job{ retry_count: 0 }
      exception = RuntimeError.exception("reasons")

      assert handle_call({ :retry, job, failed_at, exception, stacktrace}, :from, state) == { :reply, :ok, state }

      assert validate RetrySet
    end

    test "retry limit stacktrace size" do
      failed_at = 100

      stacktrace = Enum.map(1..7, fn(x) -> {:mod, :fun, x, [file: "src/file.erl", line: x]} end)
      stacktrace_snippet = Enum.slice(stacktrace, 0..4)

      job = %Job{ retry_count: 1, failed_at: failed_at, error_backtrace: Exception.format_stacktrace(stacktrace_snippet), error_message: "reasons" }
      expect(RetrySet, :add!, [job, failed_at, :redis], "payload")

      state = %State{ redis: :redis }
      job = %Job{ retry_count: 0 }
      exception = RuntimeError.exception("reasons")

      assert handle_call({ :retry, job, failed_at, exception, stacktrace}, :from, state) == { :reply, :ok, state }

      assert validate RetrySet
    end
  end

  describe "handle_cast/2 ack" do
    test "ack job" do
      encoded_job = "encoded_job"
      job = %Verk.Job{ original_json: encoded_job }

      expect(Redix, :command, [:redis, ["LREM", "inprogress:test_queue:test_node", "-1", encoded_job]], { :ok, 1 })

      state = %State{ queue_name: "test_queue", redis: :redis, node_id: "test_node" }

      assert handle_cast({ :ack, job}, state) == { :noreply, state }

      assert validate Redix
    end
  end

  describe "handle_cast/2 malformed" do
    test "malformed job" do
      job = ""

      expect(Redix, :command, [:redis, ["LREM", "inprogress:test_queue:test_node", "-1", job]], { :ok, 1 })

      state = %State{ queue_name: "test_queue", redis: :redis, node_id: "test_node" }

      assert handle_cast({ :malformed, job}, state) == { :noreply, state }

      assert validate Redix
    end
  end
end
