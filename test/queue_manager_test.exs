defmodule Verk.QueueManagerTest do
  use ExUnit.Case
  import Verk.QueueManager
  import :meck
  alias Verk.QueueManager.State
  alias Verk.Job
  alias Verk.RetrySet
  alias Verk.DeadSet

  @mrpop_script Verk.Scripts.sha("mrpop_lpush_src_dest")

  setup do
    on_exit fn -> unload end
    :ok
  end

  test "name returns queue manager name" do
    assert name("queue_name") == :"queue_name.queue_manager"
    assert name(:queue_name) == :"queue_name.queue_manager"
  end

  test "init sets up redis connection" do
    { :ok, redis_url } = Application.fetch_env(:verk, :redis_url)
    node_id = Application.get_env(:verk, :node_id, "1")

    expect(Redix, :start_link, [redis_url], {:ok, :redis })
    expect(Verk.Scripts, :load, [:redis], :ok)

    assert init(["queue_name"]) == { :ok, %State{ node_id: node_id, queue_name: "queue_name", redis: :redis } }

    assert validate [Redix, Verk.Scripts]
  end

  test "call enqueue_inprogress" do
    script = Verk.Scripts.sha("lpop_rpush_src_dest")
    expect(Redix, :command, [:redis, ["EVALSHA", script, 2, "inprogress:test_queue:test_node", "queue:test_queue"]], { :ok, 42 })

    state = %State{ queue_name: "test_queue", redis: :redis, node_id: "test_node" }

    assert handle_call(:enqueue_inprogress, :from, state) == { :reply, :ok, state }

    assert validate Redix
  end

  test "call enqueue_inprogress and redis failed" do
    script = Verk.Scripts.sha("lpop_rpush_src_dest")
    expect(Redix, :command, [:redis, ["EVALSHA", script, 2, "inprogress:test_queue:test_node", "queue:test_queue"]], { :error, :reason })

    state = %State{ queue_name: "test_queue", redis: :redis, node_id: "test_node" }

    assert handle_call(:enqueue_inprogress, :from, state) == { :stop, :redis_failed, state }

    assert validate Redix
  end

  test "call dequeue with an empty queue" do
    expect(Redix, :command, [:redis, ["EVALSHA", @mrpop_script, 2, "queue:test_queue", "inprogress:test_queue:test_node", 3]], { :ok, [] })

    state = %State{ queue_name: "test_queue", redis: :redis, node_id: "test_node" }

    assert handle_call({ :dequeue, 3 }, :from, state) == { :reply, [], state }

    assert validate Redix
  end

  test "call dequeue with a non empty queue" do
    expect(Redix, :command, [:redis, ["EVALSHA", @mrpop_script, 2, "queue:test_queue", "inprogress:test_queue:test_node", 3]], { :ok, ["job"] })

    state = %State{ queue_name: "test_queue", redis: :redis, node_id: "test_node" }

    assert handle_call({ :dequeue, 3 }, :from, state) == { :reply, ["job"], state }
    assert validate [Redix]
  end

  test "call dequeue with a non empty queue and more than max_jobs" do
    expect(Redix, :command, [:redis, ["EVALSHA", @mrpop_script, 2, "queue:test_queue", "inprogress:test_queue:test_node", 100]], { :ok, ["job"] })

    state = %State{ queue_name: "test_queue", redis: :redis, node_id: "test_node" }

    assert handle_call({ :dequeue, 500 }, :from, state) == { :reply, ["job"], state }
    assert validate [Redix]
  end

  test "call dequeue and redis failed" do
    expect(Redix, :command, 2, { :error, :reason })

    state = %State{ queue_name: "test_queue", redis: :redis }

    assert handle_call({ :dequeue, 3 }, :from, state) == { :reply, :redis_failed, state }

    assert validate Redix
  end

  test "call dequeue and redis failed to evalue the script" do
    expect(Redix, :command, 2, { :error, %Redix.Error{message: "a message" } })

    state = %State{ queue_name: "test_queue", redis: :redis }

    assert handle_call({ :dequeue, 3 }, :from, state) == { :stop, :redis_failed, :redis_failed, state }

    assert validate Redix
  end

  test "call dequeue and timeout" do
    assert dequeue(self, 1, 1) == :timeout
  end

  test "cast ack job" do
    encoded_job = "encoded_job"
    job = %Verk.Job{ original_json: encoded_job }

    expect(Redix, :command, [:redis, ["LREM", "inprogress:test_queue:test_node", "-1", encoded_job]], { :ok, 1 })

    state = %State{ queue_name: "test_queue", redis: :redis, node_id: "test_node" }

    assert handle_cast({ :ack, job}, state) == { :noreply, state }

    assert validate Redix
  end

  test "call retry on a job with retry count 0" do
    failed_at = 100

    job = %Job{ retry_count: 1, failed_at: failed_at, error_backtrace: "\n", error_message: "reasons" }
    expect(RetrySet, :add, [job, failed_at, :redis], "payload")

    state = %State{ redis: :redis }
    job = %Job{ retry_count: 0 }
    exception = RuntimeError.exception("reasons")

    assert handle_call({ :retry, job, failed_at, exception, [] }, :from, state) == { :reply, :ok, state }

    assert validate RetrySet
  end

  test "call retry on a job with no retry_count" do
    failed_at = 100

    job = %Job{ retry_count: 1, failed_at: failed_at, error_backtrace: "\n", error_message: "reasons" }
    expect(RetrySet, :add, [job, failed_at, :redis], :ok)

    state = %State{ redis: :redis }
    job = %Job{ retry_count: nil }
    exception = RuntimeError.exception("reasons")

    assert handle_call({ :retry, job, failed_at, exception, [] }, :from, state) == { :reply, :ok, state }

    assert validate RetrySet
  end

  test "call retry on a job with retry_count greater than 0" do
    failed_at = 100

    job = %Job{ retry_count: 2, retried_at: failed_at, error_backtrace: "\n", error_message: "reasons" }
    expect(RetrySet, :add, [job, failed_at, :redis], :ok)

    state = %State{ redis: :redis }
    job = %Job{ retry_count: 1 }
    exception = RuntimeError.exception("reasons")

    assert handle_call({ :retry, job, failed_at, exception, [] }, :from, state) == { :reply, :ok, state }

    assert validate RetrySet
  end

  test "call retry on a job failing to add to retry set" do
    failed_at = 100

    job = %Job{ retry_count: 1, failed_at: failed_at, error_backtrace: "\n", error_message: "reasons" }
    expect(RetrySet, :add, [job, failed_at, :redis], :error)

    state = %State{ redis: :redis }
    job = %Job{ retry_count: 0 }
    exception = RuntimeError.exception("reasons")

    assert handle_call({ :retry, job, failed_at, exception, [] }, :from, state) == { :reply, :ok, state }

    assert validate RetrySet
  end

  test "call retry on a job failing too many times" do
    failed_at = 100
    job = %Job{ retry_count: 26, error_backtrace: "\n", retried_at: failed_at, error_message: "reasons" }
    expect(DeadSet, :add, [job, failed_at, :redis], :ok)

    state = %State{ redis: :redis }
    job = %Job{ retry_count: 25 }
    exception = RuntimeError.exception("reasons")

    assert handle_call({ :retry, job, failed_at, exception, [] }, :from, state) == { :reply, :ok, state }

    assert validate DeadSet
  end
end
