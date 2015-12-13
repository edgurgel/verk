defmodule Verk.QueueManagerTest do
  use ExUnit.Case
  import Verk.QueueManager
  import :meck
  alias Verk.QueueManager.State
  alias Verk.Job

  setup do
    on_exit fn -> unload end
    :ok
  end

  test "name returns queue manager name" do
    assert name("queue_name") == :"queue_name.queue_manager"
    assert name(:queue_name) == :"queue_name.queue_manager"
  end

  test "init sets up redis connection" do
    Application.put_env(:verk, :redis_url, "redis_url")
    Application.put_env(:verk, :node_id, "test_node")

    expect(Redix, :start_link, ["redis_url"], {:ok, :redis })

    assert init(["queue_name"]) == { :ok, %State{ node_id: "test_node", queue_name: "queue_name", redis: :redis } }

    assert validate Redix
  end

  test "call enqueue_inprogress" do
    script = Verk.Scripts.sha("lpop_rpush_src_dest")
    expect(Redix, :command, [:redis, ["EVALSHA", script, 2, "inprogress:test_queue:test_node", "queue:test_queue"]], { :ok, 42 })

    state = %State{ queue_name: "test_queue", redis: :redis, node_id: "test_node" }

    assert handle_call(:enqueue_inprogress, :from, state) == { :reply, :ok, state }

    assert validate Redix
  end

  test "call dequeue with an empty queue" do
    script = Verk.Scripts.sha("mrpop_lpush_src_dest")
    expect(Redix, :command, [:redis, ["EVALSHA", script, 2, "queue:test_queue", "inprogress:test_queue:test_node", 3]], { :ok, [] })

    state = %State{ queue_name: "test_queue", redis: :redis, node_id: "test_node" }

    assert handle_call({ :dequeue, 3 }, :from, state) == { :reply, [], state }

    assert validate Redix
  end

  test "call dequeue with a non empty queue" do
    script = Verk.Scripts.sha("mrpop_lpush_src_dest")
    expect(Redix, :command, [:redis, ["EVALSHA", script, 2, "queue:test_queue", "inprogress:test_queue:test_node", 3]], { :ok, ["job"] })
    expect(Verk.Job, :decode!, ["job"], :decoded_job)

    state = %State{ queue_name: "test_queue", redis: :redis, node_id: "test_node" }

    assert handle_call({ :dequeue, 3 }, :from, state) == { :reply, [:decoded_job], state }
    assert validate [Redix, Verk.Job]
  end

  test "call dequeue and redis failed" do
    expect(Redix, :command, 2, { :error, :reason })

    state = %State{ queue_name: "test_queue", redis: :redis }

    assert handle_call({ :dequeue, 3 }, :from, state) == { :reply, :redis_failed, state }

    assert validate Redix
  end

  test "call dequeue and timeout" do
    assert dequeue(self, 1) == :timeout
  end

  test "cast ack job" do
    encoded_job = "encoded_job"
    job = %Verk.Job{ original_json: encoded_job }

    expect(Redix, :command, [:redis, ["LREM", "inprogress:test_queue:test_node", "-1", encoded_job]], { :ok, 1 })

    state = %State{ queue_name: "test_queue", redis: :redis, node_id: "test_node" }

    assert handle_cast({ :ack, job}, state) == { :noreply, state }

    assert validate Redix
  end

  test "call retry on a job" do
    failed_at = 100

    expect(Redix, :command, [:redis, ["ZADD", "retry", :_, "payload"]], { :ok, 1 })
    expect(Poison, :encode!, [%Job{ retry_count: 1, failed_at: failed_at, error_message: ":reason" }], "payload")

    state = %State{ redis: :redis }
    job = %Job{ retry_count: 0 }

    assert handle_call({ :retry, job, failed_at, :reason }, :from, state) == { :reply, :ok, state }

    assert validate [Redix, Poison]
  end

  test "call retry on a job with no retry_count" do
    failed_at = 100

    expect(Redix, :command, [:redis, ["ZADD", "retry", :_, "payload"]], { :ok, 1 })
    expect(Poison, :encode!, [%Job{ retry_count: 1, failed_at: failed_at, error_message: ":reason" }], "payload")

    state = %State{ redis: :redis }
    job = %Job{}

    assert handle_call({ :retry, job, failed_at, :reason }, :from, state) == { :reply, :ok, state }

    assert validate [Redix, Poison]
  end

  test "call retry on a job failing to add to retry set" do
    failed_at = 100

    expect(Redix, :command, [:redis, ["ZADD", "retry", :_, "payload"]], { :error, :reason })
    expect(Poison, :encode!, [%Job{ retry_count: 1, failed_at: failed_at, error_message: ":reason" }], "payload")

    state = %State{ redis: :redis }
    job = %Job{ retry_count: 0 }

    assert handle_call({ :retry, job, failed_at, :reason }, :from, state) == { :reply, :ok, state }

    assert validate [Redix, Poison]
  end
end
