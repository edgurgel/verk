defmodule Verk.WorkersManagerTest do
  use ExUnit.Case
  import :meck
  import Verk.WorkersManager
  alias Verk.WorkersManager.State

  setup do
    on_exit fn -> unload end
    table = :ets.new(:"queue_name.workers_manager", [:named_table, read_concurrency: true])
    { :ok, monitors: table }
  end

  test "name returns workers manager name" do
    assert name("queue_name") == :"queue_name.workers_manager"
    assert name(:queue_name) == :"queue_name.workers_manager"
  end

  test "list running jobs", %{ monitors: monitors } do
    row = { self, "job_id", "job", make_ref, "start_time" }
    :ets.insert(monitors, row)

    assert running_jobs("queue_name") == [row]
  end

  test "init" do
    name = :workers_manager
    queue_name = "queue_name"
    queue_manager_name = "queue_manager_name"
    pool_name = "pool_name"
    pool_size = "size"
    state = %State{ queue_name: queue_name, queue_manager_name: queue_manager_name,
                    pool_name: pool_name, pool_size: pool_size,
                    monitors: :workers_manager }

    assert init([name, queue_name, queue_manager_name, pool_name, pool_size])
      == { :ok, state }

    assert_received :enqueue_inprogress
  end

  test "handle info enqueue_inprogress" do
    queue_manager_name = "queue_manager_name"
    state = %State{ queue_manager_name: queue_manager_name }

    expect(Verk.QueueManager, :enqueue_inprogress, [queue_manager_name], :ok)

    assert handle_info(:enqueue_inprogress, state) == { :noreply, state, 0 }

    assert validate Verk.QueueManager
  end

  test "handle info timeout with no free workers", %{ monitors: monitors } do
    new Verk.QueueManager
    state = %State{ monitors: monitors, pool_name: "pool_name", pool_size: 1 }

    row = { self, "job_id", "job", make_ref, "start_time" }
    :ets.insert(monitors, row)

    assert handle_info(:timeout, state) == { :noreply, state }

    assert validate Verk.QueueManager
  end

  test "handle info timeout with free workers and no jobs", %{ monitors: monitors } do
    queue_manager_name = :queue_manager_name
    state = %State{ monitors: monitors, pool_name: "pool_name",
                    pool_size: 1, queue_manager_name: queue_manager_name }

    expect(Verk.QueueManager, :dequeue, [queue_manager_name, 1], [])

    assert handle_info(:timeout, state) == { :noreply, state, 1000 }

    assert validate Verk.QueueManager
  end

  test "handle info timeout with free workers and jobs to be done", %{ monitors: monitors } do
    queue_manager_name = :queue_manager_name
    pool_name = :pool_name
    worker = self
    module = :module
    args = [:arg1, :arg2]
    job_id = "job_id"
    state = %State{ monitors: monitors, pool_name: pool_name,
                    pool_size: 1, queue_manager_name: queue_manager_name }
    job = %Verk.Job{ class: module, args: args, jid: job_id }

    expect(Verk.QueueManager, :dequeue, [queue_manager_name, 1], [job])
    expect(:poolboy, :checkout, [pool_name, false], worker)
    expect(Verk.Worker, :perform_async, [worker, worker, module, args, job_id], :ok)

    assert handle_info(:timeout, state) == { :noreply, state, 1000 }
    assert match?([{^worker, ^job_id, ^job, _, _}], :ets.lookup(monitors, worker))

    assert validate [Verk.QueueManager, :poolboy, Verk.Worker]
  end

  test "cast done having the worker registered", %{ monitors: monitors } do
    queue_manager_name = "queue_manager_name"
    pool_name = "pool_name"
    state = %State{ monitors: monitors, pool_name: pool_name, queue_manager_name: queue_manager_name }
    worker = self
    job = "job"
    job_id = "job_id"

    expect(:poolboy, :checkin, [pool_name, worker], :ok)
    expect(Verk.QueueManager, :ack, [queue_manager_name, job], :ok)

    :ets.insert(monitors, { worker, job_id, job, make_ref })
    assert handle_cast({ :done, worker, job_id }, state) == { :noreply, state, 0 }

    assert validate [:poolboy, Verk.QueueManager]
  end

  test "handle info DOWN coming from dead worker", %{ monitors: monitors } do
    ref = make_ref
    worker = self
    pool_name = "pool_name"
    job = "job"
    queue_manager_name = "queue_manager_name"

    :ets.insert(monitors, { worker, "job_id", job, ref, "start_time" })

    state = %State{ monitors: monitors, pool_name: pool_name, queue_manager_name: queue_manager_name }

    expect(:poolboy, :checkin, [pool_name, worker], true)
    expect(Verk.Log, :fail, [job, "start_time", worker], :ok)
    expect(Verk.QueueManager, :retry, [queue_manager_name, job, :reason], :ok)
    expect(Verk.QueueManager, :ack, [queue_manager_name, job], :ok)

    assert handle_info({ :DOWN, ref, :_, worker, :reason }, state) == { :noreply, state, 0 }

    assert :ets.lookup(monitors, worker) == []

    assert validate [:poolboy, Verk.Log, Verk.QueueManager]
  end
end
