defmodule Verk.WorkersManagerTest do
  use ExUnit.Case
  import :meck
  import Verk.WorkersManager
  alias Verk.{Time, WorkersManager.State}

  defmodule Repeater do
    use GenStage

    def init(pid) do
      {:consumer, pid, subscribe_to: [Verk.EventProducer]}
    end

    def handle_events(events, _from, pid) do
      Enum.each(events, fn(event) -> send pid, event end)
      {:noreply, [], pid}
    end
  end

  setup_all do
    {:ok, _} = GenStage.start_link(Verk.EventProducer, :ok, name: Verk.EventProducer)
    :ok
  end

  setup do
    pid = self()
    new :poolboy
    {:ok, _} = GenStage.start_link(Repeater, pid)
    on_exit fn -> unload() end
    table = :ets.new(:"queue_name.workers_manager", [:named_table, read_concurrency: true])
    {:ok, monitors: table}
  end

  describe "name/1" do
    test "returns workers manager name" do
      assert name("queue_name") == :"queue_name.workers_manager"
      assert name(:queue_name) == :"queue_name.workers_manager"
    end
  end

  describe "running_jobs/1" do
    test "list running jobs with jobs to list", %{ monitors: monitors } do
      row = { self(), "job_id", "job", make_ref(), "start_time" }
      :ets.insert(monitors, row)

      assert running_jobs("queue_name") == [%{ process: self(), job: "job", started_at: "start_time" }]
    end

    test "list running jobs respecting the limit", %{ monitors: monitors } do
      row1 = { self(), "job_id", "job", make_ref(), "start_time" }
      row2 = { self(), "job_id2", "job2", make_ref(), "start_time2" }
      :ets.insert(monitors, [row2, row1])

      assert running_jobs("queue_name", 1) == [%{ process: self(), job: "job", started_at: "start_time" }]
    end

    test "list running jobs with no jobs" do
      assert running_jobs("queue_name") == []
    end
  end

  describe "inspect_worker/2" do
    test "with no matching job_id" do
      assert inspect_worker("queue_name", "job_id") == { :error, :not_found }
    end

    test "with matching job_id", %{ monitors: monitors } do
      row = { self(), "job_id", "job data", make_ref(), "start_time" }
      :ets.insert(monitors, row)

      { :ok, result } = inspect_worker("queue_name", "job_id")

      assert result[:job] == "job data"
      assert result[:process] == self()
      assert result[:started_at] == "start_time"

      expected = [:current_stacktrace, :initial_call, :reductions, :status]
      assert Enum.all?(expected, &Keyword.has_key?(result[:info], &1))
    end

    test "with matching job_id but process is gone", %{ monitors: monitors } do
      pid = :erlang.list_to_pid('<3.57.1>')
      row = { pid, "job_id", "job data", make_ref(), "start_time" }
      :ets.insert(monitors, row)

      assert inspect_worker("queue_name", "job_id") == { :error, :not_found }
    end
  end

  describe "init/1" do
    test "inits" do
      name = :workers_manager
      queue_name = "queue_name"
      queue_manager_name = "queue_manager_name"
      pool_name = "pool_name"
      pool_size = "size"
      timeout = Confex.get_env(:verk, :workers_manager_timeout)
      state = %State{ queue_name: queue_name, queue_manager_name: queue_manager_name,
                      pool_name: pool_name, pool_size: pool_size,
                      monitors: :workers_manager, timeout: timeout }
      expect(Verk.QueueStats, :reset_started, [queue_name], :ok)

      assert init([name, queue_name, queue_manager_name, pool_name, pool_size])
        == { :ok, state }

      assert_received :enqueue_inprogress
      assert validate Verk.QueueStats
    end
  end

  describe "handle_info/2 enqueue_inprogress" do
    test "enqueue_inprogress having no more jobs" do
      queue_manager_name = "queue_manager_name"
      state = %State{ queue_manager_name: queue_manager_name }

      expect(Verk.QueueManager, :enqueue_inprogress, [queue_manager_name], :ok)

      assert handle_info(:enqueue_inprogress, state) == { :noreply, state, 0 }
      refute_receive :enqueue_inprogress

      assert validate Verk.QueueManager
    end

    test "enqueue_inprogress having more jobs" do
      queue_manager_name = "queue_manager_name"
      state = %State{ queue_manager_name: queue_manager_name }

      expect(Verk.QueueManager, :enqueue_inprogress, [queue_manager_name], :more)

      assert handle_info(:enqueue_inprogress, state) == { :noreply, state }
      assert_receive :enqueue_inprogress

      assert validate Verk.QueueManager
    end
  end

  describe "handle_info/2 timeout" do
    test "timeout with no free workers", %{ monitors: monitors } do
      pool_name = "pool_name"
      new Verk.QueueManager
      state = %State{ monitors: monitors, pool_name: pool_name, pool_size: 1 }

      row = { self(), "job_id", "job", make_ref(), "start_time" }
      :ets.insert(monitors, row)

      expect(:poolboy, :status, ["pool_name"], {nil, 0, nil, nil})
      assert handle_info(:timeout, state) == { :noreply, state }

      assert validate Verk.QueueManager
    end

    test "timeout with free workers and no jobs", %{ monitors: monitors } do
      :rand.seed(:exs64, {1,2,3})
      queue_manager_name = :queue_manager_name
      timeout = 1000
      pool_name = "pool_name"
      state = %State{ monitors: monitors, pool_name: pool_name,
                      pool_size: 1, queue_manager_name: queue_manager_name, timeout: timeout }

      expect(:poolboy, :status, ["pool_name"], {nil, 1, nil, nil})
      expect(Verk.QueueManager, :dequeue, [queue_manager_name, 1], [])

      assert handle_info(:timeout, state) == { :noreply, state, 1350 }

      assert validate Verk.QueueManager
    end

    test "timeout with free workers and jobs to be done", %{ monitors: monitors } do
      :rand.seed(:exs64, {1,2,3})
      queue_manager_name = :queue_manager_name
      pool_name = :pool_name
      timeout = 1000
      worker = self()
      module = :module
      args = [:arg1, :arg2]
      job_id = "job_id"
      state = %State{ monitors: monitors, pool_name: pool_name,
                      pool_size: 1, queue_manager_name: queue_manager_name, timeout: timeout }
      job = %Verk.Job{ class: module, args: args, jid: job_id }

      expect(Verk.QueueManager, :dequeue, [queue_manager_name, 1], [:encoded_job])
      expect(Verk.Job, :decode, [:encoded_job], {:ok, job})
      expect(:poolboy, :status, [pool_name], {nil, 1, nil, nil})
      expect(:poolboy, :checkout, [pool_name, false], worker)
      expect(Verk.Worker, :perform_async, [worker, worker, job], :ok)

      assert handle_info(:timeout, state) == { :noreply, state, 1350 }
      assert match?([{^worker, ^job_id, ^job, _, _}], :ets.lookup(monitors, worker))
      assert_receive %Verk.Events.JobStarted{ job: ^job, started_at: _ }

      assert validate [Verk.QueueManager, :poolboy, Verk.Worker]
    end

    test "timeout with free workers and malformed job to be done", %{ monitors: monitors } do
      :rand.seed(:exs64, {1,2,3})
      queue_manager_name = :queue_manager_name
      pool_name = :pool_name
      timeout = 1000
      worker = self()
      module = :module
      args = [:arg1, :arg2]
      job_id = "job_id"
      state = %State{ monitors: monitors, pool_name: pool_name,
                      pool_size: 1, queue_manager_name: queue_manager_name, timeout: timeout }
      _job = %Verk.Job{ class: module, args: args, jid: job_id }

      expect(Verk.QueueManager, :dequeue, [queue_manager_name, 1], [:encoded_job])
      expect(Verk.Job, :decode, [:encoded_job], {:error, "error"})
      expect(:poolboy, :status, [pool_name], {nil, 1, nil, nil})
      expect(:poolboy, :checkout, [pool_name, false], worker)

      assert handle_info(:timeout, state) == { :noreply, state, 1350 }
      # After receiving an error from Job.decode/1 and removing from the inprogress
      # queue, we expect the job to not be present in the worker table.
      assert match?([], :ets.lookup(monitors, worker))

      assert validate [Verk.QueueManager, :poolboy]
    end
  end

  describe "handle_info/2 DOWN" do
    test "DOWN coming from dead worker with reason and stacktrace", %{ monitors: monitors } do
      ref = make_ref()
      worker = self()
      pool_name = "pool_name"
      job = "job"
      queue_manager_name = "queue_manager_name"
      reason = :reason
      exception = RuntimeError.exception(inspect(reason))

      :ets.insert(monitors, { worker, "job_id", job, ref, "start_time" })

      state = %State{ monitors: monitors, pool_name: pool_name, queue_manager_name: queue_manager_name }

      expect(Verk.Log, :fail, [job, "start_time", worker], :ok)
      expect(Verk.QueueManager, :retry, [queue_manager_name, job, exception, :stacktrace], :ok)
      expect(Verk.QueueManager, :ack, [queue_manager_name, job], :ok)
      expect(:poolboy, :checkin, [pool_name, worker], :ok)

      assert handle_info({ :DOWN, ref, :_, worker, { reason, :stacktrace } }, state) == { :noreply, state, 0 }

      assert :ets.lookup(monitors, worker) == []
      assert_receive %Verk.Events.JobFailed{ job: ^job, failed_at: _,
                                             stacktrace: :stacktrace,
                                             exception: ^exception }

      assert validate [:poolboy, Verk.Log, Verk.QueueManager]
    end

    test "DOWN coming from dead worker with reason and no stacktrace", %{ monitors: monitors } do
      ref = make_ref()
      worker = self()
      pool_name = "pool_name"
      job = "job"
      queue_manager_name = "queue_manager_name"
      reason = :reason
      exception = RuntimeError.exception(inspect(reason))

      :ets.insert(monitors, { worker, "job_id", job, ref, "start_time" })

      state = %State{ monitors: monitors, pool_name: pool_name, queue_manager_name: queue_manager_name }

      expect(Verk.Log, :fail, [job, "start_time", worker], :ok)
      expect(Verk.QueueManager, :retry, [queue_manager_name, job, exception, []], :ok)
      expect(Verk.QueueManager, :ack, [queue_manager_name, job], :ok)
      expect(:poolboy, :checkin, [pool_name, worker], :ok)

      assert handle_info({ :DOWN, ref, :_, worker, reason }, state) == { :noreply, state, 0 }

      assert :ets.lookup(monitors, worker) == []
      assert_receive %Verk.Events.JobFailed{ job: ^job, failed_at: _,
                                             stacktrace: [],
                                             exception: ^exception }

      assert validate [:poolboy, Verk.Log, Verk.QueueManager]
    end

    test "DOWN coming from dead worker with normal reason", %{ monitors: monitors } do
      queue_manager_name = "queue_manager_name"
      pool_name = "pool_name"
      state = %State{ monitors: monitors, pool_name: pool_name, queue_manager_name: queue_manager_name }
      worker = self()
      now = DateTime.utc_now
      job = %Verk.Job{}
      finished_job = %Verk.Job{ finished_at: now}
      job_id = "job_id"
      ref = make_ref()
      start_time = Time.now

      expect(Verk.QueueManager, :ack, [queue_manager_name, job], :ok)
      expect(:poolboy, :checkin, [pool_name, worker], :ok)
      expect(Time, :now, 0, now)

      :ets.insert(monitors, { worker, job_id, job, ref, start_time })
      assert handle_info({ :DOWN, ref, :_, worker, :normal }, state) == { :noreply, state, 0 }

      assert :ets.lookup(state.monitors, worker) == []
      assert_receive %Verk.Events.JobFinished{ job: ^finished_job, started_at: ^start_time, finished_at: ^now }

      assert validate [Verk.QueueManager, :poolboy, Time]
    end

    test "DOWN coming from dead worker with failed reason", %{ monitors: monitors } do
      ref = make_ref()
      worker = self()
      pool_name = "pool_name"
      job = "job"
      job_id = "job_id"
      queue_manager_name = "queue_manager_name"
      exception = RuntimeError.exception(":failed")

      :ets.insert(monitors, { worker, job_id, job, ref, "start_time" })

      state = %State{ monitors: monitors, pool_name: pool_name, queue_manager_name: queue_manager_name }

      expect(Verk.Log, :fail, [job, "start_time", worker], :ok)
      expect(Verk.QueueManager, :retry, [queue_manager_name, job, exception, []], :ok)
      expect(Verk.QueueManager, :ack, [queue_manager_name, job], :ok)
      expect(:poolboy, :checkin, [pool_name, worker], :ok)

      assert handle_info({ :DOWN, ref, :_, worker, :failed }, state) == { :noreply, state, 0 }

      assert :ets.lookup(monitors, worker) == []
      assert_receive %Verk.Events.JobFailed{ job: ^job, failed_at: _,
                                             stacktrace: [],
                                             exception: ^exception }

      assert validate [Verk.Log, Verk.QueueManager, :poolboy]
    end
  end

  describe "handle_cast/2" do
    test "done having the worker registered", %{ monitors: monitors } do
      queue_manager_name = "queue_manager_name"
      pool_name = "pool_name"
      state = %State{ monitors: monitors, pool_name: pool_name, queue_manager_name: queue_manager_name }
      worker = self()
      now = DateTime.utc_now
      job = %Verk.Job{}
      finished_job = %Verk.Job{ finished_at: now}
      job_id = "job_id"

      expect(Verk.QueueManager, :ack, [queue_manager_name, job], :ok)
      expect(:poolboy, :checkin, [pool_name, worker], :ok)
      expect(Time, :now, 0, now)

      :ets.insert(monitors, { worker, job_id, job, make_ref(), Time.now })
      assert handle_cast({ :done, worker, job_id }, state) == { :noreply, state, 0 }

      assert :ets.lookup(state.monitors, worker) == []
      assert_receive %Verk.Events.JobFinished{ job: ^finished_job, finished_at: ^now }

      assert validate [:poolboy, Verk.QueueManager, Time]
    end

    test "cast failed coming from worker", %{ monitors: monitors } do
      ref = make_ref()
      worker = self()
      pool_name = "pool_name"
      job = "job"
      job_id = "job_id"
      queue_manager_name = "queue_manager_name"
      exception = RuntimeError.exception("reasons")

      :ets.insert(monitors, { worker, job_id, job, ref, "start_time" })

      state = %State{ monitors: monitors, pool_name: pool_name, queue_manager_name: queue_manager_name }

      expect(Verk.Log, :fail, [job, "start_time", worker], :ok)
      expect(Verk.QueueManager, :retry, [queue_manager_name, job, exception, :stacktrace], :ok)
      expect(Verk.QueueManager, :ack, [queue_manager_name, job], :ok)
      expect(:poolboy, :checkin, [pool_name, worker], :ok)

      assert handle_cast({ :failed, worker, job_id, exception, :stacktrace }, state) == { :noreply, state, 0 }

      assert :ets.lookup(monitors, worker) == []
      assert_receive %Verk.Events.JobFailed{ job: ^job, failed_at: _,
                                             stacktrace: :stacktrace,
                                             exception: ^exception }

      assert validate [Verk.Log, Verk.QueueManager]
    end
  end
end
