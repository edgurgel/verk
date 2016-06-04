defmodule TestWorker do
  def perform(pid) do
    send pid, :perform_executed
  end
end

defmodule TestWorkerCurrentJob do
  def perform(pid) do
    send pid, Verk.Worker.current_job
  end
end

defmodule FailWorker do
  def perform(argument) do
     raise ArgumentError, message: "invalid argument #{argument}"
  end
end

defmodule Verk.WorkerTest do
  use ExUnit.Case
  import Verk.Worker

  test "cast perform runs the specified module with the args succeding" do
    worker = self
    job = %Verk.Job{jid: "job_id", class: "TestWorker", args: [worker]}
    assert handle_cast({ :perform, job, worker }, :state) == { :stop, :normal, :state }

    assert_receive :perform_executed
    assert_receive {:"$gen_cast", {:done, ^worker, "job_id"}}
  end

  test "cast perform runs the specified atom module with the args succeding" do
    worker = self
    job = %Verk.Job{jid: "job_id", class: TestWorker, args: [worker]}
    assert handle_cast({ :perform, job, worker }, :state) == { :stop, :normal, :state }

    assert_receive :perform_executed
    assert_receive {:"$gen_cast", {:done, ^worker, "job_id"}}
  end

  test "cast perform runs the no exist module with the args succeding" do
    worker = self
    job = %Verk.Job{jid: "job_id", class: TestWorkerNoExist, args: [worker]}
    assert handle_cast({ :perform, job, worker }, :state) == { :stop, :failed, :state }

    exception = %UndefinedFunctionError{arity: 1, function: :perform, module: TestWorkerNoExist, reason: nil}
    assert_receive { :"$gen_cast", { :failed, ^worker, "job_id", ^exception, _ } }
  end

  test "cast perform runs the specified module with the args failing" do
    worker = self
    job = %Verk.Job{jid: "job_id", class: "FailWorker", args: ["arg1"]}
    exception = ArgumentError.exception("invalid argument arg1")
    assert handle_cast({ :perform, job, worker }, :state) == { :stop, :failed, :state }

    assert_receive { :"$gen_cast", { :failed, ^worker, "job_id", ^exception, _ } }
  end

  test "perform_async cast message to worker to perform the job" do
    worker = self
    assert perform_async(worker, :manager, :job)

    assert_receive {:"$gen_cast", {:perform, :job, :manager}}
  end

  test "cast perform accessing the job" do
    worker = self
    job = %Verk.Job{jid: "job_id", class: "TestWorkerCurrentJob", args: [worker]}
    assert handle_cast({ :perform, job, worker }, :state) == { :stop, :normal, :state }

    assert_receive job
    assert_receive {:"$gen_cast", {:done, ^worker, "job_id"}}
  end

  test "current_job" do
    :erlang.put(:verk_current_job, :job)

    assert current_job == :job
  end
end
