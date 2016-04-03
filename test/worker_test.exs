defmodule TestWorker do
  def perform(pid) do
    send pid, :perform_executed
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
    assert handle_cast({ :perform, "TestWorker", [worker], "job_id", worker }, :state) == { :stop, :normal, :state }

    assert_receive :perform_executed
    assert_receive {:"$gen_cast", {:done, ^worker, "job_id"}}
  end

  test "cast perform runs the specified module with the args failing" do
    worker = self
    exception = ArgumentError.exception("invalid argument arg1")
    assert handle_cast({ :perform, "FailWorker", ["arg1"], "job_id", worker }, :state) == { :stop, :failed, :state }

    assert_receive { :"$gen_cast", { :failed, ^worker, "job_id", ^exception, _ } }
  end

  test "perform_async cast message to worker to perform the job" do
    worker = self
    assert perform_async(worker, :manager, :module, :args, :job_id)

    assert_receive {:"$gen_cast", {:perform, :module, :args, :job_id, :manager}}
  end
end
