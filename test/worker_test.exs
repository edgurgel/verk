defmodule TestWorker do
  def perform(pid) do
    send pid, :perform_executed
  end
end

defmodule Verk.WorkerTest do
  use ExUnit.Case
  import Verk.Worker

  test "cast perform runs the specified module with the args" do
    worker = self
    assert handle_cast({ :perform, "TestWorker", [worker], "job_id", worker }, :state) == { :stop, :normal, :state }

    assert_receive :perform_executed
    assert_receive {:"$gen_cast", {:done, ^worker, "job_id"}}
  end

  test "perform_async cast message to worker to perform the job" do
    worker = self
    assert perform_async(worker, :manager, :module, :args, :job_id)

    assert_receive {:"$gen_cast", {:perform, :module, :args, :job_id, :manager}}
  end
end
