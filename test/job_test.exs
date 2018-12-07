defmodule Verk.JobTest do
  use ExUnit.Case
  alias Verk.Job

  describe "encode!/1" do
    @job %Job{
      queue: "test_queue",
      class: "DummyWorker",
      args: [1, 2, 3],
      original_json: "json_payload",
      max_retry_count: 5
    }
    test "returns a valid json string" do
      result = Job.encode!(@job)
      assert {:ok, _} = Jason.decode(result)
    end

    test "includes original job info" do
      map =
        @job
        |> Job.encode!()
        |> Jason.decode!()

      assert %{
               "queue" => "test_queue",
               "class" => "DummyWorker",
               "args" => "[1,2,3]",
               "max_retry_count" => 5
             } = map
    end

    test "does not encode the original json" do
      map =
        @job
        |> Job.encode!()
        |> Jason.decode!()

      assert Map.fetch(map, "original_json") == :error
    end

    test "wraps the args in a valid json string" do
      map =
        @job
        |> Job.encode!()
        |> Jason.decode!()

      assert is_binary(map["args"])
      assert {:ok, _} = Jason.decode(map["args"])
    end

    test "Uses default options if none passed" do
      map =
        Job.encode!(%Verk.Job{})
        |> Job.decode!()

      assert map ==
               %Verk.Job{
                 args: [],
                 class: nil,
                 created_at: nil,
                 enqueued_at: nil,
                 error_backtrace: nil,
                 error_message: nil,
                 failed_at: nil,
                 finished_at: nil,
                 jid: nil,
                 max_retry_count: nil,
                 original_json:
                   "{\"args\":\"[]\",\"class\":null,\"created_at\":null,\"enqueued_at\":null,\"error_backtrace\":null,\"error_message\":null,\"failed_at\":null,\"finished_at\":null,\"jid\":null,\"max_retry_count\":null,\"queue\":null,\"retried_at\":null,\"retry_count\":0}",
                 queue: nil,
                 retried_at: nil,
                 retry_count: 0
               }
    end

    test "Overrides configurations if they are passed" do
      map =
        Job.encode!(%Verk.Job{max_retry_count: 5, queue: :default})
        |> Job.decode!()

      assert map ==
               %Verk.Job{
                 args: [],
                 class: nil,
                 created_at: nil,
                 enqueued_at: nil,
                 error_backtrace: nil,
                 error_message: nil,
                 failed_at: nil,
                 finished_at: nil,
                 jid: nil,
                 max_retry_count: 5,
                 original_json:
                   "{\"args\":\"[]\",\"class\":null,\"created_at\":null,\"enqueued_at\":null,\"error_backtrace\":null,\"error_message\":null,\"failed_at\":null,\"finished_at\":null,\"jid\":null,\"max_retry_count\":5,\"queue\":\"default\"\,\"retried_at\":null,\"retry_count\":0}",
                 queue: "default",
                 retried_at: nil,
                 retry_count: 0
               }
    end
  end

  describe "decode!/1" do
    test "includes original json" do
      payload = ~s({ "queue" : "test_queue", "args" : "[1, 2, 3]",
                   "max_retry_count" : 5})

      assert Job.decode!(payload) == %Job{
               queue: "test_queue",
               args: [1, 2, 3],
               original_json: payload,
               max_retry_count: 5
             }
    end

    test "replaces map with array when job has no args" do
      payload = ~s({ "queue" : "test_queue", "args" : "{}",
                   "max_retry_count" : 5})

      assert Job.decode!(payload) == %Job{
               queue: "test_queue",
               args: [],
               original_json: payload,
               max_retry_count: 5
             }
    end
  end

  describe "decode/1" do
    test "includes original json" do
      payload = ~s({ "queue" : "test_queue", "args" : "[1, 2, 3]",
                   "max_retry_count" : 5})

      assert Job.decode(payload) ==
               {:ok,
                %Job{
                  queue: "test_queue",
                  args: [1, 2, 3],
                  original_json: payload,
                  max_retry_count: 5
                }}
    end

    test "json decode error returns error tuple" do
      payload = ~s({ "queue" : "test_queue", "args" : "[1, 2, [[{{]",
                   "max_retry_count" : 5})

      assert {:error, _} = Job.decode(payload)
    end

    test "replaces map with array when job has no args" do
      payload = ~s({ "queue" : "test_queue", "args" : "{}",
                   "max_retry_count" : 5})

      assert Job.decode(payload) ==
               {:ok,
                %Job{queue: "test_queue", args: [], original_json: payload, max_retry_count: 5}}
    end
  end

  describe "default_max_retry_count/0" do
    test "returns 25 if max_retry_count is not set" do
      assert Verk.Job.default_max_retry_count() == 25
    end

    test "returns the set max_retry_count value" do
      Application.put_env(:verk, :max_retry_count, 100)
      assert Verk.Job.default_max_retry_count() == 100
      Application.put_env(:verk, :max_retry_count, 25)
    end
  end
end
