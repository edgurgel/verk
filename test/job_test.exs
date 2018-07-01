defmodule Verk.JobTest do
  use ExUnit.Case
  alias Verk.Job

  describe "encode!/1" do
    @job %Job{queue: "test_queue", class: "DummyWorker", args: [1, 2, 3], original_json: "json_payload", max_retry_count: 5}
    test "returns a valid json string" do
      result = Job.encode!(@job)
      assert {:ok, _} = Jason.decode(result)
    end

    test "includes original job info" do
      map =
        @job
        |> Job.encode!()
        |> Jason.decode!()

      assert %{"queue" => "test_queue", "class" => "DummyWorker", "args" => "[1,2,3]", "max_retry_count" => 5} = map
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
  end

  describe "decode!/1" do
    test "includes original json" do
      payload = ~s({ "queue" : "test_queue", "args" : "[1, 2, 3]",
                   "max_retry_count" : 5})

      assert Job.decode!(payload) == %Job{ queue: "test_queue", args: [1, 2, 3], original_json: payload, max_retry_count: 5}
    end

    test "replaces map with array when job has no args" do
      payload = ~s({ "queue" : "test_queue", "args" : "{}",
                   "max_retry_count" : 5})

      assert Job.decode!(payload) == %Job{ queue: "test_queue", args: [], original_json: payload, max_retry_count: 5}
    end
  end

  describe "decode/1" do
    test "includes original json" do
      payload = ~s({ "queue" : "test_queue", "args" : "[1, 2, 3]",
                   "max_retry_count" : 5})

      assert Job.decode(payload) == {:ok, %Job{ queue: "test_queue", args: [1, 2, 3], original_json: payload, max_retry_count: 5}}
    end

    test "json decode error returns error tuple" do
      payload = ~s({ "queue" : "test_queue", "args" : "[1, 2, [[{{]",
                   "max_retry_count" : 5})

      assert {:error, _} = Job.decode(payload)
    end

    test "replaces map with array when job has no args" do
      payload = ~s({ "queue" : "test_queue", "args" : "{}",
                   "max_retry_count" : 5})

      assert Job.decode(payload) == {:ok, %Job{ queue: "test_queue", args: [], original_json: payload, max_retry_count: 5}}
    end
  end
end
