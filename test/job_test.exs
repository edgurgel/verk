defmodule Verk.JobTest do
  use ExUnit.Case
  alias Verk.Job

  describe "decode!/1" do
    test "includes original json" do
      payload = ~s({ "queue" : "test_queue", "args" : [1, 2, 3],
                   "max_retry_count" : 5})

      assert Job.decode!(payload) == %Job{ queue: "test_queue", args: [1, 2, 3], original_json: payload, max_retry_count: 5}
    end
  end

  describe "decode/1" do
    test "includes original json" do
      payload = ~s({ "queue" : "test_queue", "args" : [1, 2, 3],
                   "max_retry_count" : 5})

      assert Job.decode(payload) == {:ok, %Job{ queue: "test_queue", args: [1, 2, 3], original_json: payload, max_retry_count: 5}}
    end

    test "json decode error returns error tuple" do
      payload = ~s({ "queue" : "test_queue", "args" : [1, 2, [[{{],
                   "max_retry_count" : 5})

      assert {:error, _} = Job.decode(payload)
    end
  end
end
