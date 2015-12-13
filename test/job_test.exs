defmodule Verk.JobTest do
  use ExUnit.Case
  alias Verk.Job

  test "decode! includes original json" do
    payload = "{ \"queue\" : \"test_queue\", \"args\" : [1, 2, 3] }"

    assert Job.decode!(payload) == %Job{ queue: "test_queue", args: [1, 2, 3], original_json: payload }
  end
end
