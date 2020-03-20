defmodule Verk.InProgressQueueTest do
  use ExUnit.Case
  import Mimic
  alias Verk.InProgressQueue

  setup :verify_on_exit!

  describe "enqueue_in_progress/3" do
    test "applies script to the correct keys" do
      script_sha = Verk.Scripts.sha("lpop_rpush_src_dest")
      key = "inprogress:queue_name:node_id"
      commands = ["EVALSHA", script_sha, 2, key, "queue:queue_name", 1000]
      result = {:ok, []}
      expect(Redix, :command, fn :redis, ^commands -> result end)
      assert InProgressQueue.enqueue_in_progress("queue_name", "node_id", :redis)
    end
  end
end
