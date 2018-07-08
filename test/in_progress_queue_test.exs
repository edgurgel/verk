defmodule Verk.InProgressQueueTest do
  use ExUnit.Case
  import :meck
  alias Verk.InProgressQueue

  setup do
    new Redix
    on_exit fn -> unload() end
    :ok
  end

  describe "enqueue_in_progress/3" do
    test "applies script to the correct keys" do
      script_sha = Verk.Scripts.sha("lpop_rpush_src_dest")
      key = "inprogress:queue_name:node_id"
      commands = [:redis, ["EVALSHA", script_sha, 2, key, "queue:queue_name", 1000]]
      result = {:ok, []}
      expect(Redix, :command, commands, result)
      assert InProgressQueue.enqueue_in_progress("queue_name", "node_id", :redis)
      assert num_calls(Redix, :command, 2) == 1
    end
  end
end
