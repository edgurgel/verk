defmodule Verk.InProgressQueue do
  @moduledoc """
  This module interacts with the in progress queue
  """

  @external_resource "priv/lpop_rpush_src_dest.lua"
  @lpop_rpush_src_dest_script_sha Verk.Scripts.sha("lpop_rpush_src_dest")
  @max_enqueue_inprogress 1000

  def enqueue_in_progress(queue_name, node_id, redis) do
    in_progress_key = inprogress(queue_name, node_id)

    Redix.command(redis, [
      "EVALSHA",
      @lpop_rpush_src_dest_script_sha,
      2,
      in_progress_key,
      "queue:#{queue_name}",
      @max_enqueue_inprogress
    ])
  end

  defp inprogress(queue_name, node_id) do
    "inprogress:#{queue_name}:#{node_id}"
  end
end
