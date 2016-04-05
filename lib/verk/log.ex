defmodule Verk.Log do
  require Logger
  import Logger
  alias Verk.Job
  alias Timex.DateTime

  def start(%Job{jid: job_id, class: module}, process_id) do
    info("#{module} #{job_id} start", process_id: inspect(process_id))
  end

  def done(%Job{jid: job_id, class: module}, start_time, process_id) do
    info("#{module} #{job_id} done: #{elapsed(start_time)} secs", process_id: inspect(process_id))
  end

  def fail(%Job{jid: job_id, class: module}, start_time, process_id) do
    info("#{module} #{job_id} fail: #{elapsed(start_time)} secs", process_id: inspect(process_id))
  end

  defp elapsed(start_time) do
    start_time |> Timex.diff(DateTime.now, :seconds)
  end
end
