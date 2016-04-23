defmodule Verk.Log do
  @moduledoc """
  Helper module to log when a job starts, fails or finishes.
  """

  require Logger
  import Logger
  alias Verk.Job
  alias Timex.DateTime

  def start(%Job{jid: job_id, class: module}, process_id) do
    info("#{module} #{job_id} start", process_id: inspect(process_id))
  end

  def done(%Job{jid: job_id, class: module}, start_time, process_id) do
    info("#{module} #{job_id} done: #{elapsed_time(start_time)}", process_id: inspect(process_id))
  end

  def fail(%Job{jid: job_id, class: module}, start_time, process_id) do
    info("#{module} #{job_id} fail: #{elapsed_time(start_time)}", process_id: inspect(process_id))
  end

  defp elapsed_time(start_time) do
    now = DateTime.now
    seconds_diff = start_time |> Timex.diff(now, :seconds)

    if seconds_diff == 0 do
      milliseconds_diff = now.millisecond - start_time.millisecond
      "#{milliseconds_diff} ms"
    else
      "#{seconds_diff} s"
    end
  end
end
