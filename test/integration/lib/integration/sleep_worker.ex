defmodule Integration.SleepWorker do
  def perform(sleep) do
    :timer.sleep(sleep)
  end
end
