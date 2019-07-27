defmodule Integration.SleepWorker do
  @moduledoc false
  def perform(sleep) do
    :timer.sleep(sleep)
  end
end
