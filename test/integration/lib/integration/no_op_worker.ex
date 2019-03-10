defmodule Integration.NoOpWorker do
  @moduledoc false
  def perform, do: :ok
end
