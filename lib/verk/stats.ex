defmodule Verk.Stats do
  @moduledoc """
  Basic stats for Verk
  """

  @doc """
  Total amount of processed and failed jobs
  """
  @spec total(GenServer.server) :: Map.t
  def total(redis \\ Verk.Redis) do
    [processed, failed] = Redix.command!(redis, ~w(MGET stat:processed stat:failed))
    %{processed: to_int(processed), failed: to_int(failed)}
  end

  defp to_int(nil), do: 0
  defp to_int(string), do: String.to_integer(string)
end
