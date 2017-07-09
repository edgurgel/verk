defmodule Verk.Time do
  @moduledoc """
  Internal API for time management and comparison functions
  """

  @doc false
  @spec now() :: DateTime.t
  def now do
    DateTime.utc_now
  end

  @doc """
  Moves DateTime supplied the indicated amount of time.

  Supported units are those specified by `System.time_unit`
  """
  @spec shift(DateTime.t, integer, System.time_unit) :: DateTime.t
  def shift(%DateTime{} = datetime, amount, units \\ :seconds) when is_integer(amount) do
    datetime
    |> DateTime.to_unix(units)
    |> Kernel.+(amount)
    |> DateTime.from_unix!(units)
  end

  @doc """
  Returns the number of indicated units that separate the two timestamps.

  A positive result indicates that `datetime2` occurred after `datetime1`.
  """
  @spec diff(DAteTime.t, DateTime.t, System.time_unit) :: integer
  def diff(datetime1 = %DateTime{}, datetime2 = %DateTime{}, units \\ :seconds) do
    unix_dt1 = DateTime.to_unix(datetime1, units)
    unix_dt2 = DateTime.to_unix(datetime2, units)
    unix_dt2 - unix_dt1
  end

  @doc """
  returns true if the first argument occurred after the second argument
  """
  @spec after?(DateTime.t, DateTime.t) :: boolean
  def after?(datetime1, datetime2) do
    diff(datetime1, datetime2) < 0
  end
end
