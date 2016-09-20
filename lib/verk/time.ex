defmodule Verk.Time do
  def now do
    DateTime.utc_now
  end

  def shift(%DateTime{} = datetime, amount, units \\ :seconds) when is_integer(amount) do
    # with {:ok, unix_ts } = DateTime.to_unix(datetime, units),
    #      shifted = shift(unix_ts, amount),
    #      {:ok, new_dt } = DateTime.from_unix(shifted) do
    #   new_dt
    # else
    #   :error
    datetime
    |> DateTime.to_unix(units)
    |> Kernel.+(amount)
    |> DateTime.from_unix!(units)

  end

  def diff(%DateTime{} = datetime1, %DateTime{} = datetime2, units \\ :seconds) do
    unix_dt1 = DateTime.to_unix(datetime1, units)
    unix_dt2 = DateTime.to_unix(datetime2, units)
    unix_dt2 - unix_dt1
  end

  def after?(datetime1, datetime2) do
    diff(datetime1, datetime2) < 0
  end

end
