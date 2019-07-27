defmodule Verk.Redis.EntryID do
  @moduledoc """
  The entry ID returned by the XADD command, and identifying univocally each entry inside a given stream, is composed of two parts:

  <millisecondsTime>-<sequenceNumber>
  """
  defstruct [:time, :seq]

  @max_value 18_446_744_073_709_551_615

  @doc """
  Parse entry ids

  Examples:

  iex> EntryID.parse("0-0")
  %EntryID{time: 0, seq: 0}

  iex> EntryID.parse("123-5")
  %EntryID{time: 123, seq: 5}
  """
  @spec parse(binary) :: %__MODULE__{}
  def parse("-"), do: %__MODULE__{time: 0, seq: 0}
  def parse("+"), do: %__MODULE__{time: @max_value, seq: @max_value}

  def parse(entry_id) when is_binary(entry_id) do
    [time, seq] = String.split(entry_id, "-")
    {time, _} = Integer.parse(time)
    {sequence, _} = Integer.parse(seq)
    %__MODULE__{time: time, seq: sequence}
  end

  @doc """
  Min possible entry id

  Example:

  iex> EntryID.min()
  %EntryID{time: 0, seq: 0}
  """
  @spec min :: %__MODULE__{}
  def min, do: %__MODULE__{time: 0, seq: 0}

  @doc """
  Max possible entry id

  Example:

  iex> EntryID.max()
  %EntryID{time: 18_446_744_073_709_551_615, seq: 18_446_744_073_709_551_615}
  """
  @spec max :: %__MODULE__{}
  def max, do: %__MODULE__{time: @max_value, seq: @max_value}

  @doc """
  Next sequence for an entry id

  Examples

  iex> EntryID.next(%EntryID{time: 123, seq: 3})
  %EntryID{time: 123, seq: 4}
  """
  @spec next(%__MODULE__{}) :: %__MODULE__{}
  def next(%__MODULE__{time: time, seq: seq}), do: %__MODULE__{time: time, seq: seq + 1}
end

defimpl String.Chars, for: Verk.Redis.EntryID do
  def to_string(%Verk.Redis.EntryID{time: time, seq: sequence}) do
    "#{time}-#{sequence}"
  end
end
