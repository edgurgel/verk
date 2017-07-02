defmodule Verk.Job do
  @moduledoc """
  The Job struct
  """

  @default_max_retry_count Confex.get_env(:verk, :max_retry_count, 25)
  @keys [error_message: nil, failed_at: nil, retry_count: 0, queue: nil, class: nil, args: [],
         jid: nil, finished_at: nil, enqueued_at: nil, retried_at: nil, error_backtrace: nil,
         max_retry_count: @default_max_retry_count]

  @type t :: %__MODULE__{error_message: String.t, failed_at: DateTime.t, retry_count: non_neg_integer,
                         queue: String.t, class: String.t | atom, jid: String.t, finished_at: DateTime.t,
                         retried_at: DateTime.t, error_backtrace: String.t}
  @derive {Poison.Encoder, only: Keyword.keys(@keys)}
  defstruct [:original_json | @keys]

  @doc """
  Decode the JSON payload storing the original json as part of the struct.
  """
  @spec decode(binary) :: {:ok, %__MODULE__{}} | {:error, Poison.Error.t}
  def decode(payload) do
    case Poison.decode(payload, as: %__MODULE__{}) do
      {:ok, job} -> {:ok, %Verk.Job{job | original_json: payload}}
      {:error, error} -> {:error, error}
    end
  end

  @doc """
  Decode the JSON payload storing the original json as part of the struct, raising if there is an error
  """
  @spec decode!(binary) :: %__MODULE__{}
  def decode!(payload) do
    job = Poison.decode!(payload, as: %__MODULE__{})
    %Verk.Job{job | original_json: payload}
  end

  def default_max_retry_count do
    @default_max_retry_count
  end
end
