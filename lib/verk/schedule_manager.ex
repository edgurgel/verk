defmodule Verk.ScheduleManager do
  @moduledoc """
  The ScheduleManager looks for jobs to be retried and accept jobs to scheduled to be retried

  The job is added back to the queue when it's ready to be retried
  """

  use GenServer
  require Logger
  alias Timex.Time

  @default_poll_interval 5000
  @retry_key "retry"

  @enqueue_retriable_script_sha Verk.Scripts.sha("enqueue_retriable_job")

  defmodule State do
    defstruct [:redis]
  end

  @doc false
  def start_link do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  @doc """
  Connect to redis and timeout with the `poll_interval`
  """
  def init(_) do
    { :ok, redis_url } = Application.fetch_env(:verk, :redis_url)
    { :ok, redis } = Redix.start_link(redis_url)
    Verk.Scripts.load(redis)

    state = %State{ redis: redis }

    Logger.info "Schedule Manager started"
    schedule_fetch_retryable!
    { :ok, state }
  end

  @doc """
  Search for jobs to be done and if removal succeeds enqueue the job
  """
  def handle_info(:fetch_retryable, state) do
    case Redix.command(state.redis, ["EVALSHA", @enqueue_retriable_script_sha, 1, @retry_key, Time.now(:secs)]) do
      { :ok, nil } ->
        schedule_fetch_retryable!
        { :noreply, state }
      { :ok, _job } ->
        schedule_fetch_retryable!(0)
        { :noreply, state }
      { :error, %Redix.Error{message: message} } ->
        Logger.error("Failed to fetch retry set. Error: #{message}")
        { :stop, :redis_failed, state }
      error ->
        Logger.error("Failed to fetch retry set. Error: #{inspect error}")
        schedule_fetch_retryable!
        { :noreply, state }
    end
  end

  defp schedule_fetch_retryable! do
    interval = Application.get_env(:verk, :poll_interval, @default_poll_interval)
    schedule_fetch_retryable!(interval)
  end
  defp schedule_fetch_retryable!(interval) do
    Process.send_after(self, :fetch_retryable, interval)
  end
end
