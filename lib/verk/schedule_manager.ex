defmodule Verk.ScheduleManager do
  @moduledoc """
  The ScheduleManager looks for jobs to be retried and accept jobs to scheduled to be retried

  The job is added back to the queue when it's ready to be run or retried
  """

  use GenServer
  require Logger
  alias Verk.Time

  @default_poll_interval 5000
  @schedule_key "schedule"
  @retry_key Verk.RetrySet.key()

  @enqueue_retriable_script_sha Verk.Scripts.sha("enqueue_retriable_job")

  defmodule State do
    @moduledoc false
    defstruct [:redis]
  end

  @doc false
  def start_link(_) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  @doc """
  Connect to redis and timeout with the `poll_interval`
  """
  def init(_) do
    {:ok, redis} = Redix.start_link(Confex.get_env(:verk, :redis_url))
    Verk.Scripts.load(redis)

    state = %State{redis: redis}

    Logger.info("Schedule Manager started")
    schedule_fetch!(:fetch_retryable)
    schedule_fetch!(:fetch_scheduled)
    {:ok, state}
  end

  @doc """
  Search for retryable jobs to be done and if removal succeeds enqueue the job
  """
  def handle_info(:fetch_retryable, state) do
    handle_info(:fetch_retryable, state, @retry_key)
  end

  @doc """
  Search for scheduled jobs to be done and if removal succeeds enqueue the job
  """
  def handle_info(:fetch_scheduled, state) do
    handle_info(:fetch_scheduled, state, @schedule_key)
  end

  defp handle_info(fetch_message, state, queue) do
    now = Time.now() |> DateTime.to_unix(:second)

    case Redix.command(state.redis, ["EVALSHA", @enqueue_retriable_script_sha, 1, queue, now]) do
      {:ok, nil} ->
        schedule_fetch!(fetch_message)
        {:noreply, state}

      {:ok, _job} ->
        schedule_fetch!(fetch_message, 0)
        {:noreply, state}

      {:error, %Redix.Error{message: message}} ->
        Logger.error("Failed to fetch #{queue} set. Error: #{message}")
        {:stop, :redis_failed, state}

      error ->
        Logger.error("Failed to fetch #{queue} set. Error: #{inspect(error)}")
        schedule_fetch!(fetch_message)
        {:noreply, state}
    end
  end

  defp schedule_fetch!(fetch_message) do
    interval = Confex.get_env(:verk, :poll_interval, @default_poll_interval)
    schedule_fetch!(fetch_message, interval)
  end

  defp schedule_fetch!(fetch_message, interval) do
    Process.send_after(self(), fetch_message, interval)
  end
end
