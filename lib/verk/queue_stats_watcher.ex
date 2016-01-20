defmodule Verk.QueueStatsWatcher do
  @moduledoc """
  This process will watch the event handler QueueStats

  Inspired by Elixir's Logger.Watcher
  """
  use GenServer
  require Logger

  @doc false
  def start_link, do: GenServer.start_link(__MODULE__, [], [])

  @doc false
  def init(_) do
    Logger.info("Starting QueueStats handler to keep track of the work done by each queue")
    ref = Process.monitor(Verk.EventManager)
    :ok = GenEvent.add_mon_handler(Verk.EventManager, Verk.QueueStats, [])
    { :ok, ref }
  end

  @doc false
  def handle_info({ :gen_event_EXIT, _handler, reason }, ref) when reason in [:normal, :shutdown] do
    { :stop, reason, ref }
  end

  def handle_info({ :gen_event_EXIT, handler, reason }, ref) do
    Logger.error "GenEvent handler #{inspect handler}\n** (exit) #{format_exit(reason)}"

    { :stop, reason, ref }
  end

  def handle_info({ :DOWN, ref, _, _, reason }, ref) do
    Logger.error "QueueStats handler\n** (exit) #{format_exit(reason)}"

    { :stop, reason, ref }
  end

  def handle_info(_msg, state) do
    { :noreply, state }
  end

  defp format_exit({:EXIT, reason}), do: Exception.format_exit(reason)
  defp format_exit(reason), do: Exception.format_exit(reason)
end
