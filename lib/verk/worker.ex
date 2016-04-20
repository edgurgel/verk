defmodule Verk.Worker do
  @moduledoc """
  Worker executes the job, messages the manager when it's done and shutdowns
  """
  use GenServer
  require Logger

  @doc false
  def start_link(args \\ []) do
    GenServer.start_link(__MODULE__, args)
  end

  @doc """
  Ask the worker to perform the job
  """
  @spec perform_async(pid, pid, %Verk.Job{}) :: :ok
  def perform_async(worker, manager, job) do
    GenServer.cast(worker, { :perform, job, manager })
  end

  @doc false
  def init(_args \\ []), do: { :ok, nil }

  @doc false
  def handle_cast({ :perform, job, manager }, state) do
    try do
      apply(String.to_atom("Elixir.#{job.class}"), :perform, job.args)
      GenServer.cast(manager, { :done, self, job.jid })
      { :stop, :normal, state }
    rescue
      exception -> GenServer.cast(manager, { :failed, self, job.jid, exception, System.stacktrace })
      { :stop, :failed, state }
    end
  end
end
