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
  @spec perform_async(pid, pid, atom, list(term), binary) :: :ok
  def perform_async(worker, manager, module, args, job_id) do
    GenServer.cast(worker, { :perform, module, args, job_id, manager })
  end

  @doc false
  def init(_args \\ []), do: { :ok, nil }

  @doc false
  def handle_cast({ :perform, module, args, job_id, manager }, state) do
    try do
      apply(String.to_atom("Elixir.#{module}"), :perform, args)
      GenServer.cast(manager, { :done, self, job_id })
      { :stop, :normal, state }
    rescue
      exception -> GenServer.cast(manager, { :failed, self, job_id, exception, System.stacktrace })
      { :stop, :failed, state }
    end
  end
end
