defmodule Verk.Worker do
  @moduledoc """
  Worker executes the job, messages the manager when it's done and shutdowns
  """
  use GenServer
  require Logger

  @process_dict_key :verk_current_job

  @doc """
  Get the current job that the worker is running
  """
  @spec current_job :: %Verk.Job{}
  def current_job, do: :erlang.get(@process_dict_key)

  @doc false
  def start_link(args \\ []) do
    GenServer.start_link(__MODULE__, args)
  end

  @doc """
  Ask the worker to perform the job
  """
  @spec perform_async(pid, pid, %Verk.Job{}) :: :ok
  def perform_async(worker, manager, job) do
    GenServer.cast(worker, {:perform, job, manager})
  end

  @doc false
  def init(_args \\ []), do: {:ok, nil}

  @doc false
  def handle_cast({:perform, job, manager}, state) do
    :erlang.put(@process_dict_key, job)
    [job.class] |> Module.safe_concat |> apply(:perform, job.args)
    GenServer.cast(manager, {:done, self(), job.jid})
    {:stop, :normal, state}
  rescue
    exception -> GenServer.cast(manager, {:failed, self(), job.jid, exception, System.stacktrace})
    {:stop, :failed, state}
  end
end
