defmodule Verk.Manager do
  @moduledoc """
  A process that manages the state of each started queue
  """

  use GenServer
  require Logger

  @table :verk_manager
  @ets_options [:ordered_set, :named_table, :public, read_concurrency: true]

  @doc false
  def start_link(queues), do: GenServer.start_link(__MODULE__, queues, name: __MODULE__)

  @doc false
  def init(queues) do
    ets = :ets.new(@table, @ets_options)
    for {queue, size} <- queues, do: :ets.insert_new(@table, {queue, size})
    {:ok, ets}
  end

  @doc """
  It returns the status of each queue currently

  [{:default, 25}, {:low_priority, 10}]
  """
  @spec status :: [{atom, pos_integer}]
  def status, do: :ets.tab2list(@table)

  @doc """
  It adds the `queue` running with the amount of `size` of workers
  It always returns the child spec
  """
  @spec add(atom, pos_integer) :: Supervisor.on_start_child
  def add(queue, size) do
    unless :ets.insert_new(@table, {queue, size}) do
      Logger.error "Queue #{queue} is already running"
    end
    Verk.Supervisor.start_child(queue, size)
  end

  @doc """
  It removes the `queue`
  It returns `:ok` if successful and `{:error, :not_found}` otherwise
  """
  @spec remove(atom) :: :ok | {:error, :not_found}
  def remove(queue) do
    :ets.delete(@table, queue)
    Verk.Supervisor.stop_child(queue)
  end
end
