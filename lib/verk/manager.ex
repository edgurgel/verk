defmodule Verk.Manager do
  @moduledoc """
  A process that manages the state of each started queue
  """

  use GenServer
  require Logger
  alias Verk.WorkersManager

  @table :verk_manager
  @ets_options [:ordered_set, :named_table, :public, read_concurrency: true]

  @doc false
  def start_link(queues), do: GenServer.start_link(__MODULE__, queues, name: __MODULE__)

  @doc false
  def init(queues) do
    ets = :ets.new(@table, @ets_options)
    local_verk_node_id = Application.fetch_env!(:verk, :local_node_id)

    for {queue, size} <- queues do
      :ets.insert_new(@table, {queue, size, :running})
      Verk.Node.add_queue!(local_verk_node_id, queue, Verk.Redis)
    end

    {:ok, ets}
  end

  @doc """
  It returns the status of each queue currently

  [{:default, 25, :paused}, {:low_priority, 10, :running}]
  """
  @spec status :: [{atom, pos_integer, atom}]
  def status, do: :ets.tab2list(@table)

  @doc """
  It returns the status of each queue currently

  [{:default, 25, :paused}, {:low_priority, 10, :running}]
  """
  @spec status(atom) :: :running | :paused
  def status(queue) do
    [{^queue, _, queue_status}] = :ets.lookup(@table, queue)
    queue_status
  end

  @spec pause(atom) :: boolean
  def pause(queue) do
    if :ets.update_element(@table, queue, {3, :paused}) do
      WorkersManager.pause(queue)
      true
    else
      false
    end
  end

  @spec resume(atom) :: boolean
  def resume(queue) do
    if :ets.update_element(@table, queue, {3, :running}) do
      WorkersManager.resume(queue)
      true
    else
      false
    end
  end

  @doc """
  It adds the `queue` running with the amount of `size` of workers
  It always returns the child spec
  """
  @spec add(atom, pos_integer) :: Supervisor.on_start_child()
  def add(queue, size) do
    unless :ets.insert_new(@table, {queue, size, :running}) do
      Logger.error("Queue #{queue} is already running")
    end

    local_verk_node_id = Application.fetch_env!(:verk, :local_node_id)
    Verk.Node.add_queue!(local_verk_node_id, queue, Verk.Redis)
    Verk.Supervisor.start_child(queue, size)
  end

  @doc """
  It removes the `queue`
  It returns `:ok` if successful and `{:error, :not_found}` otherwise
  """
  @spec remove(atom) :: :ok | {:error, :not_found}
  def remove(queue) do
    :ets.delete(@table, queue)
    local_verk_node_id = Application.fetch_env!(:verk, :local_node_id)
    Verk.Node.remove_queue!(local_verk_node_id, queue, Verk.Redis)
    Verk.Supervisor.stop_child(queue)
  end
end
