defmodule Verk.QueueStatsTest do
  use ExUnit.Case
  import Verk.QueueStats

  @table :queue_stats

  test "init creates an ETS table" do
    assert :ets.info(@table) == :undefined

    assert init([]) == { :ok, nil }

    assert :ets.info(@table) != :undefined
  end

  test "handle_event with started event" do
    init([]) # create table
    event = %Verk.Events.JobStarted{ job: %Verk.Job{ queue: "queue" } }

    assert handle_event(event, :state) == { :ok, :state }

    assert :ets.tab2list(@table) == [{"queue", 1, 0, 0}]
  end

  test "handle_event with finished event" do
    init([]) # create table
    event = %Verk.Events.JobFinished{ job: %Verk.Job{ queue: "queue" } }

    assert handle_event(event, :state) == { :ok, :state }

    assert :ets.tab2list(@table) == [{"queue", -1, 1, 0}]
  end

  test "handle_event with failed event" do
    init([]) # create table
    event = %Verk.Events.JobFailed{ job: %Verk.Job{ queue: "queue" } }

    assert handle_event(event, :state) == { :ok, :state }

    assert :ets.tab2list(@table) == [{"queue", -1, 0, 1}]
  end

  test "all with no data" do
    init([]) # create table

    assert all == []
  end

  test "all with different events happening" do
    init([]) # create table

    handle_event(%Verk.Events.JobStarted{ job: %Verk.Job{ queue: "queue_1" } }, :state)
    handle_event(%Verk.Events.JobStarted{ job: %Verk.Job{ queue: "queue_1" } }, :state)
    handle_event(%Verk.Events.JobStarted{ job: %Verk.Job{ queue: "queue_1" } }, :state)
    handle_event(%Verk.Events.JobStarted{ job: %Verk.Job{ queue: "queue_2" } }, :state)
    handle_event(%Verk.Events.JobFinished{ job: %Verk.Job{ queue: "queue_1" } }, :state)
    handle_event(%Verk.Events.JobFailed{ job: %Verk.Job{ queue: "queue_1" } }, :state)

    assert all == [%{ queue: "queue_1", running_counter: 1, finished_counter: 1, failed_counter: 1 },
                   %{ queue: "queue_2", running_counter: 1, finished_counter: 0, failed_counter: 0 } ]
  end
end
