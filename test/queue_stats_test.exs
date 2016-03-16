defmodule Verk.QueueStatsTest do
  use ExUnit.Case
  import Verk.QueueStats

  @table :queue_stats

  setup do
    { :ok, _ } = Application.fetch_env(:verk, :redis_url)
                  |> elem(1)
                  |> Redix.start_link([name: Verk.Redis])
    :ok
  end

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

  test "persist processed and failed counts" do
    init([])
    Redix.pipeline!(Verk.Redis, [["DEL",
      "stat:failed", "stat:processed",
      "stat:failed:queue_1", "stat:processed:queue_1",
      "stat:failed:queue_2", "stat:processed:queue_2"
    ]])
    handle_event(%Verk.Events.JobFinished{ job: %Verk.Job{ queue: "queue_1" } }, :state)
    handle_event(%Verk.Events.JobFailed{ job: %Verk.Job{ queue: "queue_2" } }, :state)
    handle_info(:persist_stats, :state)
    result = Redix.pipeline!(Verk.Redis, [
      ["GET", "stat:processed:queue_1"], ["GET", "stat:failed:queue_1"],
      ["GET", "stat:processed:queue_2"], ["GET", "stat:failed:queue_2"],
      ["GET", "stat:processed"], ["GET", "stat:failed"]
    ])
    assert result == [
      "1", "0",
      "0", "1",
      "1", "1"
    ]

    handle_event(%Verk.Events.JobFinished{ job: %Verk.Job{ queue: "queue_2" } }, :state)
    handle_info(:persist_stats, :state)
    result = Redix.pipeline!(Verk.Redis, [
      ["GET", "stat:processed:queue_1"], ["GET", "stat:failed:queue_1"],
      ["GET", "stat:processed:queue_2"], ["GET", "stat:failed:queue_2"],
      ["GET", "stat:processed"], ["GET", "stat:failed"]
    ])
    assert result == [
      "1", "0",
      "1", "1",
      "2", "1"
    ]
  end
end
