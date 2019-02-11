defmodule Verk.QueueStatsTest do
  use ExUnit.Case, async: true
  import Verk.QueueStats
  import Mimic
  alias Verk.QueueStatsCounters
  alias Verk.QueueStats.State

  setup :verify_on_exit!

  @table :queue_stats

  setup do
    {:ok, _} = Confex.get_env(:verk, :redis_url) |> Redix.start_link(name: Verk.Redis)

    Redix.pipeline!(Verk.Redis, [
      [
        "DEL",
        "stat:failed",
        "stat:processed",
        "stat:failed:queue_1",
        "stat:processed:queue_1",
        "stat:failed:queue_2",
        "stat:processed:queue_2"
      ]
    ])

    on_exit(fn ->
      ref = Process.monitor(Verk.Redis)
      assert_receive {:DOWN, ^ref, _, _, _}
    end)

    :ok
  end

  describe "handle_call/3" do
    test "list counters" do
      QueueStatsCounters.init()

      handle_events([%Verk.Events.JobStarted{job: %Verk.Job{queue: "queue_1"}}], :from, :state)
      handle_events([%Verk.Events.JobStarted{job: %Verk.Job{queue: "queue_1"}}], :from, :state)
      handle_events([%Verk.Events.JobStarted{job: %Verk.Job{queue: "queue_2"}}], :from, :state)
      handle_events([%Verk.Events.JobFinished{job: %Verk.Job{queue: "queue_1"}}], :from, :state)
      handle_events([%Verk.Events.JobFailed{job: %Verk.Job{queue: "queue_1"}}], :from, :state)

      state = %State{queues: %{"queue_1" => :running, "queue_2" => :pausing}}
      new_state = %State{queues: %{"queue_1" => :idle, "queue_2" => :pausing}}

      assert handle_call({:all, ""}, :from, state) ==
               {
                 :reply,
                 [
                   %{
                     queue: "queue_1",
                     running_counter: 0,
                     finished_counter: 1,
                     failed_counter: 1,
                     status: :idle
                   },
                   %{
                     queue: "queue_2",
                     running_counter: 1,
                     finished_counter: 0,
                     failed_counter: 0,
                     status: :pausing
                   }
                 ],
                 [],
                 new_state
               }
    end

    test "list counters having no status of a queue" do
      QueueStatsCounters.init()

      handle_events([%Verk.Events.JobStarted{job: %Verk.Job{queue: "default"}}], :from, :state)

      state = %State{queues: %{}}
      new_state = %State{queues: %{"default" => :running}}

      expect(Verk.Manager, :status, fn "default" -> :running end)

      assert handle_call({:all, ""}, :from, state) ==
               {
                 :reply,
                 [
                   %{
                     queue: "default",
                     running_counter: 1,
                     finished_counter: 0,
                     failed_counter: 0,
                     status: :running
                   }
                 ],
                 [],
                 new_state
               }
    end

    test "list counters searching for a prefix" do
      QueueStatsCounters.init()

      handle_events([%Verk.Events.JobStarted{job: %Verk.Job{queue: "default"}}], :from, :state)
      handle_events([%Verk.Events.JobStarted{job: %Verk.Job{queue: "default"}}], :from, :state)
      handle_events([%Verk.Events.JobFinished{job: %Verk.Job{queue: "default"}}], :from, :state)

      handle_events(
        [%Verk.Events.JobStarted{job: %Verk.Job{queue: "default-something"}}],
        :from,
        :state
      )

      handle_events(
        [%Verk.Events.JobFailed{job: %Verk.Job{queue: "default-something"}}],
        :from,
        :state
      )

      handle_events([%Verk.Events.JobStarted{job: %Verk.Job{queue: "priority"}}], :from, :state)

      state = %State{queues: %{"default" => :running, "default-something" => :pausing}}

      assert handle_call({:all, "def"}, :from, state) ==
               {
                 :reply,
                 [
                   %{
                     queue: "default",
                     running_counter: 1,
                     finished_counter: 1,
                     failed_counter: 0,
                     status: :running
                   },
                   %{
                     queue: "default-something",
                     running_counter: 0,
                     finished_counter: 0,
                     failed_counter: 1,
                     status: :pausing
                   }
                 ],
                 [],
                 state
               }
    end
  end

  describe "init/1" do
    test "creates an ETS table" do
      assert :ets.info(@table) == :undefined

      assert init([]) == {:consumer, %State{}, subscribe_to: [Verk.EventProducer]}

      assert :ets.info(@table) != :undefined
    end
  end

  describe "handle_event/2" do
    test "with queue running and existing element" do
      QueueStatsCounters.init()
      :ets.insert_new(@table, {'default', 1, 2, 3, 4, 5})

      event = %Verk.Events.QueueRunning{queue: :default}
      state = %State{queues: %{}}

      assert handle_events([event], :from, state) ==
               {:noreply, [], %State{queues: %{"default" => :running}}}

      assert :ets.tab2list(@table) == [{'default', 0, 2, 3, 4, 5}]
    end

    test "with queue running" do
      QueueStatsCounters.init()
      event = %Verk.Events.QueueRunning{queue: :default}
      state = %State{queues: %{}}

      assert handle_events([event], :from, state) ==
               {:noreply, [], %State{queues: %{"default" => :running}}}

      assert :ets.tab2list(@table) == [{'default', 0, 0, 0, 0, 0}]
    end

    test "with started event" do
      QueueStatsCounters.init()
      event = %Verk.Events.JobStarted{job: %Verk.Job{queue: "queue"}}

      assert handle_events([event], :from, :state) == {:noreply, [], :state}

      assert :ets.tab2list(@table) == [{:total, 1, 0, 0, 0, 0}, {'queue', 1, 0, 0, 0, 0}]
    end

    test "with finished event" do
      QueueStatsCounters.init()
      event = %Verk.Events.JobFinished{job: %Verk.Job{queue: "queue"}}

      assert handle_events([event], :from, :state) == {:noreply, [], :state}

      assert :ets.tab2list(@table) == [{:total, -1, 1, 0, 0, 0}, {'queue', -1, 1, 0, 0, 0}]
    end

    test "with failed event" do
      QueueStatsCounters.init()
      event = %Verk.Events.JobFailed{job: %Verk.Job{queue: "queue"}}

      assert handle_events([event], :from, :state) == {:noreply, [], :state}

      assert :ets.tab2list(@table) == [{:total, -1, 0, 1, 0, 0}, {'queue', -1, 0, 1, 0, 0}]
    end
  end

  describe "handle_info/2" do
    test "persist processed and failed counts" do
      QueueStatsCounters.init()

      handle_events([%Verk.Events.JobStarted{job: %Verk.Job{queue: "queue_1"}}], :from, :state)
      handle_events([%Verk.Events.JobFailed{job: %Verk.Job{queue: "queue_1"}}], :from, :state)
      handle_events([%Verk.Events.JobStarted{job: %Verk.Job{queue: "queue_2"}}], :from, :state)
      handle_events([%Verk.Events.JobFinished{job: %Verk.Job{queue: "queue_2"}}], :from, :state)

      assert handle_info(:persist_stats, :state) == {:noreply, [], :state}

      result =
        Redix.command!(Verk.Redis, [
          "MGET",
          "stat:processed:queue_1",
          "stat:failed:queue_1",
          "stat:processed:queue_2",
          "stat:failed:queue_2",
          "stat:processed",
          "stat:failed"
        ])

      assert result == [
               nil,
               "1",
               "1",
               nil,
               "1",
               "1"
             ]

      handle_events([%Verk.Events.JobStarted{job: %Verk.Job{queue: "queue_1"}}], :from, :state)
      handle_events([%Verk.Events.JobFinished{job: %Verk.Job{queue: "queue_1"}}], :from, :state)
      assert handle_info(:persist_stats, :state) == {:noreply, [], :state}

      result =
        Redix.command!(Verk.Redis, [
          "MGET",
          "stat:processed:queue_1",
          "stat:failed:queue_1",
          "stat:processed:queue_2",
          "stat:failed:queue_2",
          "stat:processed",
          "stat:failed"
        ])

      assert result == [
               "1",
               "1",
               "1",
               nil,
               "2",
               "1"
             ]

      assert handle_info(:persist_stats, :state) == {:noreply, [], :state}

      result =
        Redix.command!(Verk.Redis, [
          "MGET",
          "stat:processed:queue_1",
          "stat:failed:queue_1",
          "stat:processed:queue_2",
          "stat:failed:queue_2",
          "stat:processed",
          "stat:failed"
        ])

      assert result == [
               "1",
               "1",
               "1",
               nil,
               "2",
               "1"
             ]
    end

    test 'test with unexpected message' do
      QueueStatsCounters.init()
      assert handle_info(:pretty_sweet, :state) == {:noreply, [], :state}
    end
  end
end
