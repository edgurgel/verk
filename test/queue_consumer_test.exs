defmodule Verk.QueueConsumerTest do
  use ExUnit.Case
  import Mimic
  import Verk.QueueConsumer
  alias Verk.{QueueConsumer.State, Queue}

  setup :verify_on_exit!

  @queue "test_queue"
  @node_id 123

  setup_all do
    Application.put_env(:verk, :local_node_id, @node_id)

    on_exit(fn ->
      Application.delete_env(:verk, :local_node_id)
    end)
  end

  setup do
    stub(Redix)

    :ok
  end

  describe "init/1" do
    test "sets up redis connection" do
      workers_manager = :workers_manager
      timeout = 30_000
      redis = :redis
      command = ["XGROUP", "CREATE", "verk:queue:#{@queue}", "verk", 0, "MKSTREAM"]

      state = %State{
        queue: @queue,
        redis: redis,
        workers_manager: workers_manager,
        node_id: @node_id,
        demand: 0,
        last_id: 0,
        timeout: timeout
      }

      expect(Redix, :start_link, fn _ -> {:ok, :redis} end)
      expect(Redix, :command, fn :redis, ^command -> :ok end)
      assert init([@queue, workers_manager, timeout]) == {:ok, state}
    end
  end

  describe "handle_cast/2" do
    test "consumes when demand is 0" do
      state = %State{demand: 0}
      assert handle_cast({:ask, 123}, state) == {:noreply, %{state | demand: 123}}
      assert_received :consume
    end

    test "does nothing when when demand is greater than 0" do
      state = %State{demand: 1}
      assert handle_cast({:ask, 123}, state) == {:noreply, %{state | demand: 124}}
      refute_received :consume
    end
  end

  describe "handle_info/2 consume last_id '>'" do
    test "nil response" do
      demand = 25
      timeout = 30_000

      state = %State{
        queue: @queue,
        node_id: @node_id,
        demand: demand,
        redis: :redis,
        last_id: ">",
        timeout: timeout
      }

      expect(Queue, :consume, fn @queue, @node_id, ">", ^demand, ^timeout, :redis ->
        {:ok, nil}
      end)

      assert handle_info(:consume, state) == {:noreply, state}
      assert_received :consume
    end

    test "jobs that fulfil demand" do
      demand = 1
      jobs = [["item_id", ["job", "job1"]]]
      timeout = 30_000

      state = %State{
        queue: @queue,
        node_id: @node_id,
        demand: demand,
        redis: :redis,
        workers_manager: self(),
        last_id: ">",
        timeout: timeout
      }

      expect(Queue, :consume, fn @queue, @node_id, ">", ^demand, ^timeout, :redis ->
        {:ok, jobs}
      end)

      assert handle_info(:consume, state) == {:noreply, %{state | demand: 0}}

      assert_received {:jobs, ^jobs}
      refute_received :consume
    end

    test "jobs that do not fulfil demand" do
      demand = 3
      jobs = [["item_id", ["job", "job1"]]]
      timeout = 30_000

      state = %State{
        queue: @queue,
        node_id: @node_id,
        demand: demand,
        redis: :redis,
        workers_manager: self(),
        last_id: ">",
        timeout: timeout
      }

      expect(Queue, :consume, fn @queue, @node_id, ">", ^demand, ^timeout, :redis ->
        {:ok, jobs}
      end)

      assert handle_info(:consume, state) == {:noreply, %{state | demand: 2}}

      assert_received {:jobs, ^jobs}
      assert_received :consume
    end

    test "error consuming" do
      demand = 3
      timeout = 30_000

      state = %State{
        queue: @queue,
        node_id: @node_id,
        demand: demand,
        redis: :redis,
        last_id: ">",
        timeout: timeout
      }

      expect(Queue, :consume, fn @queue, @node_id, ">", ^demand, ^timeout, :redis ->
        {:error, :reason}
      end)

      assert handle_info(:consume, state) == {:noreply, state}

      assert_received :consume
    end
  end

  describe "handle_info/2 consume last_id is not '>'" do
    test "nil response" do
      demand = 25
      last_id = 0
      timeout = 30_000

      state = %State{
        queue: @queue,
        node_id: @node_id,
        demand: demand,
        redis: :redis,
        last_id: last_id,
        timeout: timeout
      }

      expect(Queue, :consume, fn @queue, @node_id, ^last_id, ^demand, ^timeout, :redis ->
        {:ok, nil}
      end)

      assert handle_info(:consume, state) == {:noreply, state}
      assert_received :consume
    end

    test "empty jobs response" do
      demand = 25
      last_id = 0
      timeout = 30_000

      state = %State{
        queue: @queue,
        node_id: @node_id,
        demand: demand,
        redis: :redis,
        last_id: last_id,
        timeout: timeout
      }

      expect(Queue, :consume, fn @queue, @node_id, ^last_id, ^demand, ^timeout, :redis ->
        {:ok, []}
      end)

      assert handle_info(:consume, state) == {:noreply, %{state | last_id: ">"}}
      assert_received :consume
    end

    test "jobs that fulfil demand" do
      demand = 2
      last_id = 0
      timeout = 30_000
      jobs = [["item_id1", ["job", "job1"]], ["item_id2", ["job", "job2"]]]

      state = %State{
        queue: @queue,
        node_id: @node_id,
        demand: demand,
        redis: :redis,
        workers_manager: self(),
        last_id: last_id,
        timeout: timeout
      }

      expect(Queue, :consume, fn @queue, @node_id, ^last_id, ^demand, ^timeout, :redis ->
        {:ok, jobs}
      end)

      assert handle_info(:consume, state) == {:noreply, %{state | demand: 0, last_id: "item_id2"}}

      assert_received {:jobs, ^jobs}
      refute_received :consume
    end

    test "jobs that do not fulfil demand" do
      demand = 3
      last_id = 0
      timeout = 30_000

      jobs = [["item_id1", ["job", "job1"]], ["item_id2", ["job", "job2"]]]

      state = %State{
        queue: @queue,
        node_id: @node_id,
        demand: demand,
        redis: :redis,
        workers_manager: self(),
        last_id: last_id,
        timeout: timeout
      }

      expect(Queue, :consume, fn @queue, @node_id, ^last_id, ^demand, ^timeout, :redis ->
        {:ok, jobs}
      end)

      assert handle_info(:consume, state) == {:noreply, %{state | last_id: "item_id2", demand: 1}}

      assert_received {:jobs, ^jobs}
      assert_received :consume
    end

    test "error consuming" do
      demand = 3
      timeout = 30_000

      state = %State{
        queue: @queue,
        node_id: @node_id,
        demand: demand,
        redis: :redis,
        last_id: ">",
        timeout: timeout
      }

      expect(Queue, :consume, fn @queue, @node_id, ">", ^demand, ^timeout, :redis ->
        {:error, :reason}
      end)

      assert handle_info(:consume, state) == {:noreply, state}

      assert_received :consume
    end
  end
end
