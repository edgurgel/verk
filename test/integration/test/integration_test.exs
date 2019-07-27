defmodule IntegrationTest do
  use ExUnit.Case
  alias Verk.Events.{QueueRunning, QueuePausing, QueuePaused}

  defmodule Consumer do
    use GenStage

    def start(), do: GenStage.start(__MODULE__, self())

    def init(parent) do
      filter = fn event -> event.__struct__ in [QueueRunning, QueuePausing, QueuePaused] end
      {:consumer, parent, subscribe_to: [{Verk.EventProducer, selector: filter}]}
    end

    def handle_events(events, _from, parent) do
      Enum.each(events, fn event -> send(parent, event) end)
      {:noreply, [], parent}
    end
  end

  setup do
    Application.ensure_all_started(:redix)
    {:ok, redis} = Redix.start_link(Confex.get_env(:verk, :redis_url))
    Redix.command!(redis, ["FLUSHDB"])
    {:ok, redis: redis}
  end

  defp enqueue_jobs!(redis) do
    for _x <- 0..10 do
      Verk.enqueue(
        %Verk.Job{queue: "queue_one", class: Integration.SleepWorker, args: [1_500]},
        redis
      )

      Verk.enqueue(
        %Verk.Job{queue: "queue_two", class: Integration.SleepWorker, args: [2_000]},
        redis
      )
    end
  end

  @tag integration: true
  test "shutdown gracefully stops queues", %{redis: redis} do
    enqueue_jobs!(redis)

    Application.ensure_all_started(:integration)
    {:ok, _consumer} = Consumer.start()
    assert Redix.command!(redis, ["SMEMBERS", "verk_nodes"]) == []
    Application.stop(:integration)

    assert_receive %Verk.Events.QueuePaused{queue: :queue_one}
    assert_receive %Verk.Events.QueuePaused{queue: :queue_two}
  end

  @tag integration: true
  test "maintains verk_nodes", %{redis: redis} do
    enqueue_jobs!(redis)

    Application.ensure_all_started(:integration)
    {:ok, _consumer} = Consumer.start()
    node_id = Application.fetch_env!(:verk, :local_node_id)
    assert Redix.command!(redis, ["TTL", "verk:node:#{node_id}"]) > 0

    Application.stop(:integration)

    assert_receive %Verk.Events.QueuePaused{queue: :queue_one}
    assert_receive %Verk.Events.QueuePaused{queue: :queue_two}
  end
end
