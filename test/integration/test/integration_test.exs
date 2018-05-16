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
      Enum.each(events, fn event -> send parent, event end)
      {:noreply, [], parent}
    end
  end

  setup_all do
    {:ok, redis} = Redix.start_link(Confex.get_env(:verk, :redis_url))
    Redix.command!(redis, ["FLUSHDB"])
    {:ok, redis: redis}
  end

  @tag integration: true
  test "shutdown", %{redis: redis} do
    for _x <- (0..10) do
      Verk.enqueue(%Verk.Job{queue: "queue_one", class: Integration.SleepWorker, args: [1_500]}, redis)
      Verk.enqueue(%Verk.Job{queue: "queue_two", class: Integration.SleepWorker, args: [2_000]}, redis)
    end
    Application.ensure_all_started(:integration)
    {:ok, _consumer} = Consumer.start
    Application.stop(:integration)

    assert_receive %Verk.Events.QueuePausing{queue: :queue_one}
    assert_receive %Verk.Events.QueuePausing{queue: :queue_two}
    assert_receive %Verk.Events.QueuePaused{queue: :queue_one}
    assert_receive %Verk.Events.QueuePaused{queue: :queue_two}
  end
end
