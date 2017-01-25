defmodule Verk.Events do
  defmodule JobFinished do
    @moduledoc false
    defstruct [:job, :finished_at]
  end

  defmodule JobStarted do
    @moduledoc false
    defstruct [:job, :started_at]
  end

  defmodule JobFailed do
    @moduledoc false
    defstruct [:job, :failed_at, :stacktrace, :exception]
  end
end

if Code.ensure_loaded?(GenStage) do
  defmodule Verk.EventHandler do
    @moduledoc """
    A GenStage producer that broadcasts events to subscribed consumers.
    """
    use GenStage

    def start_link do
      GenStage.start_link(__MODULE__, :ok, name: __MODULE__)
    end

    def async_notify(event) do
      GenStage.cast(__MODULE__, {:notify, event})
    end


    def init(:ok) do
      {:producer, :ok, dispatcher: GenStage.BroadcastDispatcher}
    end

    def handle_cast({:notify, event}, state) do
      {:noreply, [event], state}
    end

    def handle_demand(_demand, state) do
      {:noreply, [], state}
    end
  end
end
