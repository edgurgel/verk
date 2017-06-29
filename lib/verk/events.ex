defmodule Verk.Events do
  @moduledoc """
  Events generated while jobs are being processed
  """

  defmodule JobFinished do
    @moduledoc """
    When a job finishes this event is generated
    """
    @type t :: %__MODULE__{job: Verk.Job.t, finished_at: DateTime.t, started_at: DateTime.t}
    defstruct [:job, :started_at, :finished_at]
  end

  defmodule JobStarted do
    @moduledoc """
    When a job starts this event is generated
    """
    @type t :: %__MODULE__{job: Verk.Job.t, started_at: DateTime.t}
    defstruct [:job, :started_at]
  end

  defmodule JobFailed do
    @moduledoc """
    When a job fails this event is generated
    """
    @type t :: %__MODULE__{job: Verk.Job.t, started_at: DateTime.t, failed_at: DateTime.t,
                           stacktrace: [:erlang.stack_item()], exception: Exception.t}
    defstruct [:job, :started_at, :failed_at, :stacktrace, :exception]
  end

  defmodule QueueRunning do
    @moduledoc """
    When a queue is running
    """
    @type t :: %__MODULE__{queue: atom}
    defstruct [:queue]
  end
  defmodule QueuePausing do
    @moduledoc """
    When a queue is pausing
    """
    @type t :: %__MODULE__{queue: atom}
    defstruct [:queue]
  end
  defmodule QueuePaused do
    @moduledoc """
    When a queue paused
    """
    @type t :: %__MODULE__{queue: atom}
    defstruct [:queue]
  end
end
