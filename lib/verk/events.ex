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
end
