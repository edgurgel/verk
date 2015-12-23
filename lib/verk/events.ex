defmodule Verk.Events do
  defmodule JobFinished do
    defstruct [:job, :finished_at]
  end

  defmodule JobStarted do
    defstruct [:job, :started_at]
  end

  defmodule JobFailed do
    defstruct [:job, :failed_at, :stacktrace]
  end
end
