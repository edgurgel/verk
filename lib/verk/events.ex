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
