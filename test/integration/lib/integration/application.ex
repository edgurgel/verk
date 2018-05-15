defmodule Integration.Application do
  @moduledoc false

  use Application

  def start(_type, _args) do
    import Supervisor.Spec, warn: false

    children = [
      supervisor(Verk.Supervisor, [])
    ]

    opts = [strategy: :one_for_one, name: Integration.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
