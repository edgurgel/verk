defmodule Verk.PoisonVersion do
  defmacro __using__(_) do
    quote do
      def poison_version do
        unquote(Verk.PoisonVersion.runtime_poison_version)
      end

      def is_before_poison_2 do
        unquote(Verk.PoisonVersion.runtime_poison_version) |> Version.match?("<2.0.0")
      end
    end
  end

  def runtime_poison_version do
    Mix.Project.config
    |> Keyword.get(:deps)
    |> Mix.Dep.loaded
    |> Enum.find(fn(%{app: app})-> app == :poison end)
    |> Map.get(:status)
    |> elem(1)
  end
end
