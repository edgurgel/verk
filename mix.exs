defmodule Verk.Mixfile do
  use Mix.Project

  @description """
    Verk is a job processing system backed by Redis.
  """

  def project do
    [app: :verk,
     version: "0.9.0",
     elixir: "~> 1.1",
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     name: "Verk",
     description: @description,
     package: package,
     deps: deps]
  end

  def application do
    [applications: [:logger],
     mod: {Verk, []}]
  end

  defp deps do
    [{ :redix, "~> 0.1.0" },
     { :poison, "~> 1.5" },
     { :timex, "~> 0.19" },
     { :poolboy, "~> 1.5.1" },
     { :meck, "~> 0.8", only: :test }]
  end

  defp package do
    [ contributors: ["Eduardo Gurgel Pinho"],
      licenses: ["MIT"],
      links: %{"Github" => "https://github.com/edgurgel/verk"} ]
  end
end
