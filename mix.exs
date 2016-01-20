defmodule Verk.Mixfile do
  use Mix.Project

  @description """
    Verk is a job processing system backed by Redis.
  """

  def project do
    [app: :verk,
     version: "0.9.4",
     elixir: "~> 1.0",
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     test_coverage: [tool: ExCoveralls],
     preferred_cli_env: ["coveralls": :test, "coveralls.detail": :test, "coveralls.post": :test],
     name: "Verk",
     description: @description,
     package: package,
     deps: deps]
  end

  def application do
    [applications: [:logger, :poison, :tzdata, :timex, :redix, :poolboy],
     mod: {Verk, []}]
  end

  defp deps do
    [{ :redix, "~> 0.3.3" },
     { :poison, "~> 1.5" },
     { :timex, "~> 1.0" },
     { :poolboy, "~> 1.5.1" },
     { :earmark, "~> 0.1.17", only: :docs },
     { :ex_doc, "~> 0.8.0", only: :docs },
     { :excoveralls, "~> 0.4", only: :test },
     { :meck, "~> 0.8", only: :test }]
  end

  defp package do
    [ maintainers: ["Eduardo Gurgel Pinho"],
      licenses: ["MIT"],
      links: %{"Github" => "https://github.com/edgurgel/verk"} ]
  end
end
