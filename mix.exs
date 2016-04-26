defmodule Verk.Mixfile do
  use Mix.Project

  @description """
    Verk is a job processing system backed by Redis.
  """

  def project do
    [app: :verk,
     version: "0.11.0",
     elixir: "~> 1.0",
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     test_coverage: [tool: Coverex.Task, coveralls: true],
     name: "Verk",
     description: @description,
     package: package,
     deps: deps]
  end

  def application do
    [applications: [:logger, :poison, :tzdata, :timex, :redix, :poolboy, :watcher],
     env: [node_id: "1", redis_url: "redis://127.0.0.1:6379"],
     mod: {Verk, []}]
  end

  defp deps do
    [{ :redix, "~> 0.3" },
     { :poison, "~> 1.5 or ~> 2.0"},
     { :timex, "~> 2.0" },
     { :poolboy, "~> 1.5.1" },
     { :watcher, "~> 1.0" },
     { :credo, "~> 0.3", only: [:dev, :test] },
     { :earmark, "~> 0.1.17", only: :docs },
     { :ex_doc, "~> 0.8.0", only: :docs },
     { :coverex, "~> 1.4.7", only: :test },
     { :meck, "~> 0.8", only: :test }]
  end

  defp package do
    [ maintainers: ["Eduardo Gurgel Pinho", "Alisson Sales"],
      licenses: ["MIT"],
      links: %{"Github" => "https://github.com/edgurgel/verk"} ]
  end
end
