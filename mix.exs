defmodule Verk.Mixfile do
  use Mix.Project

  @source_url "https://github.com/edgurgel/verk"
  @version "1.7.1"

  def project do
    [
      app: :verk,
      version: "1.7.1",
      elixir: "~> 1.9",
      build_embedded: Mix.env() == :prod,
      start_permanent: Mix.env() == :prod,
      test_coverage: [tool: Coverex.Task, coveralls: true],
      name: "Verk",
      package: package(),
      deps: deps(),
      docs: docs()
    ]
  end

  def application do
    [
      env: [
        node_id: "1",
        redis_url: "redis://127.0.0.1:6379"
      ]
    ]
  end

  defp deps do
    [
      {:redix, "~> 0.8"},
      {:jason, "~> 1.0"},
      {:poolboy, "~> 1.5"},
      {:confex, "~> 3.3"},
      {:gen_stage, "~> 1.0"},
      {:credo, "~> 0.9", only: [:dev, :test]},
      {:earmark, "~> 1.0", only: :dev},
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false},
      {:coverex, "== 1.5.0", only: :test},
      {:mimic, "~> 0.2", only: :test}
    ]
  end

  defp package do
    [
      description: "Verk is a job processing system backed by Redis.",
      maintainers: ["Eduardo Gurgel Pinho", "Alisson Sales"],
      licenses: ["MIT"],
      links: %{"Github" => @source_url}
    ]
  end

  defp docs do
    [
      extras: [
        "LICENSE.md": [title: "License"],
        "README.md": [title: "Overview"]
      ],
      main: "readme",
      source_url: @source_url,
      source_ref: "v#{@version}",
      formatters: ["html"]
    ]
  end
end
