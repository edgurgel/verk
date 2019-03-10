defmodule Verk.Mixfile do
  use Mix.Project

  @description """
    Verk is a job processing system backed by Redis.
  """

  def project do
    [
      app: :verk,
      version: "2.0.0",
      elixir: "~> 1.8",
      build_embedded: Mix.env() == :prod,
      start_permanent: Mix.env() == :prod,
      test_coverage: [tool: Coverex.Task, coveralls: true],
      name: "Verk",
      description: @description,
      package: package(),
      deps: deps()
    ]
  end

  def application do
    [env: [redis_url: "redis://127.0.0.1:6379"]]
  end

  defp deps do
    [
      {:redix, "~> 0.10"},
      {:jason, "~> 1.0"},
      {:poolboy, "~> 1.5"},
      {:confex, "~> 3.3"},
      {:gen_stage, "~> 0.12"},
      {:credo, "~> 1.0", only: [:dev, :test], runtime: false},
      {:earmark, "~> 1.0", only: :dev, runtime: false},
      {:ex_doc, "~> 0.21", only: :dev, runtime: false},
      {:coverex, "== 1.4.13", only: :test},
      {:mimic, "~> 1.1", only: :test}
    ]
  end

  defp package do
    [
      maintainers: ["Eduardo Gurgel Pinho", "Alisson Sales"],
      licenses: ["MIT"],
      links: %{"Github" => "https://github.com/edgurgel/verk"}
    ]
  end
end
