defmodule VallumflowElixirDp.MixProject do
  use Mix.Project

  def project do
    [
      app: :vallumflow_elixir_dp,
      version: "0.1.0",
      elixir: "~> 1.17",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger, :tzdata],
      mod: {VallumflowElixirDp.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:mongodb_driver, "~> 1.5"},
      {:brod, "~> 4.3.3"},
      {:tzdata, "~> 1.1"},
      {:telemetry, "~> 1.3"},
      {:telemetry_metrics, "~> 1.0"},
      {:telemetry_metrics_prometheus, "~> 1.1"},
      {:plug_cowboy, "~> 2.7"},
      {:jason, "~> 1.4"},
      {:dotenvy, "~> 0.8.0"}
    ]
  end
end
