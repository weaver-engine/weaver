defmodule Weaver.MixProject do
  use Mix.Project

  def project do
    [
      app: :weaver,
      version: "0.1.0",
      elixir: "~> 1.9",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      dialyzer: [
        flags: [:unmatched_returns, :error_handling, :race_conditions, :no_opaque]
      ],
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Specifies which paths to compile per environment.
  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:jason, "~> 1.1"},
      {:graphql, "~> 0.15.0", hex: :graphql_erl},
      {:dlex, "0.4.1"},
      {:extwitter, "~> 0.9.0"},

      # must be added by the main app
      {:gen_stage, github: "weaver-engine/gen_stage", branch: "prosumer", only: [:dev, :test]},

      # Dev & Test
      {:credo, "~> 1.1", only: :dev, runtime: false},
      {:dialyxir, "~> 1.0.0-rc.7", only: :dev, runtime: false},
      {:mox, "~> 0.5", only: :test},
      {:faker, "~> 0.12", only: [:dev, :test]},
      {:mix_test_watch, "~> 0.9", only: :dev, runtime: false}
    ]
  end
end
