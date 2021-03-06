defmodule DexyPluginKV.Mixfile do
  use Mix.Project

  def project do
    [app: :dexy_plugin_kv,
     version: "0.1.0",
     elixir: "~> 1.3",
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     deps: deps()]
  end

  # Configuration for the OTP application
  #
  # Type "mix help compile.app" for more information
  def application do
    [
      applications: [:logger],
    ]
  end

  # Dependencies can be Hex packages:
  #
  #   {:mydep, "~> 0.3.0"}
  #
  # Or git/path repositories:
  #
  #   {:mydep, git: "https://github.com/elixir-lang/mydep.git", tag: "0.1.0"}
  #
  # Type "mix help deps" for more examples and options
  defp deps do
    [
      {:dexy_lib, github: "dexyio/dexy_lib"},
      {:pooler, "~> 1.5", only: [:test, :dev]},

      # adapters
      #{:riakc, "~> 2.4", override: true}
      {:riakc, github: "basho/riak-erlang-client", tag: "2.5.2", override: true}
    ]
  end
end
