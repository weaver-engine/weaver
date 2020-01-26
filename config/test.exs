use Mix.Config

config :weaver, :twitter,
  client_module: Weaver.ExTwitter.Mock,
  api_count: 2,
  api_take: 2

config :weaver, :dgraph,
  host: System.get_env("DGRAPH_HOST") || "localhost",
  port: 9081,
  show_sensitive_data_on_connection_error: true
