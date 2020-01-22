use Mix.Config

config :weaver, :twitter,
  client_module: Weaver.ExTwitter.Mock,
  api_count: 2,
  api_take: 2
