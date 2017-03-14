# This file is responsible for configuring your application
# and its dependencies with the aid of the Mix.Config module.
use Mix.Config

app = :dexy_plugin_kv

config app, DexyPluginKV, [
  adapter: DexyPluginKV.Adapters.Riak
]

config app, DexyPluginKV.Adapters.Riak, [
  userdata_bucket_type: "userdata",
  userdata_content_type: "application/dexyml",
  userdata_index: "idx_userdata",
]

config :pooler, :pools, [
  [
    name: DexyPluginKV.Adapters.Riak,
    group: app,
    max_count: 10,
    init_count: 1, 
    start_mfa: {DexyPluginKV.Adapters.Riak, :start_link, []} 
  ]
]

config :logger,
  backends: [:console]

config :logger, :console, [
  level: :debug,
  format: "$time $metadata[$level] $message\n",
  metadata: [:module]
]
