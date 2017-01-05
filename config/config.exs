# This file is responsible for configuring your application
# and its dependencies with the aid of the Mix.Config module.
use Mix.Config

g_APP = :dexy_plugin_kv

config g_APP, DexyPluginKV, [
  adapter: DexyPluginKV.Adapters.Riak
]

config g_APP, DexyPluginKV.Adapters.Riak, [
  userdata_bucket_type: "userdata",
  userdata_content_type: "application/dexyml",
  userdata_index: "idx_userdata",
]

config :pooler, :pools, [
  [
    name: DexyPluginKV.Adapters.Riak,
    group: g_APP,
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

# This configuration is loaded before any dependency and is restricted
# to this project. If another project depends on this project, this
# file won't be loaded nor affect the parent project. For this reason,
# if you want to provide default values for your application for
# 3rd-party users, it should be done in your "mix.exs" file.

# You can configure for your application as:
#
#     config :dexy_plugin_kv, key: :value
#
# And access this configuration in your application as:
#
#     Application.get_env(:dexy_plugin_kv, :key)
#
# Or configure a 3rd-party app:
#
#     config :logger, level: :info
#

# It is also possible to import configuration files, relative to this
# directory. For example, you can emulate configuration per environment
# by uncommenting the line below and defining dev.exs, test.exs and such.
# Configuration from the imported file will override the ones defined
# here (which is why it is important to import them last).
#
#     import_config "#{Mix.env}.exs"
