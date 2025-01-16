import Config

config :elixir, :time_zone_database, Tzdata.TimeZoneDatabase

config :tzdata, :data_dir, "./priv/tzdata"

config :brod,
  clients: [
    brod_client_1: [
      endpoints: [{"127.0.0.1", 9092}],
      auto_start_producers: true,
      default_producer_config: [],
      reconnect_cool_down_seconds: 10
    ]
  ]
