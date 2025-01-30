import Config

config :elixir, :time_zone_database, Tzdata.TimeZoneDatabase

config :tzdata, :data_dir, "./priv/tzdata"
