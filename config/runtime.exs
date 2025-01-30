import Config
import Dotenvy

source!([".env"])

# Get env variables
mongodb_db = env!("MONGODB_DB")
kafka_broker = env!("KAFKA_BROKER")
kafka_port = env!("KAKFA_PORT")
mongodb_collection = env!("MONGODB_COLLECTION")
mongodb_username = env!("MONGODB_USERNAME")
mongodb_password = env!("MONGODB_PASSWORD")
mongo_uri = env!("MONGO_URI")

# Convert string to integer with default value
check_interval =
  try do
    case System.get_env("CHECK_INTERVAL_MS") do
      nil -> 1000
      val -> String.to_integer(val)
    end
  rescue
    _ -> 1000
  end

pool_size =
  try do
    case System.get_env("MONGODB_POOL_SIZE") do
      nil -> 10
      val -> String.to_integer(val)
    end
  rescue
    _ -> 10
  end

# Convert port to integer
kafka_port_int =
  try do
    String.to_integer(kafka_port)
  rescue
    # Default Kafka port if conversion fails
    _ -> 9092
  end

config :brod,
  clients: [
    brod_client_1: [
      endpoints: [{kafka_broker, kafka_port_int}],
      auto_start_producers: true,
      default_producer_config: [],
      reconnect_cool_down_seconds: 10
    ]
  ]

config :vallumflow_elixir_dp,
  database: mongodb_db,
  collection: mongodb_collection,
  username: mongodb_username,
  password: mongodb_password,
  mongo_uri: mongo_uri,
  check_interval: check_interval,
  pool_size: pool_size,
  kafka_broker: kafka_broker,
  kafka_port: kafka_port_int

config :logger, :console,
  format: {VallumflowElixirDp.JsonFormatter, :format},
  metadata: [
    module: nil,
    function: nil,
    line: nil,
    pid: nil,
    request_id: nil,
    trace_id: nil,
    document_id: nil,
    modified_count: nil
  ]
