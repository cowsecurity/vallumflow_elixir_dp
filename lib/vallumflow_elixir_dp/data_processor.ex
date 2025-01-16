defmodule VallumflowElixirDp.DataProcessor do
  use GenServer
  require Logger
  require Record

  @topic "dashboard-topic"
  @group_id "vallumflow_group_1"
  @client :brod_client_1

  Record.defrecord(
    :kafka_message,
    Record.extract(:kafka_message, from_lib: "brod/include/brod.hrl")
  )

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  # Server Callbacks
  @impl true
  def init(state) do
    Logger.info("Initializing Data Processing service...")

    # Configure consumer group settings
    group_config = [
      offset_commit_policy: :commit_to_kafka_v2,
      offset_commit_interval_seconds: 5,
      offset_reset_policy: :latest,
      rejoin_delay_seconds: 2
    ]

    # Configure consumer settings
    consumer_config = [
      begin_offset: :earliest,
      max_bytes: 1_000_000,
      max_wait_time: 500,
      auto_start_producers: true
    ]

    Logger.info("Starting consumer with group_id: #{@group_id}")

    # Start the group subscriber
    case :brod.start_link_group_subscriber(
           @client,
           @group_id,
           [@topic],
           group_config,
           consumer_config,
           __MODULE__,
           []
         ) do
      {:ok, pid} ->
        Logger.info("Successfully started group subscriber with PID: #{inspect(pid)}")
        Process.monitor(pid)
        {:ok, Map.put(state, :subscriber_pid, pid)}

      {:error, reason} ->
        Logger.error("Failed to start group subscriber: #{inspect(reason)}")
        {:stop, reason}
    end
  end

  # Required callback for brod group subscriber
  def init(_group_id, _init_arg) do
    {:ok, %{}}
  end

  # Handle incoming messages
  def handle_message(_topic, partition, msg, state) do
    try do
      value = kafka_message(msg, :value)
      offset = kafka_message(msg, :offset)
      key = kafka_message(msg, :key)

      Logger.info("Processing message: partition=#{partition}, offset=#{offset}, key=#{key}")

      case Jason.decode(value) do
        {:ok, decoded_value} ->
          # Extract and log relevant information
          agent_id = decoded_value["agent_id"]
          dashboard_json = decoded_value["dashboard_json"]
          node_id = decoded_value["node_id"]
          org_id = decoded_value["org_id"]

          Logger.info("""
          Processed Message:
          Agent ID: #{agent_id}
          Node ID: #{node_id}
          Org ID: #{org_id}
          Domains count: #{length(dashboard_json)}
          Domains: #{Enum.join(dashboard_json, ", ")}
          """)

          # Let brod handle offset management
          {:ok, :ack, state}

        {:error, error} ->
          Logger.error("Failed to decode JSON: #{inspect(error)}")
          Logger.error("Raw value: #{inspect(value)}")
          {:ok, :ack, state}
      end
    rescue
      e ->
        Logger.error("Error processing message: #{inspect(e)}")
        Logger.error("Stack trace: #{Exception.format_stacktrace(__STACKTRACE__)}")
        {:ok, :ack, state}
    end
  end

  # GenServer callback implementations
  @impl true
  def handle_info({:DOWN, _ref, :process, pid, reason}, %{subscriber_pid: pid} = state) do
    Logger.warning("Kafka subscriber #{inspect(pid)} went down: #{inspect(reason)}")
    Process.send_after(self(), :restart_subscriber, 5000)
    {:noreply, Map.delete(state, :subscriber_pid)}
  end

  @impl true
  def handle_info(:restart_subscriber, state) do
    Logger.info("Attempting to restart Kafka subscriber...")
    {:ok, new_state} = init(state)
    {:noreply, new_state}
  end

  # Catch-all for unknown messages
  @impl true
  def handle_info(msg, state) do
    Logger.warning("Received unexpected message: #{inspect(msg)}")
    {:noreply, state}
  end
end
