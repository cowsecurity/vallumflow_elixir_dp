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

    group_config = [
      offset_commit_policy: :commit_to_kafka_v2,
      offset_commit_interval_seconds: 5,
      offset_reset_policy: :latest,
      rejoin_delay_seconds: 2
    ]

    consumer_config = [
      begin_offset: :earliest,
      max_bytes: 1_000_000,
      max_wait_time: 500,
      auto_start_producers: true
    ]

    Logger.info("Starting consumer with group_id: #{@group_id}")

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
          process_message(decoded_value, state)

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

  # Process incoming message with standard structure
  defp process_message(
         %{
           "agent_id" => agent_id,
           "dashboard_json" => dashboard_json,
           "node_id" => node_id,
           "org_id" => org_id
         } = message,
         state
       ) do
    processed_json = preprocess_dashboard_json(dashboard_json)
    visualization_data = extract_visualization_data(processed_json)

    Logger.info("""
    Processed Message:
    Agent ID: #{agent_id}
    Node ID: #{node_id}
    Org ID: #{org_id}
    Visualization Data: #{inspect(visualization_data)}
    """)

    {:ok, :ack, state}
  end

  defp process_message(message, state) do
    Logger.warning("Message does not match expected structure: #{inspect(message)}")
    {:ok, :ack, state}
  end

  # Pre-process dashboard JSON before sending to extractor
  defp preprocess_dashboard_json(dashboard_json) when is_list(dashboard_json) do
    # Join all strings and parse as a single JSON object
    case Enum.join(dashboard_json)
         # Remove any new lines
         |> String.replace("\r\n", "")
         # Remove any extra whitespace
         |> String.trim()
         |> Jason.decode() do
      {:ok, parsed_json} ->
        parsed_json

      {:error, error} ->
        Logger.error("Failed to pre parse dashboard_json #{inspect(error)}")
        %{}
    end
  end

  # Handle where input is already a map
  defp preprocess_dashboard_json(dashboard_json) when is_map(dashboard_json), do: dashboard_json

  # Handle any other unexpected format
  defp preprocess_dashboard_json(dashboard_json) do
    Logger.error("Unexpected dashboard_json format: #{inspect(dashboard_json)}")
    %{}
  end

  # Extract data suitable for visualization from any tool output
  defp extract_visualization_data(data) when is_map(data) do
    # Start with basic metadata
    metadata = extract_metadata(data)

    # Extract various types of data that could be visualized
    %{
      metadata: metadata,
      metrics: extract_metrics(data),
      counts: extract_counts(data),
      categories: extract_categories(data),
      trends: extract_trends(data),
      status_distribution: extract_status_distribution(data),
      severity_distribution: extract_severity_distribution(data),
      temporal_data: extract_temporal_data(data)
    }
  end

  defp extract_visualization_data(data) when is_list(data) do
    # Handle array data by processing each item
    processed_items = Enum.map(data, &extract_visualization_data/1)

    # Merge similar data points
    merge_visualization_data(processed_items)
  end

  defp extract_visualization_data(data) do
    Logger.warning("Unexpected data format for visualization: #{inspect(data)}")
    %{raw_data: data}
  end

  # Extract common metadata fields
  defp extract_metadata(data) do
    common_fields = [
      "version",
      "timestamp",
      "tool_version",
      "tool_name",
      "scan_id",
      "report_id",
      "environment",
      "source"
    ]

    data
    |> Map.take(common_fields)
    |> Enum.reject(fn {_, v} -> is_nil(v) end)
    |> Enum.into(%{})
  end

  # Extract numeric metrics suitable for graphs
  defp extract_metrics(data) do
    data
    |> Enum.filter(fn {_, v} -> is_number(v) end)
    |> Enum.into(%{})
  end

  # Extract count data (usually integers)
  defp extract_counts(data) do
    data
    |> Enum.reduce(%{}, fn {key, value}, acc ->
      cond do
        is_list(value) ->
          Map.put(acc, "#{key}_count", length(value))

        is_map(value) and Map.has_key?(value, "count") ->
          Map.put(acc, key, value["count"])

        true ->
          acc
      end
    end)
  end

  # Extract category-based data
  defp extract_categories(data) do
    data
    |> Enum.reduce(%{}, fn {key, value}, acc ->
      cond do
        is_list(value) ->
          categories = categorize_list_items(value)
          if map_size(categories) > 0, do: Map.put(acc, key, categories), else: acc

        is_map(value) ->
          categories = categorize_map_items(value)
          if map_size(categories) > 0, do: Map.put(acc, key, categories), else: acc

        true ->
          acc
      end
    end)
  end

  # Extract trend data (time-based or sequential)
  defp extract_trends(data) do
    data
    |> Enum.reduce(%{}, fn {key, value}, acc ->
      cond do
        is_list(value) and has_temporal_data?(value) ->
          Map.put(acc, key, extract_temporal_sequence(value))

        is_map(value) and has_trend_data?(value) ->
          Map.put(acc, key, extract_trend_points(value))

        true ->
          acc
      end
    end)
  end

  defp extract_temporal_sequence(items) when is_list(items) do
    items
    |> Enum.map(fn item ->
      timestamp = extract_timestamp(item)
      value = extract_sequence_value(item)

      %{
        timestamp: timestamp,
        value: value
      }
    end)
    |> Enum.reject(fn %{timestamp: ts, value: v} -> is_nil(ts) or is_nil(v) end)
    |> Enum.sort_by(fn %{timestamp: ts} -> ts end)
  end

  defp extract_trend_points(data) when is_map(data) do
    trend_keys = ["trend", "history", "sequence", "timeline"]

    case Enum.find(trend_keys, fn key -> Map.has_key?(data, key) end) do
      nil ->
        []

      key ->
        case data[key] do
          points when is_list(points) ->
            points
            |> Enum.map(&normalize_trend_point/1)
            |> Enum.reject(&is_nil/1)

          _ ->
            []
        end
    end
  end

  defp normalize_trend_point(point) when is_map(point) do
    case {extract_timestamp(point), extract_point_value(point)} do
      {nil, _} -> nil
      {_, nil} -> nil
      {timestamp, value} -> %{timestamp: timestamp, value: value}
    end
  end

  defp normalize_trend_point(_), do: nil

  defp extract_sequence_value(item) when is_map(item) do
    value_keys = ["value", "count", "total", "amount", "score"]
    Enum.find_value(value_keys, fn key -> item[key] end)
  end

  defp extract_sequence_value(item) when is_number(item), do: item
  defp extract_sequence_value(_), do: nil

  defp extract_point_value(point) when is_map(point) do
    value_keys = ["value", "count", "measure", "score", "total"]
    Enum.find_value(value_keys, fn key -> point[key] end)
  end

  defp extract_point_value(point) when is_number(point), do: point
  defp extract_point_value(_), do: nil

  # Extract status-based distributions
  defp extract_status_distribution(data) do
    status_fields = ["status", "state", "result", "outcome"]

    data
    |> Enum.reduce(%{}, fn {key, value}, acc ->
      cond do
        key in status_fields and is_binary(value) ->
          Map.update(
            acc,
            "status_distribution",
            %{value => 1},
            &Map.update(&1, value, 1, fn x -> x + 1 end)
          )

        is_list(value) ->
          distribution = count_status_in_list(value)

          if map_size(distribution) > 0,
            do: Map.put(acc, "#{key}_status", distribution),
            else: acc

        true ->
          acc
      end
    end)
  end

  # Extract severity-based distributions
  defp extract_severity_distribution(data) do
    severity_fields = ["severity", "priority", "risk_level", "impact"]

    data
    |> Enum.reduce(%{}, fn {key, value}, acc ->
      cond do
        key in severity_fields and is_binary(value) ->
          Map.update(
            acc,
            "severity_distribution",
            %{value => 1},
            &Map.update(&1, value, 1, fn x -> x + 1 end)
          )

        is_list(value) ->
          distribution = count_severity_in_list(value)

          if map_size(distribution) > 0,
            do: Map.put(acc, "#{key}_severity", distribution),
            else: acc

        true ->
          acc
      end
    end)
  end

  # Extract time-based data
  defp extract_temporal_data(data) do
    data
    |> Enum.reduce(%{}, fn {key, value}, acc ->
      cond do
        is_list(value) and has_temporal_data?(value) ->
          Map.put(acc, key, group_by_time(value))

        is_map(value) and has_timestamp_field?(value) ->
          Map.put(acc, key, extract_temporal_point(value))

        true ->
          acc
      end
    end)
  end

  # Helper functions for data extraction
  defp categorize_list_items(items) when is_list(items) do
    items
    |> Enum.reduce(%{}, fn item, acc ->
      category = determine_category(item)
      Map.update(acc, category, 1, &(&1 + 1))
    end)
  end

  defp categorize_map_items(items) when is_map(items) do
    items
    |> Enum.reduce(%{}, fn {key, value}, acc ->
      Map.put(acc, key, categorize_value(value))
    end)
  end

  defp determine_category(item) when is_map(item) do
    cond do
      Map.has_key?(item, "type") -> item["type"]
      Map.has_key?(item, "category") -> item["category"]
      true -> "unknown"
    end
  end

  defp determine_category(_), do: "unknown"

  defp categorize_value(value) when is_binary(value), do: value
  defp categorize_value(value) when is_number(value), do: "numeric"
  defp categorize_value(value) when is_boolean(value), do: "boolean"
  defp categorize_value(value) when is_list(value), do: "array"
  defp categorize_value(value) when is_map(value), do: "object"
  defp categorize_value(_), do: "unknown"

  defp has_temporal_data?(items) when is_list(items) do
    Enum.any?(items, &has_timestamp_field?/1)
  end

  defp has_timestamp_field?(item) when is_map(item) do
    Enum.any?(["timestamp", "date", "time", "created_at", "updated_at"], &Map.has_key?(item, &1))
  end

  defp has_timestamp_field?(_), do: false

  defp has_trend_data?(data) when is_map(data) do
    Enum.any?(["trend", "history", "sequence", "timeline"], &Map.has_key?(data, &1))
  end

  defp has_trend_data?(_), do: false

  defp count_status_in_list(items) when is_list(items) do
    items
    |> Enum.reduce(%{}, fn item, acc ->
      status = extract_status(item)
      if status, do: Map.update(acc, status, 1, &(&1 + 1)), else: acc
    end)
  end

  defp extract_status(item) when is_map(item) do
    Enum.find_value(["status", "state", "result"], fn key -> item[key] end)
  end

  defp extract_status(_), do: nil

  defp count_severity_in_list(items) when is_list(items) do
    items
    |> Enum.reduce(%{}, fn item, acc ->
      severity = extract_severity(item)
      if severity, do: Map.update(acc, severity, 1, &(&1 + 1)), else: acc
    end)
  end

  defp extract_severity(item) when is_map(item) do
    Enum.find_value(["severity", "priority", "risk_level"], fn key -> item[key] end)
  end

  defp extract_severity(_), do: nil

  defp group_by_time(items) when is_list(items) do
    items
    |> Enum.group_by(&extract_timestamp/1)
    |> Enum.sort_by(fn {timestamp, _} -> timestamp end)
    |> Enum.map(fn {timestamp, group} -> %{timestamp: timestamp, count: length(group)} end)
  end

  defp extract_timestamp(item) when is_map(item) do
    Enum.find_value(["timestamp", "date", "created_at"], fn key -> item[key] end)
  end

  defp extract_timestamp(_), do: nil

  defp extract_temporal_point(data) when is_map(data) do
    timestamp = extract_timestamp(data)
    if timestamp, do: Map.put(data, "timestamp", timestamp), else: data
  end

  defp merge_visualization_data(items) when is_list(items) do
    Enum.reduce(items, %{}, fn item, acc ->
      Map.merge(acc, item, fn _k, v1, v2 -> merge_values(v1, v2) end)
    end)
  end

  defp merge_values(v1, v2) when is_map(v1) and is_map(v2) do
    Map.merge(v1, v2, fn _k, v1, v2 -> merge_values(v1, v2) end)
  end

  defp merge_values(v1, v2) when is_list(v1) and is_list(v2), do: v1 ++ v2
  defp merge_values(v1, v2) when is_number(v1) and is_number(v2), do: v1 + v2
  defp merge_values(_, v2), do: v2

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

  @impl true
  def handle_info(msg, state) do
    Logger.warning("Received unexpected message: #{inspect(msg)}")
    {:noreply, state}
  end
end
