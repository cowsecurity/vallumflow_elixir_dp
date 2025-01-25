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
         } = _message,
         state
       ) do
    processed_data = preprocess_dashboard_json(dashboard_json)

    Logger.info("""
    Processed Message:
    Agent ID: #{agent_id}
    Node ID: #{node_id}
    Org ID: #{org_id}
    Processed Data: #{inspect(processed_data)}
    """)

    {:ok, :ack, state}
  end

  defp process_message(message, state) do
    Logger.warning("Message does not match expected structure: #{inspect(message)}")
    {:ok, :ack, state}
  end

  # Preprocess dashboard JSON before parsing
  defp preprocess_dashboard_json(dashboard_json) when is_binary(dashboard_json) do
    case Jason.decode(dashboard_json) do
      {:ok, parsed_json} ->
        parse_security_data(parsed_json)

      {:error, error} ->
        Logger.error("Faield to parse binary dashboard_json: #{inspect(error)}")
        %{}
    end
  end

  defp preprocess_dashboard_json(dashboard_json) when is_list(dashboard_json) do
    case Enum.join(dashboard_json)
         |> String.replace("\r\n", "")
         |> String.trim()
         |> Jason.decode() do
      {:ok, parsed_json} ->
        parse_security_data(parsed_json)

      {:error, error} ->
        Logger.error("Failed to parse dashboard_json: #{inspect(error)}")
        %{}
    end
  end

  defp preprocess_dashboard_json(dashboard_json) when is_map(dashboard_json) do
    parse_security_data(dashboard_json)
  end

  defp preprocess_dashboard_json(data) do
    Logger.error("Unexpected dashboard_json format: #{inspect(data)}")
    %{}
  end

  # Main parser for security data
  defp parse_security_data(data) when is_map(data) do
    findings = extract_findings_from_structure(data)

    %{
      "metadata" => extract_metadata(data),
      "summary" => generate_summary(findings),
      "findings" => findings,
      "analysis" => analyze_findings(findings)
    }
  end

  # Extract findings from any data structure
  defp extract_findings_from_structure(data) do
    cond do
      has_results_array?(data) ->
        get_results_array(data)
        |> Enum.flat_map(&normalize_finding/1)

      has_direct_findings?(data) ->
        [normalize_finding(data)]

      has_nested_findings?(data) ->
        extract_nested_findings(data)

      true ->
        []
    end
    # Only reject nil values
    |> Enum.reject(&is_nil/1)
  end

  # Pattern detection helpers
  defp has_results_array?(data) do
    result_keys = ["results", "findings", "matches", "vulnerabilities", "issues"]

    Enum.any?(result_keys, fn key ->
      case data[key] do
        items when is_list(items) -> Enum.any?(items, &has_security_indicators?/1)
        _ -> false
      end
    end)
  end

  defp get_results_array(data) do
    result_keys = ["results", "findings", "matches", "vulnerabilities", "issues"]

    Enum.find_value(result_keys, [], fn key ->
      case data[key] do
        items when is_list(items) ->
          if Enum.any?(items, &has_security_indicators?/1), do: items, else: []

        _ ->
          []
      end
    end)
  end

  defp has_direct_findings?(data) when is_map(data), do: has_security_indicators?(data)
  defp has_direct_findings?(_), do: false

  defp has_nested_findings?(data) when is_map(data) do
    Enum.any?(data, fn {_, v} ->
      case v do
        v when is_map(v) -> has_security_indicators?(v)
        v when is_list(v) -> Enum.any?(v, &has_security_indicators?/1)
        _ -> false
      end
    end)
  end

  defp has_nested_findings?(_), do: false

  defp has_security_indicators?(data) when is_map(data) do
    keys = Map.keys(data) |> Enum.map(&String.downcase/1)

    values =
      Map.values(data)
      |> Enum.map(fn
        v when is_binary(v) -> String.downcase(v)
        v when is_number(v) -> to_string(v)
        v when is_list(v) -> ""
        v when is_map(v) -> ""
        nil -> ""
        v -> inspect(v)
      end)

    security_indicators = [
      "vulnerability",
      "vuln",
      "cve",
      "finding",
      "issue",
      "alert",
      "severity",
      "priority",
      "risk",
      "impact",
      "cwe",
      "cvss",
      "owasp",
      "affected",
      "remediation",
      "solution"
    ]

    Enum.any?(security_indicators, fn indicator ->
      Enum.any?(keys, &String.contains?(&1, indicator)) ||
        Enum.any?(values, &String.contains?(&1, indicator))
    end)
  end

  defp has_security_indicators?(_), do: false

  # Extract nested findings recursively
  defp extract_nested_findings(data) when is_map(data) do
    direct_findings = if has_security_indicators?(data), do: [normalize_finding(data)], else: []

    nested_findings =
      Enum.flat_map(data, fn
        {_, v} when is_map(v) -> extract_nested_findings(v)
        {_, v} when is_list(v) -> Enum.flat_map(v, &extract_nested_findings/1)
        _ -> []
      end)

    direct_findings ++ nested_findings
  end

  defp extract_nested_findings(data) when is_list(data) do
    Enum.flat_map(data, &extract_nested_findings/1)
  end

  defp extract_nested_findings(_), do: []

  # Normalize finding into standard format
  defp normalize_finding(data) when is_map(data) do
    finding =
      %{
        "id" => extract_id(data),
        "name" => extract_name(data),
        "description" => extract_description(data),
        "severity" => extract_severity(data),
        "location" => extract_location(data),
        "metadata" => extract_finding_metadata(data),
        "details" => extract_details(data)
      }
      |> add_if_found("cvss", extract_cvss(data))
      |> add_if_found("affected_components", extract_affected_components(data))
      |> add_if_found("remediation", extract_remediation(data))
      |> add_if_found("references", extract_references(data))
      |> clean_nil_values()

    if finding["id"] || finding["name"] || finding["description"], do: finding, else: nil
  end

  defp normalize_finding(_), do: nil

  # Extract various components
  defp extract_metadata(data) when is_map(data) do
    metadata_keys = [
      "version",
      "timestamp",
      "created",
      "scanner",
      "tool",
      "source",
      "target",
      "environment",
      "host"
    ]

    data
    |> Enum.filter(fn {k, v} ->
      Enum.any?(metadata_keys, &String.contains?(String.downcase(k), &1)) and
        not is_nil(v)
    end)
    |> Enum.into(%{})
  end

  defp extract_metadata(_), do: %{}

  defp extract_id(data) do
    find_first_value(data, [
      "id",
      "vulnerabilityID",
      "cve",
      "check_id",
      "rule_id",
      "issue_id",
      "finding_id",
      "alert_id"
    ])
  end

  defp extract_name(data) do
    find_first_value(data, [
      "name",
      "title",
      "summary",
      "alert",
      "rule_name",
      "vulnerability_name",
      "issue_name"
    ])
  end

  defp extract_description(data) do
    find_first_value(data, [
      "description",
      "details",
      "message",
      "info",
      "explanation",
      "summary"
    ])
  end

  defp extract_severity(data) do
    severity = find_first_value(data, ["severity", "priority", "risk_level", "impact"])
    normalize_severity_level(severity)
  end

  defp normalize_severity_level(level) when is_binary(level) do
    level = String.downcase(level)

    cond do
      Enum.member?(["critical", "crit"], level) -> "critical"
      Enum.member?(["high", "h"], level) -> "high"
      Enum.member?(["medium", "med", "moderate", "m"], level) -> "medium"
      Enum.member?(["low", "l"], level) -> "low"
      Enum.member?(["negligible", "info", "information", "i"], level) -> "info"
      true -> "unknown"
    end
  end

  defp normalize_severity_level(_), do: "unknown"

  defp extract_location(data) do
    location =
      %{}
      |> add_if_found("file", find_first_value(data, ["file", "path", "filename"]))
      |> add_if_found("line", find_first_value(data, ["line", "line_number", "lineno"]))
      |> add_if_found("position", extract_position(data))

    if map_size(location) > 0, do: location, else: nil
  end

  defp extract_position(data) do
    start_data = find_nested_value(data, ["start", "position", "location"])
    end_data = find_nested_value(data, ["end", "position", "location"])

    case {start_data, end_data} do
      {nil, nil} ->
        nil

      {start, end_pos} ->
        position =
          %{}
          |> maybe_add_position("start", start)
          |> maybe_add_position("end", end_pos)

        if map_size(position) > 0, do: position, else: nil
    end
  end

  defp extract_cvss(data) do
    cvss_data = find_nested_value(data, ["cvss", "metrics", "score", "risk_score"])

    case cvss_data do
      score when is_number(score) -> %{"score" => score}
      map when is_map(map) -> clean_nil_values(map)
      _ -> nil
    end
  end

  defp extract_finding_metadata(data) do
    metadata_fields = [
      "type",
      "category",
      "technology",
      "confidence",
      "cwe",
      "owasp",
      "impact",
      "likelihood",
      "tags",
      "labels",
      "source"
    ]

    metadata =
      Enum.reduce(metadata_fields, %{}, fn field, acc ->
        case find_nested_value(data, [field]) do
          nil -> acc
          value -> Map.put(acc, field, value)
        end
      end)

    if map_size(metadata) > 0, do: metadata, else: nil
  end

  defp extract_details(data) do
    details =
      %{}
      |> add_if_found("technical", extract_technical_details(data))
      |> add_if_found("security", extract_security_details(data))
      |> add_if_found("context", extract_context_details(data))

    if map_size(details) > 0, do: details, else: nil
  end

  defp extract_technical_details(data) do
    fields = ["type", "category", "technology", "language", "confidence"]
    extract_field_group(data, fields)
  end

  defp extract_security_details(data) do
    fields = ["cwe", "owasp", "impact", "likelihood"]
    extract_field_group(data, fields)
  end

  defp extract_context_details(data) do
    fields = ["tags", "labels", "categories"]
    extract_field_group(data, fields)
  end

  defp extract_field_group(data, fields) do
    result =
      Enum.reduce(fields, %{}, fn field, acc ->
        case find_nested_value(data, [field]) do
          nil -> acc
          value -> Map.put(acc, field, value)
        end
      end)

    if map_size(result) > 0, do: result, else: nil
  end

  defp extract_affected_components(data) do
    components =
      []
      |> extract_packages(data)
      |> extract_files(data)
      |> extract_urls(data)

    if length(components) > 0, do: components, else: nil
  end

  defp extract_packages(components, data) do
    case find_nested_value(data, ["package", "packages", "module", "component"]) do
      packages when is_list(packages) ->
        packages
        |> Enum.map(&normalize_component(&1, "package"))
        |> Enum.reject(&is_nil/1)
        |> Kernel.++(components)

      package when is_map(package) ->
        case normalize_component(package, "package") do
          nil -> components
          comp -> [comp | components]
        end

      _ ->
        components
    end
  end

  defp extract_files(components, data) do
    case find_nested_value(data, ["file", "files", "path", "filepath"]) do
      files when is_list(files) ->
        files
        |> Enum.map(&normalize_component(&1, "file"))
        |> Enum.reject(&is_nil/1)
        |> Kernel.++(components)

      file when is_binary(file) ->
        [%{"type" => "file", "path" => file} | components]

      file when is_map(file) ->
        case normalize_component(file, "file") do
          nil -> components
          comp -> [comp | components]
        end

      _ ->
        components
    end
  end

  defp extract_urls(components, data) do
    case find_nested_value(data, ["url", "urls", "endpoint", "link"]) do
      urls when is_list(urls) ->
        urls
        |> Enum.map(&normalize_component(&1, "url"))
        |> Enum.reject(&is_nil/1)
        |> Kernel.++(components)

      url when is_binary(url) ->
        [%{"type" => "url", "url" => url} | components]

      url when is_map(url) ->
        case normalize_component(url, "url") do
          nil -> components
          comp -> [comp | components]
        end

      _ ->
        components
    end
  end

  defp normalize_component(component, type) when is_map(component) do
    base = %{"type" => type}

    component
    |> Enum.reduce(base, fn {k, v}, acc ->
      if not is_nil(v), do: Map.put(acc, k, v), else: acc
    end)
    |> clean_nil_values()
  end

  defp normalize_component(component, type) when is_binary(component) do
    case type do
      "file" -> %{"type" => type, "path" => component}
      "url" -> %{"type" => type, "url" => component}
      "package" -> %{"type" => type, "name" => component}
      _ -> nil
    end
  end

  defp normalize_component(_, _), do: nil

  defp extract_remediation(data) do
    case find_nested_value(data, ["remediation", "fix", "solution", "recommendation"]) do
      remedy when is_binary(remedy) ->
        %{"description" => remedy}

      remedy when is_map(remedy) ->
        clean_nil_values(remedy)

      _ ->
        nil
    end
  end

  defp extract_references(data) do
    case find_nested_value(data, ["references", "urls", "links", "see_also"]) do
      refs when is_list(refs) ->
        refs
        |> Enum.map(fn
          ref when is_binary(ref) -> ref
          %{"url" => url} -> url
          %{"href" => href} -> href
          _ -> nil
        end)
        |> Enum.reject(&is_nil/1)

      _ ->
        nil
    end
  end

  # Generate a summary of all findings
  defp generate_summary(findings) do
    %{
      "total_findings" => length(findings),
      "severity_distribution" => count_by_key(findings, "severity"),
      "component_distribution" => count_by_component_type(findings),
      "remediation_available" =>
        Enum.count(findings, fn
          {key, _value} -> key == "remediation"
          finding when is_map(finding) -> Map.has_key?(finding, "remediation")
          _ -> false
        end),
      "findings_with_references" =>
        Enum.count(findings, fn
          {key, _value} -> key == "references"
          finding when is_map(finding) -> Map.has_key?(finding, "references")
          _ -> false
        end)
    }
  end

  defp count_by_key(findings, key) do
    findings
    |> Enum.reduce(%{}, fn finding, acc ->
      case finding do
        finding when is_map(finding) ->
          value = finding[key] || "unknown"
          Map.update(acc, value, 1, &(&1 + 1))

        {^key, value} when is_list(value) ->
          Enum.reduce(value, acc, fn item, inner_acc ->
            Map.update(inner_acc, item["type"] || "unknown", 1, &(&1 + 1))
          end)

        _ ->
          acc
      end
    end)
  end

  defp count_by_component_type(findings) do
    findings
    # Ensure we have a list
    |> List.wrap()
    |> Enum.reduce(%{}, fn
      {"affected_components", components}, acc when is_list(components) ->
        # Handle tuple format
        Enum.reduce(components, acc, fn comp, inner_acc ->
          type = comp["type"] || "unknown"
          Map.update(inner_acc, type, 1, &(&1 + 1))
        end)

      %{"affected_components" => components}, acc when is_list(components) ->
        # Handle map format
        Enum.reduce(components, acc, fn comp, inner_acc ->
          type = comp["type"] || "unknown"
          Map.update(inner_acc, type, 1, &(&1 + 1))
        end)

      _, acc ->
        acc
    end)
  end

  # Analyze findings
  defp analyze_findings(findings) do
    %{
      "severity_analysis" => analyze_severity(findings),
      "component_analysis" => analyze_components(findings),
      "metadata_analysis" => analyze_metadata(findings)
    }
  end

  defp analyze_severity(findings) do
    severity_counts = count_by_key(findings, "severity")
    total = length(findings)
    critical_count = Map.get(severity_counts, "critical", 0)
    high_count = Map.get(severity_counts, "high", 0)

    %{
      "distribution" => severity_counts,
      "high_critical_percentage" => percentage_of_total(critical_count + high_count, total)
    }
  end

  defp analyze_components(findings) do
    components =
      Enum.flat_map(findings, fn
        {"affected_components", comps} when is_list(comps) -> comps
        finding when is_map(finding) -> finding["affected_components"] || []
        _ -> []
      end)

    %{
      "total_affected" => length(components),
      "type_distribution" => count_by_key(components, "type"),
      "unique_components" =>
        components
        |> Enum.uniq_by(&(&1["path"] || &1["name"] || &1["url"]))
        |> length()
    }
  end

  defp analyze_metadata(findings) do
    findings
    |> Enum.flat_map(fn
      # Keep tuple data as key-value pairs
      {key, value} -> [{key, value}]
      finding when is_map(finding) -> Map.to_list(finding["metadata"] || %{})
      _ -> []
    end)
    |> Enum.reduce(%{}, fn {key, value}, acc ->
      Map.update(acc, key, [value], &[value | &1])
    end)
    |> Enum.map(fn {key, values} -> {key, Enum.frequencies(values)} end)
    |> Enum.into(%{})
  end

  # Helper functions
  defp percentage_of_total(count, total) when total > 0, do: count / total * 100
  defp percentage_of_total(_, _), do: 0

  defp maybe_add_position(map, _key, nil), do: map

  defp maybe_add_position(map, key, position) when is_map(position) do
    cleaned_position =
      position
      |> Enum.filter(fn {_, v} -> not is_nil(v) end)
      |> Enum.into(%{})

    if map_size(cleaned_position) > 0, do: Map.put(map, key, cleaned_position), else: map
  end

  defp maybe_add_position(map, _key, _position), do: map

  defp add_if_found(map, _key, nil), do: map
  defp add_if_found(map, _key, value) when map_size(value) == 0, do: map
  defp add_if_found(map, key, value), do: Map.put(map, key, value)

  defp find_nested_value(data, keys) when is_map(data) do
    Enum.find_value(keys, fn search_key ->
      Enum.find_value(data, fn {key, value} ->
        if String.contains?(String.downcase(key), String.downcase(search_key)), do: value
      end)
    end)
  end

  defp find_nested_value(_, _), do: nil

  defp find_first_value(data, keys) do
    Enum.find_value(keys, fn key -> find_nested_value(data, [key]) end)
  end

  defp clean_nil_values(map) when is_map(map) do
    map
    |> Enum.reject(fn {_, v} -> is_nil(v) or (is_map(v) and map_size(v) == 0) end)
    |> Enum.map(fn {k, v} ->
      {k,
       case v do
         v when is_map(v) -> clean_nil_values(v)
         v when is_list(v) -> Enum.map(v, &clean_nil_values/1)
         _ -> v
       end}
    end)
    |> Enum.into(%{})
  end

  defp clean_nil_values(value), do: value

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
