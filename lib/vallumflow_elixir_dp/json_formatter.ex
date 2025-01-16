defmodule VallumflowElixirDp.JsonFormatter do
  def format(level, message, timestamp, metadata) do
    {date, {hour, minute, second, millisecond}} = timestamp

    timestamp =
      NaiveDateTime.from_erl!({date, {hour, minute, second}}, {millisecond * 1000, 3})
      |> DateTime.from_naive!("Etc/UTC")
      |> DateTime.to_iso8601()

    log_entry =
      %{
        "time" => timestamp,
        "level" => format_level(level),
        "msg" => IO.iodata_to_binary(message)
      }
      |> add_metadata(metadata)
      |> Jason.encode!()

    "#{log_entry}\n"
  end

  defp format_level(:debug), do: "DEBUG"
  defp format_level(:info), do: "INFO"
  defp format_level(:warn), do: "WARN"
  defp format_level(:warning), do: "WARN"
  defp format_level(:error), do: "ERROR"

  defp add_metadata(log_entry, metadata) do
    metadata
    |> Enum.reduce(log_entry, fn {key, value}, acc ->
      if value != nil do
        Map.put(acc, format_key(key), format_value(value))
      else
        acc
      end
    end)
  end

  defp format_key(key), do: to_string(key)

  defp format_value(value) when is_pid(value), do: inspect(value)
  defp format_value(value) when is_tuple(value), do: inspect(value)
  defp format_value(value) when is_function(value), do: "#Function<...>"
  defp format_value(value) when is_map(value), do: value
  defp format_value(value) when is_list(value), do: value
  defp format_value(value), do: to_string(value)
end
