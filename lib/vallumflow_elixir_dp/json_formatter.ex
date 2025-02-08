# VallumflowElixirDp - A data processor for security findings
# Copyright (C) 2025 cowsecurity
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

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
  defp format_level(:critical), do: "CRITICAL"

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
