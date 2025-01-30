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

defmodule VallumflowElixirDp.Application do
  @moduledoc false
  use Application
  require Logger

  @impl true
  def start(_type, _args) do
    Logger.info("Starting Vallumflow Data Processing Service...")

    children = [
      # Main data processing service
      {VallumflowElixirDp.DataProcessor, []}
    ]

    opts = [strategy: :one_for_one, name: VallumflowElixirDp.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
