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
