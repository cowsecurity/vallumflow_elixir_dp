# VallumflowElixirDp

An Elixir-based data processor for security findings that consumes tool outputs from Kafka and normalizes them into a consistent format. This application is part of the Vallumflow platform and handles security findings from various vulnerability scanners and security tools.

## Features

- Consumes security findings from Kafka topics
- Universal parsing of security tool outputs (supports Trivy, Grype, Semgrep, and more)
- Intelligent detection and extraction of security-related data
- Standardized output format for consistent downstream processing
- Rich analysis capabilities including:
  - Severity distributions
  - Component analysis
  - Metadata analysis
  - Temporal analysis
  - Impact assessment

## Prerequisites

- Elixir
- Erlang
- Kafka

## Environment Variables

Create a `.env` file in the root directory based on `.env.example` with the following variables:

```env
KAFKA_BROKER=""
KAKFA_PORT=9092
```

## Installation

1. Clone the repository:
```bash
git clone git@github.com:cowsecurity/vallumflow_elixir_dp.git
cd vallumflow_elixir_dp
```

2. Install dependencies:
```bash
mix deps.get
```

## Running

Start the application with:
```bash
mix run --no-halt
```

## Input Format

The application expects messages in the following format:
```json
{
  "agent_id": "string",
  "dashboard_json": <tool_output>,
  "node_id": "string",
  "org_id": "string"
}
```

The `dashboard_json` field can contain:
- JSON string
- Array of JSON strings
- Parsed JSON object

## Output Format

The processor normalizes findings into:
```json
{
  "metadata": {
    "version": "string",
    "timestamp": "string"
  },
  "summary": {
    "total_findings": number,
    "severity_distribution": {},
    "component_distribution": {},
    "remediation_available": number,
    "findings_with_references": number
  },
  "findings": [
    {
      "id": "string",
      "name": "string",
      "description": "string",
      "severity": "string",
      "location": {},
      "metadata": {},
      "details": {}
    }
  ],
  "analysis": {
    "severity_analysis": {},
    "component_analysis": {},
    "metadata_analysis": {}
  }
}
```

