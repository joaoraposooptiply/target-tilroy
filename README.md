# target-tilroy

A [Singer](https://singer.io) target for Tilroy that loads data into Tilroy's Purchase API, built with the Meltano SDK for Singer Targets.

## Installation


### Install from source

```bash
git clone https://github.com/your-username/target-tilroy.git
cd target-tilroy

```

## Configuration

Create a `config.json` file in the `secrets` folder with your Tilroy API configuration:

```json
{
  "base_url": "https://api.tilroy.com",
  "Tilroy-Api-Key": "your-api-key-here",
  "warehouse_id": 500134,
}
```

### Configuration Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `base_url` | string | No | Base URL for Tilroy API (default: https://api.tilroy.com) |
| `Tilroy-Api-Key` | string | Yes | Your Tilroy API key |
| `warehouse_id` | integer | Yes | Warehouse ID for purchase orders |

## Usage

### Basic Usage

```bash
# Read from stdin and write to Tilroy
poetry run target-tilroy --config secrets/config.json
```

### With State File

```bash
# Use a state file to track sync progress
poetry run target-tilroy --config secrets/config.json --state state.json
```

### Test with Sample Data

```bash
# Test with the provided sample data
cat payloads/sample_purchase_orders.json | target-tilroy --config secrets/config.json
```
```bash
# For Windows command 
Get-Content payloads/data_fixed.singer | poetry run target-tilroy --config secrets/config.json

```

## Architecture

The target follows a clean architecture pattern:

- **TilroyClient**: Base client class that handles authentication, base URL, and common API operations
- **PurchaseOrderSink**: Sink class that defines the endpoint path and transformation logic
- **TargetTilroy**: Main target class that orchestrates the sinks

This structure allows for easy extension to support additional Tilroy API endpoints by creating new sink classes.

## Development

### Setup Development Environment

```bash
# Clone the repository
git clone https://github.com/your-username/target-tilroy.git
cd target-tilroy

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate


```

### Project Structure

```
target-tilroy/
├── target_tilroy/
│   ├── __init__.py
│   ├── target.py              # Main target implementation
│   ├── client.py              # Base Tilroy API client
│   └── sinks.py               # Sink implementations
├── payloads/
│   ├── README.md
│   └── sample_purchase_orders.json  # Sample Singer data
├── secrets/
│   └── config.json            # Configuration file (not in git)
├── pyproject.toml            # Poetry configuration
├── README.md                 # This file
└── .gitignore
```

## Supported Python Versions

This target supports Python 3.9, 3.10, and 3.11.

## License

MIT License - see LICENSE file for details.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Run the test suite
6. Submit a pull request

## Support

For issues and questions, please open an issue on GitHub or contact the maintainers.