from singer_sdk import Target
from singer_sdk.typing import PropertiesList, Property, StringType, IntegerType
from typing import Dict, Any, List, Optional

from target_tilroy.sinks import PurchaseOrderSink


class TargetTilroy(Target):
    """Singer target for Tilroy Purchase API."""

    def __init__(self, *, config: Dict[str, Any], parse_env_config: bool = False, validate_config: bool = True):
        """Initialize the target."""
        super().__init__(config=config, parse_env_config=parse_env_config, validate_config=validate_config)
        self._sinks: Dict[str, PurchaseOrderSink] = {}
    
    name = "target-tilroy"
    config_jsonschema = PropertiesList(
        Property("api_url", StringType, default="https://api.tilroy.com", description="API URL for Tilroy API"),
        Property("tilroy_api_key", StringType, required=True, description="API Key for Tilroy authentication"),
        Property("x_api_key", StringType, required=True, description="X API Key for Tilroy authentication"),
        Property("warehouse_id", IntegerType, required=True, description="Warehouse ID for purchase orders")
    ).to_dict()
    
    def get_sink(self, stream_name: str, schema: Dict[str, Any], key_properties: List[str]) -> PurchaseOrderSink:
        """Get a sink for the given stream."""
        if stream_name == "BuyOrders":
            return PurchaseOrderSink(self, stream_name, schema, key_properties)
        else:
            raise ValueError(f"Unknown stream: {stream_name}")
    
    def process_record(self, record: Dict[str, Any], context: Dict[str, Any]) -> None:
        """Process a single record."""
        stream_name = record.get("stream")
        if not stream_name:
            self.logger.warning("Record missing stream name, skipping")
            return
            
        # Get or create sink for this stream
        if stream_name not in self._sinks:
            if stream_name == "BuyOrders":
                # Create a default schema for BuyOrders if not provided
                schema = record.get("schema", self._get_default_buy_orders_schema())
                key_properties = record.get("key_properties", [])
                self._sinks[stream_name] = self.get_sink(stream_name, schema, key_properties)
            else:
                raise ValueError(f"Unknown stream: {stream_name}")
        
        sink = self._sinks[stream_name]
        # Extract the actual record data from the Singer message
        record_data = record.get("record", {})
        sink.process_record(record_data, context)
    
    def _process_schema_message(self, message: Dict[str, Any]) -> None:
        """Process a schema message."""
        stream_name = message.get("stream")
        if stream_name == "BuyOrders":
            # Create sink for this stream if it doesn't exist
            if stream_name not in self._sinks:
                schema = message.get("schema", self._get_default_buy_orders_schema())
                key_properties = message.get("key_properties", [])
                self._sinks[stream_name] = self.get_sink(stream_name, schema, key_properties)
                self.logger.info(f"Created sink for stream: {stream_name}")
        else:
            self.logger.warning(f"Unknown stream in schema message: {stream_name}")
    
    def _assert_sink_exists(self, stream_name: str) -> None:
        """Override to handle BuyOrders stream."""
        if stream_name == "BuyOrders":
            if stream_name not in self._sinks:
                # Create sink automatically for supported streams
                schema = self._get_default_buy_orders_schema()
                key_properties = []
                self._sinks[stream_name] = self.get_sink(stream_name, schema, key_properties)
                self.logger.info(f"Auto-created sink for stream: {stream_name}")
        else:
            # Use the parent method for other streams
            super()._assert_sink_exists(stream_name)
    
    def _process_record_message(self, message: Dict[str, Any]) -> None:
        """Override to handle BuyOrders stream directly."""
        stream_name = message.get("stream")
        if stream_name == "BuyOrders":
            # Process directly without stream mapping
            self.process_record(message, {})
        else:
            # Use the parent method for other streams
            super()._process_record_message(message)
    
    def _get_default_buy_orders_schema(self) -> Dict[str, Any]:
        """Get the default schema for buy orders."""
        return {
            "type": "object",
            "properties": {
                "id": {"type": ["integer", "null"]},
                "transaction_date": {"format": "date-time", "type": ["string", "null"]},
                "created_at": {"format": "date-time", "type": ["string", "null"]},
                "line_items": {"type": ["string", "null"]},
                "customer_id": {"type": ["integer", "null"]},
                "remoteId": {"type": ["string", "null"]},
                "supplier_name": {"type": ["string", "null"]},
                "supplier_remoteId": {"type": ["string", "null"]},
                "syncedDate": {"type": ["string", "null"]},
                "externalid": {"type": ["integer", "null"]}
            },
            "additionalProperties": True
        }
    
    def process_batch(self, context: Dict[str, Any]) -> None:
        """Process any remaining records in all sinks."""
        for sink in self._sinks.values():
            sink.process_batch(context)
    
    def clean_up(self) -> None:
        """Clean up resources."""
        for sink in self._sinks.values():
            sink.clean_up()

def cli():
    """CLI entry point for target-tilroy."""
    import sys
    import os
    import glob
    
    # Check if no --input parameter is provided
    if '--input' not in sys.argv:
        # Look for data.singer file in common locations
        possible_paths = [
            'data.singer',
            'etl-output/data.singer',
            'sync-output/data.singer',
            '/home/hotglue/*/etl-output/data.singer',
            '/home/hotglue/*/sync-output/data.singer'
        ]
        
        data_file = None
        for path in possible_paths:
            if '*' in path:
                # Handle glob patterns
                matches = glob.glob(path)
                if matches:
                    data_file = matches[0]
                    break
            elif os.path.exists(path):
                data_file = path
                break
        
        if data_file:
            # Insert --input parameter
            sys.argv.insert(1, '--input')
            sys.argv.insert(2, data_file)
            print(f"Auto-detected input file: {data_file}")
    
    TargetTilroy.cli()


if __name__ == "__main__":
    cli()