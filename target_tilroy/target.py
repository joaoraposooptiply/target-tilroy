from target_hotglue.target import TargetHotglue
from typing import List, Optional, Union
from pathlib import PurePath
from singer_sdk import typing as th

from target_tilroy.sinks import PurchaseOrderSink


class TargetTilroy(TargetHotglue):
    """Singer target for Tilroy Purchase API."""

    def __init__(
        self,
        config: Optional[Union[dict, PurePath, str, List[Union[PurePath, str]]]] = None,
        parse_env_config: bool = False,
        validate_config: bool = True,
        state: str = None
    ) -> None:
        if isinstance(config, list):
            self.config_file = config[0]
        else:
            self.config_file = config
        super().__init__(config, parse_env_config, validate_config)
    
    name = "target-tilroy"
    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_url",
            th.StringType,
            default="https://api.tilroy.com",
            description="API URL for Tilroy API"
        ),
        th.Property(
            "tilroy_api_key",
            th.StringType,
            required=True,
            description="API Key for Tilroy authentication"
        ),
        th.Property(
            "x_api_key",
            th.StringType,
            required=True,
            description="X API Key for Tilroy authentication"
        ),
        th.Property(
            "warehouse_id",
            th.IntegerType,
            required=True,
            description="Warehouse ID for purchase orders"
        )
    ).to_dict()
    
    SINK_TYPES = [PurchaseOrderSink]

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