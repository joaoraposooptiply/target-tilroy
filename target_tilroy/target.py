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
            "base_url",
            th.StringType,
            default="https://api.tilroy.com",
            description="Base URL for Tilroy API"
        ),
        th.Property(
            "Tilroy-Api-Key",
            th.StringType,
            required=True,
            description="API Key for Tilroy authentication"
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
    TargetTilroy.cli()


if __name__ == "__main__":
    cli()