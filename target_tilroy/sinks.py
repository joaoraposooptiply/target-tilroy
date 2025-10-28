"""Purchase Order Sink for Tilroy API."""

import logging
from typing import Any, Dict, List, Optional

# import requests
# from singer_sdk import typing as th
# from singer_sdk.sinks import Sink
from datetime import datetime
from target_tilroy.client import TilroySink

logger = logging.getLogger(__name__)


class PurchaseOrderSink(TilroySink):
    """Sink for Purchase Orders to Tilroy API."""

    name = "BuyOrders"
    # Define the endpoint path for this sink
    endpoint = "/purchaseapi/production/import/purchaseorders"
    
    def preprocess_record(self, record: dict, context: dict) -> Optional[dict]:
        """Prepare Tilroy purchase order payload before sending to API."""
    
        # process order_date
        order_date = record.get("transaction_date")
        if isinstance(order_date, datetime):
            order_date = order_date.strftime("%Y-%m-%d")
            
        requested_delivery_date = record.get("delivery_date")
        if isinstance(requested_delivery_date, datetime):
            requested_delivery_date = requested_delivery_date.strftime("%Y-%m-%d")

        payload = {
            "orderDate": order_date,
        }
        
        # Set requestedDeliveryDate - use provided date or default to 30 days from order date
        if requested_delivery_date:
            payload["requestedDeliveryDate"] = requested_delivery_date
        else:
            # Default to 30 days from order date
            from datetime import timedelta
            if order_date:
                order_dt = datetime.strptime(order_date, "%Y-%m-%d")
                default_delivery = order_dt + timedelta(days=30)
                payload["requestedDeliveryDate"] = default_delivery.strftime("%Y-%m-%d")
            else:
                # Fallback to current date + 30 days
                default_delivery = datetime.now() + timedelta(days=30)
                payload["requestedDeliveryDate"] = default_delivery.strftime("%Y-%m-%d")

        # supplierReference - use supplier_remoteId or supplier_reference
        supplier_ref = record.get("supplier_reference") or record.get("supplier_remoteId")
        if supplier_ref:
            payload["supplierReference"] = str(supplier_ref)

        # supplier tilroyId check
        if record.get("supplier_remoteId"):
            payload["supplier"] = {"tilroyId": record.get("supplier_remoteId")}
        else:
            self.logger.info(
                f"Skipping order {record.get('id')} because supplier_remoteId is missing"
            )
            return None

        # process items (lines) - handle both "items" and "line_items" fields
        items = record.get("items", [])
        if not items:
            items = record.get("line_items", [])
        
        if isinstance(items, str):
            items = self.parse_objs(items)

        if not items:
            self.logger.info(f"Skipping order {record.get('id')} with no line items")
            return None

        payload["lines"] = []
        for item in items:
            transformed_item = {
                "status": item.get("status", "open"),  # Default status if not provided
                "sku": {"tilroyId": str(item.get("product_remoteId"))},
                "qty": {"ordered": item.get("quantity")},
                "warehouse": {"number": int(self.config.get("warehouse_id"))}
            }
            
            # Set requestedDeliveryDate for line item - use provided date or default to order's delivery date
            if item.get("delivery_date"):
                transformed_item["requestedDeliveryDate"] = item.get("delivery_date")
            else:
                # Use the order's requestedDeliveryDate as default
                transformed_item["requestedDeliveryDate"] = payload.get("requestedDeliveryDate")
                
            payload["lines"].append(transformed_item)

        return payload
    
    def upsert_record(self, record: dict, context: dict) -> None:
        """Send purchase order to Tilroy API."""
        state_updates = {}
        if record:
            params = {}

            # Construct and log the full URL
            full_url = f"{self.base_url}{self.endpoint}"
            self.logger.info(f"Making API request to: {full_url}")
            self.logger.info(f"Request method: POST")
            self.logger.info(f"Request payload: {record}")

            response = self.request_api(
                "POST",
                endpoint=self.endpoint,
                request_data=record,
                params=params
            )
            res_json_id = response.json().get("supplierReference")
            self.logger.info(f"{self.name} created in Tilroy with ID: {res_json_id}")
            return res_json_id, True, state_updates

