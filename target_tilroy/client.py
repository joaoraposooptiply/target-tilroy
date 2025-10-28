
from singer_sdk.sinks import Sink
import json
from singer_sdk.plugin_base import PluginBase
from typing import Dict, List, Optional
import json
from difflib import SequenceMatcher
from heapq import nlargest as _nlargest
import ast

from urllib.parse import urlparse
class TilroySink(Sink):
    def __init__(
        self,
        target: PluginBase,
        stream_name: str,
        schema: Dict,
        key_properties: Optional[List[str]],
    ) -> None:
        """Initialize target sink."""
        self._target = target
        super().__init__(target, stream_name, schema, key_properties)

    auth_state = {}


    @property
    def base_url(self) -> str:
        """Return the API base URL for Tilroy API."""
        return self._target.config.get("api_url", "https://api.tilroy.com")
    
    
    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        # headers.update(self.authenticator.auth_headers or {})
        
        # Add Tilroy API key headers
        if hasattr(self._target, 'config'):
            if self._target.config.get('tilroy_api_key'):
                headers['Tilroy-Api-Key'] = self._target.config['tilroy_api_key']
            if self._target.config.get('x_api_key'):
                headers['X-Api-Key'] = self._target.config['x_api_key']
        
        return headers
    
    def parse_objs(self, obj):
        parsed_obj = None
        try:
            parsed_obj = ast.literal_eval(obj)
        except:
            parsed_obj = json.loads(obj)
        finally:
            return parsed_obj
