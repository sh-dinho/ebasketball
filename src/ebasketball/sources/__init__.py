"""External data sources and collection adapter interfaces."""

from ebasketball.sources.interface import SourceAdapter
from ebasketball.sources.json_file import JsonFileAdapter

__all__ = ["JsonFileAdapter", "SourceAdapter"]
