import logging
from typing import List, Optional
from stac_pydantic.shared import BBox
from fastapi import Request
from stac_fastapi.types import stac as stac_types


from stac_fastapi.core.core import CoreClient

logger = logging.getLogger(__name__)

class PDSClient(CoreClient):
    """PDS OpenSearch Client."""

    async def get_search(
        self,
        request: Request,
        collections: Optional[List[str]] = None,
        ids: Optional[List[str]] = None,
        bbox: Optional[BBox] = None,
        datetime: Optional[str] = None,
        limit: Optional[int] = None,
        query: Optional[str] = None,
        token: Optional[str] = None,
        fields: Optional[List[str]] = None,
        sortby: Optional[str] = None,
        q: Optional[List[str]] = None,
        intersects: Optional[str] = None,
        filter_expr: Optional[str] = None,
        filter_lang: Optional[str] = None,
        **kwargs,
    ) -> stac_types.ItemCollection:
        """Get search results from the database.

        Args:
            collections (Optional[List[str]]): List of collection IDs to search in.
            ids (Optional[List[str]]): List of item IDs to search for.
            bbox (Optional[BBox]): Bounding box to search in.
            datetime (Optional[str]): Filter items based on the datetime field.
            limit (Optional[int]): Maximum number of results to return.
            query (Optional[str]): Query string to filter the results.
            token (Optional[str]): Access token to use when searching the catalog.
            fields (Optional[List[str]]): Fields to include or exclude from the results.
            sortby (Optional[str]): Sorting options for the results.
            q (Optional[List[str]]): Free text query to filter the results.
            intersects (Optional[str]): GeoJSON geometry to search in.
            kwargs: Additional parameters to be passed to the API.
        Returns:
            ItemCollection: Collection of `Item` objects representing the search results.

        Raises:
            HTTPException: If any error occurs while searching the catalog.
        """

        logger.info("Performing PDS item search")
        items = ["olem ipsum"]  # Placeholder for actual items retrieved from the database
        links = []
        count = 0
        return stac_types.ItemCollection(
            type="FeatureCollection",
            features=items,
            links=links,
            numberReturned=len(items),
            numberMatched=count,
        )
