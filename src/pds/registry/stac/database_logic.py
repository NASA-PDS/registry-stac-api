import asyncio
import logging
import orjson
from functools import partial
from base64 import urlsafe_b64decode
from base64 import urlsafe_b64encode
from typing import Any, Dict, List, Iterable, Optional, Tuple


from stac_pydantic.shared import BBox

from stac_fastapi.core.utilities import MAX_LIMIT
from stac_fastapi.opensearch.database_logic import DatabaseLogic
from stac_fastapi.types.errors import NotFoundError
from opensearchpy.helpers.search import Search
from opensearchpy import exceptions

from .types import Collection
from .types import Item

logger = logging.getLogger(__name__)

class PDSDatabaseLogic(DatabaseLogic):
    """Database logic."""

    PRODUCT_INDEX_NAME = "registry"
    DEFAULT_SORT = "ops:Harvest_Info/ops:harvest_date_time"

    def __init__(self):
        super().__init__()
        self.__found_collections_cache: Optional[Dict[str, dict]] = self.__get_all_collection_ids()

    def get_all_catalog_ids(self) -> list[str]:
        """Get all catalog ids from the database.

            Get the list of catalogs fron the targets of products with bounding coordinates

            POST registry/_search
            {
              "size": 0,
              "query": {
                "bool": {
                  "filter": [
                    {
                      "exists": {
                        "field": "cart:Bounding_Coordinates/cart:east_bounding_coordinate"
                      }
                    }
                  ]
                }
              },
              "aggs": {
                "unique_targets": {
                  "terms": {
                    "field": "pds:Target_Identification/pds:name",
                    "size": 1000
                  }
                }
              }
            }


        """
        return []
        # query = {
        #     "size": 0,
        #     "aggs": {
        #         "unique_catalogs": {
        #             "terms": {
        #                 "field": "pds:Catalog_Identification/pds:catalog_id.keyword",
        #                 "size": 10000
        #             }
        #         }
        #     }
        # }
        # response = self.client.search(index=self.index_name, body=query)
        # catalog_buckets = response.get("aggregations", {}).get("unique_catalogs", {}).get("buckets", [])
        # catalog_ids = [bucket["key"] for bucket in catalog_buckets]
        # return catalog_ids

    def __get_all_collection_ids(self) -> list[str]:
        # Build the query using opensearch-py DSL
        search = Search(index=self.PRODUCT_INDEX_NAME)
        search = search.filter("exists", field="cart:Bounding_Coordinates/cart:east_bounding_coordinate")
        search = search.filter("term", product_class="Product_Observational")
        search = search.extra(size=0)  # No hits, only aggs
        # TODO see what to do when west is greater than east (crossing the antimeridian)
        search.aggs.bucket(
                "unique_parent_collections",
                "terms",
                field="ops:Provenance/ops:parent_collection_identifier",
                size=1000,
        ).metric(
            "max_east_bound",
            "max",
            field="cart:Bounding_Coordinates/cart:east_bounding_coordinate"
        ).metric(
            "min_west_bound",
            "min",
            field="cart:Bounding_Coordinates/cart:west_bounding_coordinate"
        ).metric(
           "max_north_bound",
           "max",
           field="cart:Bounding_Coordinates/cart:north_bounding_coordinate"
        ).metric(
           "min_south_bound",
           "min",
           field="cart:Bounding_Coordinates/cart:south_bounding_coordinate"
        )

        search = search.using(self.sync_client)

        response = search.execute()
        response_dict = response.to_dict()

        def bucket_to_collection(bucket):
            return {
                "bbox": [[
                    bucket["min_west_bound"]["value"],
                    bucket["max_east_bound"]["value"],
                    bucket["min_south_bound"]["value"],
                    bucket["max_north_bound"]["value"],
                ]]
            }

        collections = {bucket["key"]:bucket_to_collection(bucket) for bucket in response_dict["aggregations"]["unique_parent_collections"]["buckets"]}
        return collections

    async def get_all_collections(
            self,
            token: Optional[str],
            limit: int,
            request: Any = None,
            sort: Optional[List[Dict[str, Any]]] = None,
            bbox: Optional[BBox] = None,
            q: Optional[List[str]] = None,
            filter: Optional[Dict[str, Any]] = None,
            query: Optional[Dict[str, Dict[str, Any]]] = None,
            datetime: Optional[str] = None,
    ) -> Tuple[List[Dict[str, Any]], Optional[str], Optional[int]]:
        """Retrieve a list of collections from the database, supporting pagination.

        All the arguments are currently ignored none of them are implemented.
        The number of collections returned is small enough to not require any of the filters.
        """

        search = Search(index=self.PRODUCT_INDEX_NAME)
        search = search.query("ids", values=list(self.__found_collections_cache.keys()))
        search = search.using(self.sync_client)

        response = search.execute()
        response_dict = response.to_dict()

        collections = []
        for hit in response_dict["hits"]["hits"]:
            collection = Collection(
                hit["_source"],
                ancillary=self.__found_collections_cache.get(hit["_id"], None)
            ).to_stac()
            collections.append(collection)

        return (collections, None, None)

    async def find_collection(self, collection_id: str) -> Dict:
        """Find a collection in the database."""

        try:
            collection = await self.client.get(
                index=self.PRODUCT_INDEX_NAME, id=collection_id
            )
        except exceptions.NotFoundError:
            raise NotFoundError(f"Collection {collection_id} not found")

        if collection["_source"]["product_class"] != "Product_Collection":
            raise NotFoundError(f"Collection {collection_id} not found")

        return Collection(
            collection["_source"],
            ancillary=self.__found_collections_cache.get(collection["_id"], None)
        ).to_stac()


    async def execute_search(
            self,
            search: Search,
            limit: int,
            token: Optional[str],
            sort: Optional[Dict[str, Dict[str, str]]],
            collection_ids: Optional[List[str]],
            datetime_search: Dict[str, Optional[str]],
            ignore_unavailable: bool = True,
    ) -> Tuple[Iterable[Dict[str, Any]], Optional[int], Optional[str]]:
        """Execute a search query with limit and other optional parameters.

        Args:
            search (Search): The search query to be executed.
            limit (int): The maximum number of results to be returned.
            token (Optional[str]): The token used to return the next set of results.
            sort (Optional[Dict[str, Dict[str, str]]]): Specifies how the results should be sorted.
            collection_ids (Optional[List[str]]): The collection ids to search.
            datetime_search (Dict[str, Optional[str]]): Datetime range used for index selection.
            ignore_unavailable (bool, optional): Whether to ignore unavailable collections. Defaults to True.

        Returns:
            Tuple[Iterable[Dict[str, Any]], Optional[int], Optional[str]]: A tuple containing:
                - An iterable of search results, where each result is a dictionary with keys and values representing the
                fields and values of each document.
                - The total number of results (if the count could be computed), or None if the count could not be
                computed.
                - The token to be used to retrieve the next set of results, or None if there are no more results.

        Raises:
            NotFoundError: If the collections specified in `collection_ids` do not exist.
        """

        logger.info("Performing search in collecion's items")

        search_after = None

        search_body: Dict[str, Any] = {}

        if token:
            search_after = orjson.loads(urlsafe_b64decode(token))
        if search_after:
            search_body["search_after"] = search_after

        search_body["sort"] = sort if sort else self.DEFAULT_SORT

        filters = [{"term": {"product_class": "Product_Observational"}}]

        if collection_ids:
            filters.append({"terms": {"ops:Provenance/ops:parent_collection_identifier": collection_ids}})


        search_body["query"] = {"bool": {"filter": filters}}

        max_result_window = MAX_LIMIT
        size_limit = min(limit + 1, MAX_LIMIT)

        search_task = asyncio.create_task(
            self.client.search(
                index=self.PRODUCT_INDEX_NAME,
                ignore_unavailable=ignore_unavailable,
                body=search_body,
                size=size_limit,
            )
        )

        count_task = asyncio.create_task(
            self.client.count(
                index=self.PRODUCT_INDEX_NAME,
                ignore_unavailable=ignore_unavailable,
                body=search.to_dict(count=True),
            )
        )

        try:
            es_response = await search_task
        except exceptions.NotFoundError:
            raise NotFoundError(f"Collections '{collection_ids}' do not exist")

        hits = es_response["hits"]["hits"]
        items = (Item(hit["_source"]).to_stac() for hit in hits[:limit])

        next_token = None
        if len(hits) > limit and limit < max_result_window:
            if hits and (sort_array := hits[limit - 1].get("sort")):
                next_token = urlsafe_b64encode(orjson.dumps(sort_array)).decode()

        matched = (
            es_response["hits"]["total"]["value"]
            if es_response["hits"]["total"]["relation"] == "eq"
            else None
        )
        if count_task.done():
            try:
                matched = count_task.result().get("count")
            except Exception as e:
                logger.error(f"Count task failed: {e}")

        return items, matched, next_token


    async def get_one_item(self, collection_id: str, item_id: str) -> Dict:
        """Retrieve a single item from the database."""
        try:
            response = await self.client.search(
                index=self.PRODUCT_INDEX_NAME,
                body={
                    "query": {"term": {"_id": item_id}},
                    "size": 1,
                },
            )
            if response["hits"]["total"]["value"] == 0:
                raise NotFoundError(
                    f"Item {item_id} does not exist inside Collection {collection_id}"
                )

            candidate_item = response["hits"]["hits"][0]["_source"]

            if candidate_item.get("product_class") != "Product_Observational":
                raise NotFoundError(
                    f"Item {item_id} does not exist inside Collection {collection_id}"
                )

            collection_id_found = candidate_item.get("ops:Provenance/ops:parent_collection_identifier", [None])[0]
            if collection_id_found != collection_id:
                raise NotFoundError(
                    f"Item {item_id} does not exist inside Collection {collection_id} but was found in collection {collection_id_found}"
                )

            return Item(response["hits"]["hits"][0]["_source"]).to_stac()
        except exceptions.NotFoundError:
            raise NotFoundError(
                f"Item {item_id} does not exist inside Collection {collection_id}"
            )

    async def get_items_unique_values(
        self, collection_id: str, field_names: Iterable[str], *, limit: int = ...
    ) -> Dict[str, List[str]]:
        """Get the unique values for the given fields in the collection."""
        return {}

    async def create_item(self, item: Dict, refresh: bool = False) -> None:
        """Create an item in the database."""
        raise NotImplementedError()

    async def merge_patch_item(
        self,
        collection_id: str,
        item_id: str,
        item: Dict,
        base_url: str,
        refresh: bool = True,
    ) -> Dict:
        """Patch a item in the database follows RF7396."""
        raise NotImplementedError()

    async def json_patch_item(
        self,
        collection_id: str,
        item_id: str,
        operations: List,
        base_url: str,
        create_nest: bool = False,
        refresh: bool = True,
    ) -> Dict:
        """Patch a item in the database follows RF6902."""
        raise NotImplementedError()

    async def delete_item(
        self, item_id: str, collection_id: str, refresh: bool = False
    ) -> None:
        """Delete an item from the database."""
        raise NotImplementedError()

    async def get_items_mapping(self, collection_id: str) -> Dict[str, Dict[str, Any]]:
        """Get the mapping for the items in the collection."""
        raise NotImplementedError()

    async def create_collection(self, collection: Dict, refresh: bool = False) -> None:
        """Create a collection in the database."""
        raise NotImplementedError()

    async def merge_patch_collection(
        self,
        collection_id: str,
        collection: Dict,
        base_url: str,
        refresh: bool = True,
    ) -> Dict:
        """Patch a collection in the database follows RF7396."""
        raise NotImplementedError()

    async def json_patch_collection(
        self,
        collection_id: str,
        operations: List,
        base_url: str,
        create_nest: bool = False,
        refresh: bool = True,
    ) -> Dict:
        """Patch a collection in the database follows RF6902."""
        raise NotImplementedError()

    async def delete_collection(
        self, collection_id: str, refresh: bool = False
    ) -> None:
        """Delete a collection from the database."""
        raise NotImplementedError()
