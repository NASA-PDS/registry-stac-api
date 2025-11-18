from stac_pydantic.version import STAC_VERSION

# TODO review the design as the object approach does not make a lot of sense here

class STACObject:
    """Base STAC Object model for PDS Registry."""

    MAIN_API_BASE_URL = "http://pds.nasa.gov/api/search/1/"

    def __init__(self, source: dict, ancillary: dict = None):
        self.stac_vervion = STAC_VERSION
        self.licence = "CC0-1.0"
        self.id = source.get("lidvid")
        self.title = source.get("pds:Identification_Area/pds:title")
        self.description = source.get("pds:Identification_Area/pds:description")
        keywords = []
        keywords.extend(source.get("pds:Observing_System/pds:name", []))
        keywords.extend(source.get("pds:Target_Identification/pds:name", []))
        keywords.extend(source.get("pds:Investigation_Area/pds:name", []))
        keywords.extend(source.get("pds:Observing_System_Component/pds:name", []))
        keywords.extend(source.get("pds:Science_Facets/pds:domain", []))
        self.keywords = keywords

        main_api_prefix = STACObject.MAIN_API_BASE_URL + "products/"
        self.investigation = main_api_prefix + source.get("ref_lid_investigation")[0] if "ref_lid_investigation" in source else None
        self.platform = main_api_prefix + source.get("ref_lid_platform")[0] if "ref_lid_platform" in source else None
        self.instrument = main_api_prefix + source.get("ref_lid_instrument")[0] if "ref_lid_instrument" in source else None

        providers = []
        discipline_node = {}
        discipline_node["name"] = source["ops:Harvest_Info/ops:node_name"][0]
        discipline_node["role"] = "custodian"
        discipline_node["url"] = "https://pds.nasa.gov/"
        providers.append(discipline_node)

        if providers:
            self.providers = providers

        if "pds:Time_Coordinates/pds:start_date_time" in source or "pds:Time_Coordinates/pds:stop_date_time" in source:
            self.temporal_interval = [[
                source.get("pds:Time_Coordinates/pds:start_date_time", [None])[0],
                source.get("pds:Time_Coordinates/pds:stop_date_time", [None])[0]
            ]]

    def to_stac(self):
        return dict(
            stac_version=self.stac_vervion,
            license=self.licence,
            id=self.id,
        )


class Collection(STACObject):
    """STAC Collection model for PDS Registry."""

    def __init__(self, source: dict, ancillary: dict = None):
        super().__init__(source)
        self.bbox = ancillary["bbox"]

    def to_stac(self):
        collection = super().to_stac()
        collection["type"] = "Collection"
        collection["title"] = self.title
        collection["description"] = self.description
        collection["keywords"] = self.keywords
        collection["providers"] = self.providers
        collection["extent"] = {
            "spatial": {
                "bbox": [self.bbox]
            },
            "temporal": {
                "interval": self.temporal_interval
            }
        }
        return collection



class Item(STACObject):
    """STAC Item model for PDS Registry."""

    def __init__(self, source: dict, ancillary: dict = None):
        super().__init__(source)
        self.type = "Collection"
        self.collection = source.get("ops:Provenance/ops:parent_collection_identifier")

        self.geometry = source.get("bbox_polygon", None)

        if "ops:Data_File_Info/ops:file_ref" in source:
            assets = {}
            file_refs = source["ops:Data_File_Info/ops:file_ref"]
            for idx, file_ref in enumerate(file_refs):
                asset_key = f"data_file_{idx+1}"
                assets[asset_key] = {
                    "href": file_ref,
                    "title": source.get("ops:Data_File_Info/ops:file_name", ["Data File"])[0],
                    "type": source.get("ops:Data_File_Info/ops:mime_type", ["application/octet-stream"])[0],
                }
            self.assets = assets


    def to_stac(self):
        result = super().to_stac()
        result["type"] = "Feature"
        result["collection"] = self.collection

        if self.geometry:
            result["geometry"] = self.geometry

        result["assets"] = self.assets
        result["properties"] = dict(
            title=self.title,
            description=self.description,
            keywords=self.keywords,
            providers=self.providers,
            temporal_interval=self.temporal_interval,
        )

        return result
