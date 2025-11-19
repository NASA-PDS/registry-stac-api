from stac_validator import stac_validator

STAC_BASE_URL = "http://localhost:8000/"

stac = stac_validator.StacValidate(STAC_BASE_URL + "collections/urn:nasa:pds:insight_rad:data_derived::7.0")
stac.run()
print(stac.message)


stac = stac_validator.StacValidate(STAC_BASE_URL + "collections/urn:nasa:pds:insight_rad:data_derived::7.0/items")
stac.run()
print(stac.message)


stac = stac_validator.StacValidate(STAC_BASE_URL + "collections/urn:nasa:pds:insight_rad:data_derived::7.0/items/urn:nasa:pds:insight_rad:data_derived:hp3_rad_der_00122_20190401_123217::1.0")
stac.run()
print(stac.message)


stac = stac_validator.StacValidate(STAC_BASE_URL + "collections/urn:nasa:pds:insight_rad:data_derived::7.0/items/urn:nasa:pds:insight_rad:data_derived:hp3_rad_der_00478_20200401_121608::1.0")
stac.run()
print(stac.message)

