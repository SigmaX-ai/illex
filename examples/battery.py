import pyarrow as pa

schema_fields = [pa.field("voltage",
                          pa.list_(pa.field("item", pa.uint64(), False)
                                   .with_metadata({"illex_MIN": "0", "illex_MAX": "2047"})), False)
                 .with_metadata({"illex_MIN_LENGTH": "64", "illex_MAX_LENGTH": "64"})]

schema = pa.schema(schema_fields)
serialized_schema = schema.serialize()
pa.output_stream('battery.as').write(serialized_schema)
