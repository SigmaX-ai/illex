import pyarrow as pa

schema_fields = [pa.field("timestamp", pa.date64(), False),
                 pa.field("string", pa.utf8(), False),
                 pa.field("integer", pa.uint64(), False).with_metadata({"illex_MIN": "13", "illex_MAX": "37"}),
                 pa.field("list_of_strings", pa.list_(pa.field("item", pa.utf8(), False)), False).with_metadata(
                     {"illex_MIN_LENGTH": "3"}),
                 pa.field("bool", pa.bool_(), False)
                 ]

schema = pa.schema(schema_fields)
serialized_schema = schema.serialize()
pa.output_stream('basics.as').write(serialized_schema)

print("Basics schema generated.")
