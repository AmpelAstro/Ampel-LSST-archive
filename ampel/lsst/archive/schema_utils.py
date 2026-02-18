import pyarrow as pa


def avro_type_to_pyarrow_type(
    avro_schema: dict | str | list, named_schemas: dict[str, pa.DataType]
) -> tuple[pa.DataType, bool]:
    """Convert an Avro schema to a PyArrow schema."""

    if isinstance(avro_schema, dict):
        if avro_schema.get("type") == "record":
            fields = []
            for field in avro_schema["fields"]:
                dtype, nullable = avro_type_to_pyarrow_type(
                    field["type"], named_schemas
                )
                fields.append(pa.field(field["name"], dtype, nullable=nullable))
            s = pa.struct(fields)
            named_schemas[avro_schema["name"]] = s
            return s, False
        if avro_schema.get("type") == "array":
            dtype, nullable = avro_type_to_pyarrow_type(
                avro_schema["items"], named_schemas
            )
            return pa.list_(dtype), nullable
        if avro_schema.get("type") == "map":
            dtype, nullable = avro_type_to_pyarrow_type(
                avro_schema["values"], named_schemas
            )
            return pa.map_(pa.string(), dtype), nullable
        if isinstance(avro_schema.get("type"), (list, str)):
            return avro_type_to_pyarrow_type(avro_schema["type"], named_schemas)

    if isinstance(avro_schema, str):
        if avro_schema in named_schemas:
            return named_schemas[avro_schema], False
        raise ValueError(f"Unsupported Avro type: {avro_schema}")

    if isinstance(avro_schema, list):
        # Handle union types (e.g., ["null", "string"])
        nullable = False
        members = []
        for i, t in enumerate(avro_schema):
            if t == "null":
                nullable = True
                continue
            dtype, field_nullable = avro_type_to_pyarrow_type(t, named_schemas)
            members.append(pa.field(f"field_{i}", dtype, nullable=field_nullable))
        if len(members) == 1:
            return members[0].type, nullable
        return pa.dense_union(members), nullable

    raise ValueError(f"Unsupported Avro schema: {avro_schema}")


def avro_schema_to_pyarrow_schema(
    avro_schema: dict,
    metadata: dict[bytes | str, bytes | str] | None = None,
) -> pa.Schema:
    dtype, nullable = avro_type_to_pyarrow_type(
        avro_schema,
        {
            "string": pa.string(),
            "int": pa.int32(),
            "long": pa.int64(),
            "float": pa.float32(),
            "double": pa.float64(),
            "boolean": pa.bool_(),
            "bytes": pa.binary(),
        },
    )
    assert not nullable, "Top-level schema cannot be nullable"
    assert isinstance(dtype, pa.StructType), "Top-level schema must be a struct"
    return pa.schema(dtype.fields, metadata=metadata)
