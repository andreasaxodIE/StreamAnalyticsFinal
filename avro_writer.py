from __future__ import annotations
import io
import json
import struct
import zlib
import os
import uuid
from typing import Any, Dict, List

def encode_null(_value: None) -> bytes:
    return b""


def encode_boolean(value: bool) -> bytes:
    return b"\x01" if value else b"\x00"


def _encode_long_raw(n: int) -> bytes:
    """
    This function takes a regular Python integer and converts it into something
    called a zigzag encoded varint, which is how Avro stores integer values.

    Two things are happening here:

    1. Zigzag encoding: Avro uses this  to handle negative numbers efficiently.
       Normally, negative numbers in binary take up a lot of space. Zigzag encoding
       maps negative numbers to positive ones in a pattern like:
           0 -> 0, -1 -> 1, 1 -> 2, -2 -> 3, 2 -> 4, ...
       The formula for this is: (n << 1) ^ (n >> 63)
       That shifts n left by 1 bit and XORs it with the sign bit spread across 64 positions.

    2. Variable length encoding (varint): Instead of always using 8 bytes for a number,
       we only use as many bytes as we actually need. Each byte uses 7 bits for data
       and 1 bit to signal there are more bytes coming.
       This means small numbers like 1 or 2 only take 1 byte instead of 8.
    """
    # Apply the zigzag transformation to handle negatives cleanly
    n = (n << 1) ^ (n >> 63)

    buf = []
    while True:
        # Grab the lowest 7 bits of n
        b = n & 0x7F
        # Shift n right by 7 to move on to the next chunk
        n >>= 7
        if n:
            # If there's still more data left, set the high bit to 1 as a "continue" flag
            buf.append(b | 0x80)
        else:
            # If this is the last chunk, leave the high bit as 0 to signal we're done
            buf.append(b)
            break
    return bytes(buf)


def encode_int(value: int) -> bytes:
    # Avro's int type is a 32 bit integer, but we encode it the same way as long.
    # The underlying zigzag varint format handles both just fine.
    return _encode_long_raw(value)


def encode_long(value: int) -> bytes:
    # Avro's long type is a 64 bit integer. Same encoding as int.
    return _encode_long_raw(value)


def encode_float(value: float) -> bytes:
    # Floats are stored as 4 bytes using IEEE 754 single precision format.
    # The "<f" format string means little-endian float (4 bytes).
    return struct.pack("<f", value)


def encode_double(value: float) -> bytes:
    # Doubles are stored as 8 bytes using IEEE 754 double precision format.
    # The "<d" format string means little-endian double (8 bytes).
    # More bytes means more precision compared to float.
    return struct.pack("<d", value)


def encode_bytes(value: bytes) -> bytes:
    # To encode a byte string, we first write how many bytes are coming (as a long),
    # and then write the actual bytes right after.
    return _encode_long_raw(len(value)) + value


def encode_string(value: str) -> bytes:
    # Strings in Avro are just UTF-8 encoded bytes with a length prefix.
    # So we first convert the string to bytes, then use the encode_bytes logic.
    encoded = value.encode("utf-8")
    return _encode_long_raw(len(encoded)) + encoded


def encode_enum(value: str, schema: Dict) -> bytes:
    # Enums in Avro are defined with a list of allowed string values called symbols.
    # Instead of storing the string itself, Avro just stores the index 
    # of that string in the symbols list. So "HEARTS" might become 0 if it's first.
    symbols = schema["symbols"]
    idx = symbols.index(value)
    return encode_int(idx)


def encode_array(value: List, item_schema: Any, schema_registry: Dict) -> bytes:
    # Arrays in Avro are encoded in blocks. Each block starts with the count of items,
    # followed by the encoded items themselves, and ends with a 0 to signal no more blocks.

    if not value:
        return encode_long(0)

    # Write how many items are in this block
    buf = encode_long(len(value))

    # Encode each item according to the array's item schema
    for item in value:
        buf += encode_value(item, item_schema, schema_registry)

    buf += encode_long(0)
    return buf


def encode_map(value: Dict, value_schema: Any, schema_registry: Dict) -> bytes:
    # Maps are encoded similarly to arrays.
    # Each block starts with a count, then alternates key/value pairs,
    # and ends with a 0. 

    if not value:
        return encode_long(0)

    buf = encode_long(len(value))
    for k, v in value.items():
        # Keys are always encoded as strings
        buf += encode_string(k)
        # Values are encoded according to whatever the map's value schema says
        buf += encode_value(v, value_schema, schema_registry)

    buf += encode_long(0)
    return buf


def encode_union(value: Any, union_schemas: List, schema_registry: Dict) -> bytes:
    if value is None:
        # Look through the union options for a null branch
        for i, s in enumerate(union_schemas):
            if s == "null" or (isinstance(s, dict) and s.get("type") == "null"):
                # write the index, no actual data needed for null
                return encode_long(i)
        raise ValueError("None value but no null branch in union")

    # For non-null values, try each branch until one successfully encodes the value
    for i, s in enumerate(union_schemas):
        if s == "null" or (isinstance(s, dict) and s.get("type") == "null"):
            continue  # Skip the null branch since our value is not None
        try:
            encoded = encode_value(value, s, schema_registry)
            # If we get here without an exception, this branch worked
            return encode_long(i) + encoded
        except Exception:
            # That branch didn't work, try the next one
            continue

    raise ValueError(f"Cannot encode union value {value!r} with schemas {union_schemas}")


def encode_value(value: Any, schema: Any, schema_registry: Dict) -> bytes:

    if isinstance(schema, str):
        # First check if this string is actually a reference to a named type
        # we've seen before
        if schema in schema_registry:
            return encode_value(value, schema_registry[schema], schema_registry)

        # Otherwise it's one of Avro's built in primitive type names
        if schema == "null":    return encode_null(value)
        if schema == "boolean": return encode_boolean(value)
        if schema == "int":     return encode_int(value)
        if schema == "long":    return encode_long(value)
        if schema == "float":   return encode_float(value)
        if schema == "double":  return encode_double(value)
        if schema == "bytes":   return encode_bytes(value)
        if schema == "string":  return encode_string(value)
        raise ValueError(f"Unknown primitive schema name: {schema!r}")

    if isinstance(schema, list):
        # A list means this is a union type
        return encode_union(value, schema, schema_registry)

    if isinstance(schema, dict):
        t = schema.get("type")

        if t == "record":
            return encode_record(value, schema, schema_registry)
        if t == "enum":
            return encode_enum(value, schema)
        if t == "array":
            return encode_array(value, schema["items"], schema_registry)
        if t == "map":
            return encode_map(value, schema["values"], schema_registry)

        # Sometimes a dict just wraps a primitive type like {"type": "string"}
        # In that case we just unwrap it and handle the primitive directly
        if t in ("null", "boolean", "int", "long", "float", "double", "bytes", "string"):
            return encode_value(value, t, schema_registry)

    raise ValueError(f"Cannot encode value {value!r} using schema {schema!r}")


def encode_record(record: Dict, schema: Dict, schema_registry: Dict) -> bytes:
    buf = b""
    for field in schema["fields"]:
        fname = field["name"]
        fschema = field["type"]
        # Use the record's value if it exists, otherwise fall back to the default
        val = record.get(fname, field.get("default"))
        buf += encode_value(val, fschema, schema_registry)
    return buf


def build_schema_registry(schema: Dict, registry: Dict = None) -> Dict:
    if registry is None:
        registry = {}

    if isinstance(schema, dict):
        t = schema.get("type")
        if t in ("record", "enum", "fixed"):
            ns = schema.get("namespace", "")
            name = schema.get("name", "")
            # Build the fully qualified name 
            full_name = f"{ns}.{name}" if ns else name
            registry[full_name] = schema
            # Also register under the short name for convenience
            registry[name] = schema

        # Recurse into all values of this dict to find nested named types
        for v in schema.values():
            build_schema_registry(v, registry)

    elif isinstance(schema, list):
        # Could be a union or a list of schemas; recurse into each one
        for item in schema:
            build_schema_registry(item, registry)

    return registry


AVRO_MAGIC = b"Obj\x01"
AVRO_SYNC_MARKER_SIZE = 16  


class AvroWriter:
    """
    This class handles writing an Avro Object Container File (OCF) to disk.
    An OCF file has three parts:
      1. A header with the schema and metadata
      2. One or more data blocks, each containing a batch of encoded records
      3. A sync marker between blocks so readers can detect corruption

    We use the "null" codec by default, meaning no compression is applied.
    """

    def __init__(self, schema: Dict, filepath: str, codec: str = "null"):
        self.schema = schema
        self.filepath = filepath
        self.codec = codec

        # Build the registry upfront so we can resolve named type references later
        self.registry = build_schema_registry(schema)

        # The sync marker is a random 16-byte sequence unique to this file.
        # It gets written after every data block so readers can verify they're
        # reading blocks correctly and detect if the file is corrupted.
        self.sync_marker = os.urandom(AVRO_SYNC_MARKER_SIZE)

        # We buffer records in memory and flush them to disk in batches
        self._buffer: List[bytes] = []
        self._record_count = 0

        # Open the file right away and write the header
        self._f = open(filepath, "wb")
        self._write_header()

    def _write_header(self):
        # The Avro file header contains:
        #   * The magic bytes to identify this as an Avro file
        #   * A map of metadata
        #   * The sync marker 

        schema_json = json.dumps(self.schema).encode("utf-8")

        # These are the two required metadata entries every Avro file must have
        meta = {
            "avro.schema": schema_json,
            "avro.codec": self.codec.encode("utf-8"),
        }

        header = AVRO_MAGIC

        # Write the number of metadata entries
        header += encode_long(len(meta))

        # Write each key/value pair in the metadata map
        for k, v in meta.items():
            header += encode_string(k)
            header += encode_bytes(v)

        # Write a 0 to signal the end of the metadata map
        header += encode_long(0)

        # Write the sync marker that will also appear between data blocks
        header += self.sync_marker

        self._f.write(header)

    def write(self, record: Dict):
        # Encode the record and add it to our in-memory buffer.
        # We don't write to disk immediately since it's much faster to batch
        # writes together. We flush automatically once we've buffered 100 records.
        encoded = encode_record(record, self.schema, self.registry)
        self._buffer.append(encoded)
        self._record_count += 1

        if len(self._buffer) >= 100:
            self._flush_block()

    def _flush_block(self):
        # Write all buffered records to disk as a single Avro data block.
        # The block format is:
        #   * record count (long)
        #   * byte count of the data that follows (long)
        #   * the actual encoded record data
        #   * the sync marker (so readers can verify the block boundary)

        if not self._buffer:
            return  # Nothing to write

        data = b"".join(self._buffer)
        count = len(self._buffer)

        block = encode_long(count) + encode_long(len(data)) + data + self.sync_marker
        self._f.write(block)

        # Clear the buffer now that we've written everything out
        self._buffer = []

    def close(self):
        # Make sure any remaining buffered records get written before closing.
        # If we skip this step, the last batch of records would be lost!
        self._flush_block()
        self._f.close()

    def __enter__(self):
        # This is what makes the `with AvroWriter(...) as w:` syntax work
        return self

    def __exit__(self, *_):
        # When we exit the `with` block (whether normally or due to an exception),
        # close the file and flush any remaining records
        self.close()
