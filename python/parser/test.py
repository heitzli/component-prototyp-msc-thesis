import io

import avro.io
import avro.schema
import pytest
from queryparser import (
    analyze_query,
    from_avro_node,
    parse_sql,
    to_avro_node,
    translate_query,
)

# Load your Avro schema here if you want to test binary serialization.
SCHEMA = avro.schema.parse(open("../../protocol/algebranode.avsc").read())

test_queries_parser = [
    ("SELECT e.name FROM employee AS e", False),  # valid sql but invalid since table non existant
    ("INVALID SQL QUERY", True),
    (
        "BEGIN TRANSACTION SELECT e.name, d.id FROM employees e, departments d WHERE e.name = d.id AND NOT (e.salary >=50000) COMMIT;",
        False,
    ),
]


@pytest.mark.parametrize("query, expected_exception", test_queries_parser)
def test_parser(query: str, expected_exception: bool):
    if expected_exception:
        with pytest.raises(Exception):
            parse_sql(sql=query)
    else:
        parsed_query = parse_sql(sql=query)
        assert parsed_query is not None


test_queries_translator = [
    ("SELECT e.name FROM employees AS e", False),
    (
        "BEGIN TRANSACTION SELECT e.name, d.id FROM employees e, departments d WHERE e.name = d.id AND NOT (e.salary >=50000) COMMIT;",
        False,
    ),
]


@pytest.mark.parametrize("query, expected_exception", test_queries_translator)
def test_translator(query: str, expected_exception: bool):
    if expected_exception:
        with pytest.raises(Exception):
            parsed = parse_sql(sql=query)
            validated = analyze_query(parsed)
            translated = translate_query(validated)
    else:
        parsed = parse_sql(sql=query)
        validated = analyze_query(parsed)
        translated = translate_query(validated)
        assert translated is not None


test_queries_serde = [
    (
        "BEGIN TRANSACTION SELECT e.name, d.id FROM employees e, departments d WHERE e.name = d.id AND NOT (e.salary >=50000) COMMIT;",
        False,
    ),
]


@pytest.mark.parametrize("query, expected_exception", test_queries_serde)
def test_serde(query: str, expected_exception: bool):
    if expected_exception:
        with pytest.raises(Exception):
            parsed = parse_sql(sql=query)
            validated = analyze_query(parsed)
            translated = translate_query(validated)
            node_data = to_avro_node(translated)

            buffer = io.BytesIO()
            writer = avro.io.DatumWriter(SCHEMA)
            writer.write(node_data, avro.io.BinaryEncoder(buffer))

            encoded = buffer.getvalue()

            data = io.BytesIO(encoded)
            reader = avro.io.DatumReader(writers_schema=SCHEMA, readers_schema=SCHEMA)
            res = reader.read(decoder=avro.io.BinaryDecoder(data))

            deserialized = from_avro_node(res)
    else:
        parsed = parse_sql(sql=query)
        validated = analyze_query(parsed)
        translated = translate_query(validated)
        node_data = to_avro_node(translated)

        buffer = io.BytesIO()
        writer = avro.io.DatumWriter(SCHEMA)
        writer.write(node_data, avro.io.BinaryEncoder(buffer))

        encoded = buffer.getvalue()

        data = io.BytesIO(encoded)
        reader = avro.io.DatumReader(writers_schema=SCHEMA, readers_schema=SCHEMA)
        res = reader.read(decoder=avro.io.BinaryDecoder(data))

        deserialized = from_avro_node(res)

        assert translated == deserialized
