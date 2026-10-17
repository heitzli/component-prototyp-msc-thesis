from __future__ import annotations

import io
import json
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Literal

import avro.io
import avro.schema
from antlr4 import CommonTokenStream, InputStream
from generated.SchofSQLLexer import SchofSQLLexer
from generated.SchofSQLParser import SchofSQLParser
from generated.SchofSQLVisitor import SchofSQLVisitor
from pydantic import BaseModel, ConfigDict

"""
Overall Idea of Query Processing:
1. ANTLR parse tree: syntax-oriented and grammar-specific.
2. Semantic analyzer: resolves names, checks types and scopes.
3. Resolved logical representation: schema-aware and independent of ANTLR.
4. Relational algebra translator: translates resolved constructs into operators.
5. Optimizer: rewrites the algebra while preserving semantics.
"""

# TODO: compare with algebra presented in DSC, missing for example aggregations / group by
"""
From Database System Slides

Relational Algebra Operators:
    - Union(R, S)
    - Intersection(R, S)
    - Difference(R,S)
    - Selection(expression, Relation)
    - Projection(column, Relation)
    - Natural Join (equality over join attribute)
    - Assignement / renaming
    - Division(R, S)
    - CartesianProduct(R, S)
    - ThetaJoin(R,S, expression(R,comp,S))

Minimal Algebra:
    - Cartesian Product
    - Projection
    - Selection
    - Union
    - Difference

    => Relational Complete
"""


# NOTE: every relational operator produces a relational operator as output
class AbstractAlgebraNode(BaseModel, ABC):
    model_config = ConfigDict(frozen=True)

    @abstractmethod
    def children(self) -> list[AbstractAlgebraNode]: ...

    @abstractmethod
    def label(self) -> str: ...

    def print_tree(self, indent=0):
        print("  " * indent + self.label())
        for child in self.children():
            child.print_tree(indent + 1)


class TableScan(AbstractAlgebraNode):
    kind: Literal["TableScan"] = "TableScan"
    table: str
    alias: str | None = None

    def children(self) -> list[AbstractAlgebraNode]:
        return []

    def label(self) -> str:
        if self.alias:
            return f"TableScan({self.table} AS {self.alias})"
        return f"TableScan({self.table})"


# σ for selection, π for projection


class Projection(AbstractAlgebraNode):
    kind: Literal["Projection"] = "Projection"
    columns: tuple[ResolvedColumn, ...]
    child: AbstractAlgebraNode

    def children(self) -> list[AbstractAlgebraNode]:
        return [self.child]

    def label(self) -> str:
        projection_str = ", ".join(f"{col.table}.{col.name}" for col in self.columns)
        return f"π[{projection_str}]"


class Product(AbstractAlgebraNode):
    kind: Literal["Product"] = "Product"
    left: AbstractAlgebraNode
    right: AbstractAlgebraNode

    def children(self) -> list[AbstractAlgebraNode]:
        return [self.left, self.right]

    def label(self) -> str:
        return "PRODUCT"


def to_avro_node(node: AbstractAlgebraNode) -> dict:
    if isinstance(node, TableScan):
        branch = {
            "table": node.table,
            "alias": node.alias,
        }

    elif isinstance(node, Projection):
        branch = {
            "columns": [
                {
                    "table": col.table,
                    "name": col.name,
                    "type": col.type,
                }
                for col in node.columns
            ],
            "child": to_avro_node(node.child),
        }

    elif isinstance(node, Product):
        branch = {
            "left": to_avro_node(node.left),
            "right": to_avro_node(node.right),
        }

    else:
        raise ValueError(f"Unsupported algebra node: {type(node).__name__}")

    return {"node": branch}


def from_avro_node(data: dict) -> AbstractAlgebraNode:
    branch = data.get("node", data)

    if "table" in branch and "table" in branch:
        return TableScan(
            table=branch["table"],
            alias=branch.get("alias"),
        )

    elif "columns" in branch and "child" in branch:
        return Projection(
            columns=tuple(ResolvedColumn(**col) for col in branch["columns"]),
            child=from_avro_node(branch["child"]),
        )

    elif "left" in branch and "right" in branch:
        return Product(
            left=from_avro_node(branch["left"]),
            right=from_avro_node(branch["right"]),
        )

    else:
        raise ValueError(f"Unsupported Avro AlgebraNode representation: {branch!r}")


# TODO: structure this like in masterproject with nodes that inherit from an abstract algebra node etc, try to map basic relational algebra
# Stage 3: Logical representations (independent of ANTLR)


@dataclass(frozen=True)
class ColumnRef(BaseModel):
    name: str
    qualifier: str | None = None


@dataclass(frozen=True)
class TableRef(BaseModel):
    name: str
    alias: str | None = None


@dataclass(frozen=True)
class UnresolvedQuery(BaseModel):
    columns: list[ColumnRef]
    tables: list[TableRef]


@dataclass(frozen=True)
class ResolvedColumn(BaseModel):
    table: str
    name: str
    type: str


@dataclass(frozen=True)
class ResolvedQuery(BaseModel):
    columns: list[ResolvedColumn]
    tables: list[TableRef]


# TODO move this to catalog component
# Database catalog

CATALOG = {
    "employees": {
        "id": "INTEGER",
        "name": "VARCHAR",
        "salary": "DECIMAL",
    },
    "departments": {
        "id": "INTEGER",
        "name": "VARCHAR",
    },
}


# Stage 1: Parse SQL, then extract its syntax
class SyntaxExtractor(SchofSQLVisitor):
    def visitQuery(self, ctx: SchofSQLParser.QueryContext):
        return UnresolvedQuery(
            columns=self.visit(ctx.selectList()),
            tables=self.visit(ctx.tableList()),
        )

    def visitSelectList(self, ctx: SchofSQLParser.QueryContext):
        return [self.visit(c) for c in ctx.columnRef()]

    def visitTableList(self, ctx: SchofSQLParser.QueryContext):
        return [self.visit(t) for t in ctx.tableRef()]

    def visitColumnRef(self, ctx: SchofSQLParser.QueryContext):
        return ColumnRef(
            name=ctx.column.text,
            qualifier=ctx.table.text if ctx.table else None,
        )

    def visitTableRef(self, ctx: SchofSQLParser.QueryContext):
        return TableRef(
            name=ctx.name.text,
            alias=ctx.alias.text if ctx.alias else None,
        )


def parse_sql(sql: str) -> UnresolvedQuery:
    lexer = SchofSQLLexer(InputStream(sql))
    tokens = CommonTokenStream(lexer)
    parser = SchofSQLParser(tokens)

    # Do not proceed when ANTLR reports syntax errors.
    tree = parser.query()
    if parser.getNumberOfSyntaxErrors() > 0:
        raise ValueError("Invalid SQL syntax")

    return SyntaxExtractor().visit(tree)  # type:ignore


# Stage 2: Semantic analysis
# TODO this should communicate with the catalog in the future
# TODO add custom errors in the future that are returned to the client
# TODO also need to add type compatibility, alias resolution and scopes for full support
def analyze(query: UnresolvedQuery) -> ResolvedQuery:
    scope = {}

    # Register tables and aliases.
    for table_ref in query.tables:
        table_name = table_ref.name.lower()

        if table_name not in CATALOG:
            raise ValueError(f"Unknown table: {table_ref.name}")

        qualifier = (table_ref.alias or table_ref.name).lower()

        if qualifier in scope:
            raise ValueError(f"Duplicate table alias: {qualifier}")

        scope[qualifier] = (table_name, CATALOG[table_name])

    resolved_columns = []

    for col in query.columns:
        name = col.name.lower()

        if col.qualifier:
            qualifier = col.qualifier.lower()

            if qualifier not in scope:
                raise ValueError(f"Unknown table or alias: {col.qualifier}")

            table_name, schema = scope[qualifier]

            if name not in schema:
                raise ValueError(f"Unknown column: {col.qualifier}.{col.name}")

            resolved_columns.append(ResolvedColumn(table=table_name, name=name, type=schema[name]))

        else:
            # An unqualified column must resolve uniquely.
            matches = [(table_name, schema[name]) for table_name, schema in scope.values() if name in schema]

            if not matches:
                raise ValueError(f"Unknown column: {col.name}")

            if len(matches) > 1:
                raise ValueError(f"Ambiguous column: {col.name}")

            table_name, column_type = matches[0]
            resolved_columns.append(ResolvedColumn(table=table_name, name=name, type=column_type))

    return ResolvedQuery(columns=resolved_columns, tables=query.tables)


# Stage 4: Translate resolved query to relational algebra
# TODO this should be recursive, check master project for how impl done there
# TODO: Check Algebrabuilder and translate that logic to here, this should happen in resolved query also
def translate(query: ResolvedQuery):
    relations = [TableScan(table=table.name.lower()) for table in query.tables]

    source = relations[0]

    for relation in relations[1:]:
        source = Product(left=source, right=relation)

    return Projection(
        columns=tuple(query.columns),
        child=source,
    )


# Stage 5: Optimize relational algebra
# TODO: send of to optimizer: check on how done in master project and how can be defined in avro idl / if it allows "recursive definitions"
# TODO: impl simple, and interaction with catalog snapshot
def optimize(plan):
    # No rewrites needed for this minimal query.
    return plan


# TODO transfrom catalog to static summary / snapshot use the snapshot for the query validation not queries on the catalog directly
# helps for excahnge over api, over lifetime of transaction a snapshot of the catalog is used!
# tend to put a lot of logic in the catalog


def compile_sql(sql: str):
    syntax = parse_sql(sql)
    resolved = analyze(syntax)
    algebra = translate(resolved)
    # algebra.print_tree(indent=1)
    # optimized = optimize(algebra)
    print(to_avro_node(algebra))

    return algebra


# TODO: Language features: Relational
# - Minimal DDL (Table, pk, fk??, indexes)
# - DQL (WHERE, TXStart, TXEnd)
# - DML (Insert, Update, Delete)

if __name__ == "__main__":
    plan = compile_sql("SELECT e.name, d.id FROM employees e, departments d")

    print(plan)
    plan.print_tree(indent=1)

    with open("../../protocol/algebranode.avsc") as f:
        schema = avro.schema.parse(f.read())

    node_data = to_avro_node(plan)
    print(node_data)

    buffer = io.BytesIO()
    writer = avro.io.DatumWriter(schema)
    writer.write(node_data, avro.io.BinaryEncoder(buffer))

    encoded = buffer.getvalue()
    print(encoded)

    data = io.BytesIO(encoded)
    reader = avro.io.DatumReader(writers_schema=schema, readers_schema=schema)
    res = reader.read(decoder=avro.io.BinaryDecoder(data))
    print(res)

    deserialized = from_avro_node(res)
    deserialized.print_tree(indent=1)
    # print(compile_sql("SELECT missing FROM employees;"))
