from __future__ import annotations

import io
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Literal, TypeAlias

import avro.io
import avro.schema
from antlr4 import CommonTokenStream, InputStream
from antlr4.error.ErrorListener import ErrorListener
from generated.SchofSQLLexer import SchofSQLLexer
from generated.SchofSQLParser import SchofSQLParser
from generated.SchofSQLVisitor import SchofSQLVisitor
from pydantic import BaseModel, ConfigDict


class SQLSyntaxError(Exception):
    pass


class RaisingErrorListener(ErrorListener):
    def syntaxError(
        self,
        recognizer,
        offendingSymbol,
        line,
        column,
        msg,
        e,
    ):
        raise SQLSyntaxError(f"Syntax error at line {line}, column {column}: {msg}")


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


class Selection(AbstractAlgebraNode):
    kind: Literal["Selection"] = "Selection"
    expr: Expr
    child: AbstractAlgebraNode

    def children(self) -> list[AbstractAlgebraNode]:
        return [self.child]

    def label(self) -> str:
        return f"σ[{pretty(self.expr)}]"


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


AllowedValue: TypeAlias = str | int | float | None


class Expr(BaseModel, ABC):
    model_config = ConfigDict(frozen=True)


def pretty(expr: Expr) -> str:
    if isinstance(expr, ColumnRef):
        return str(expr)

    if isinstance(expr, LiteralExpr):
        return repr(expr.value)

    if isinstance(expr, BinOp):
        return f"({pretty(expr.left)} {expr.op} {pretty(expr.right)})"

    if isinstance(expr, UnOp):
        return f"({expr.op} {pretty(expr.expr)})"

    raise TypeError(f"Unknown expression type: {type(expr).__name__}")


class ColumnRef(Expr):
    name: str
    qualifier: str | None = None

    def __str__(self) -> str:
        if self.qualifier:
            return f"{self.qualifier}.{self.name}"
        return self.name


class LiteralExpr(Expr):
    value: AllowedValue


class BinOp(Expr):
    op: str
    left: Expr
    right: Expr


class UnOp(Expr):
    op: str
    expr: Expr


class TableRef(Expr):
    name: str
    alias: str | None = None


@dataclass(frozen=True)
class UnresolvedQuery(BaseModel):
    columns: list[ColumnRef]
    tables: list[TableRef]
    where: Expr | None = None


@dataclass(frozen=True)
class ResolvedColumn(BaseModel):
    table: str
    name: str
    type: str


@dataclass(frozen=True)
class ResolvedQuery(BaseModel):
    columns: list[ResolvedColumn]
    tables: list[TableRef]
    where: Expr | None = None


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
    def visitQuery(self, ctx: SchofSQLParser.QueryContext):  # type: ignore
        return UnresolvedQuery(
            columns=self.visit(ctx.selectList()),  # type: ignore
            tables=self.visit(ctx.tableList()),  # type: ignore
            where=self.visit(ctx.whereClause()) if ctx.whereClause() else None,  # type: ignore
        )

    def visitSelectList(self, ctx: SchofSQLParser.SelectListContext):  # type: ignore
        # print("Select list:", ctx.getText())
        # print("Column refs:", ctx.columnRef())
        # result = [self.visit(c) for c in ctx.columnRef()]
        # print("Parsed columns:", result)
        return [self.visit(item) for item in ctx.columnRef()]  # type: ignore

    def visitColumnRef(self, ctx: SchofSQLParser.ColumnRefContext) -> ColumnRef:  # type: ignore
        return ColumnRef(
            name=ctx.column.text,  # type: ignore
            qualifier=ctx.table.text if ctx.table else None,
        )

    def visitTableList(self, ctx: SchofSQLParser.TableListContext):  # type: ignore
        return [self.visit(t) for t in ctx.tableRef()]  # type: ignore

    def visitColumnItem(self, ctx: SchofSQLParser.ColumnItemContext):  # type: ignore
        return ColumnRef(
            name=ctx.column.text,
            qualifier=ctx.table.text if ctx.table else None,
        )

    def visitTableRef(self, ctx: SchofSQLParser.TableRefContext):  # type: ignore
        return TableRef(
            name=ctx.name.text,  # type: ignore
            alias=ctx.alias.text if ctx.alias else None,
        )

    def visitWhereClause(self, ctx: SchofSQLParser.WhereClauseContext):
        return self.visit(ctx.expression())  # type: ignore

    def visitComparisonExpression(  # type: ignore
        self,
        ctx: SchofSQLParser.ComparisonExpressionContext,
    ):
        return BinOp(  # type: ignore
            left=self.visit(ctx.operand(0)),  # type: ignore
            op=ctx.comparisonOperator().getText(),  # type: ignore
            right=self.visit(ctx.operand(1)),  # type: ignore
        )

    def visitColumnOperand(self, ctx: SchofSQLParser.ColumnOperandContext):  # type: ignore
        return ColumnRef(
            name=ctx.column.text,  # type: ignore
            qualifier=ctx.table.text if ctx.table else None,
        )

    def visitValueOperand(self, ctx: SchofSQLParser.ValueOperandContext):
        return self.visit(ctx.value())  # type: ignore

    def visitAndExpression(self, ctx: SchofSQLParser.AndExpressionContext):  # type: ignore
        return BinOp(  # type: ignore
            op="AND",
            left=self.visit(ctx.expression(0)),  # type: ignore
            right=self.visit(ctx.expression(1)),  # type: ignore
        )

    def visitOrExpression(self, ctx: SchofSQLParser.OrExpressionContext):  # type: ignore
        return BinOp(  # type: ignore
            op="OR",
            left=self.visit(ctx.expression(0)),  # type: ignore
            right=self.visit(ctx.expression(1)),  # type: ignore
        )

    def visitNotExpression(self, ctx: SchofSQLParser.NotExpressionContext):  # type: ignore
        return UnOp(op="NOT", expression=self.visit(ctx.expression()))  # type: ignore

    def visitParenExpression(self, ctx: SchofSQLParser.ParenExpressionContext):
        return self.visit(ctx.expression())  # type: ignore

    def visitValue(self, ctx: SchofSQLParser.ValueContext):  # type: ignore
        if ctx.STRING():
            # SQL escapes a quote by doubling it: 'it''s'
            return LiteralExpr(value=ctx.STRING().getText()[1:-1].replace("''", "'"))  # type: ignore

        if ctx.NUMBER():
            text = ctx.NUMBER().getText()  # type: ignore
            return LiteralExpr(value=float(text) if "." in text else int(text))

        if ctx.NULL():
            return LiteralExpr(value=None)

        raise ValueError(f"Unsupported value: {ctx.getText()}")


def parse_sql(sql: str) -> UnresolvedQuery:
    lexer = SchofSQLLexer(InputStream(sql))
    tokens = CommonTokenStream(lexer)
    parser = SchofSQLParser(tokens)
    parser.removeErrorListeners()
    parser.addErrorListener(RaisingErrorListener())
    # Do not proceed when ANTLR reports syntax errors.
    try:
        tree = parser.query()
    except SQLSyntaxError as e:
        print(f"Invalid SQL: {e}")

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

            resolved_columns.append(
                ResolvedColumn(
                    table=table_name,
                    name=name,
                    type=schema[name],
                )
            )

        else:
            # An unqualified column must resolve uniquely.
            matches = [(table_name, schema[name]) for table_name, schema in scope.values() if name in schema]

            if not matches:
                raise ValueError(f"Unknown column: {col.name}")

            if len(matches) > 1:
                raise ValueError(f"Ambiguous column: {col.name}")

            table_name, column_type = matches[0]

            resolved_columns.append(
                ResolvedColumn(
                    table=table_name,
                    name=name,
                    type=column_type,
                )
            )

    return ResolvedQuery(
        columns=resolved_columns,
        tables=query.tables,
        where=query.where,  # TODO: currently where not validated
    )


# Stage 4: Translate resolved query to relational algebra
# TODO this should be recursive, check master project for how impl done there
# TODO: Check Algebrabuilder and translate that logic to here, this should happen in resolved query also
def translate(query: ResolvedQuery):
    relations = [
        TableScan(
            table=table.name.lower(),
            alias=table.alias,
        )
        for table in query.tables
    ]

    if not relations:
        raise ValueError("Query must reference at least one table")

    source = relations[0]

    for relation in relations[1:]:
        source = Product(left=source, right=relation)

    # Apply WHERE before projection
    if query.where is not None:
        source = Selection(
            expr=query.where,
            child=source,
        )

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
    # print(to_avro_node(algebra))

    return algebra


# TODO: Language features: Relational
# - Minimal DDL (Table, pk, fk??, indexes)
# - DQL (WHERE, TXStart, TXEnd)
# - DML (Insert, Update, Delete)

if __name__ == "__main__":
    plan = compile_sql("SELECT e.name, d.id FROM employees e, departments d WHERE e.name = d.id;")

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

    deserialized = from_avro_node(res)  # type: ignore
    deserialized.print_tree(indent=1)
    # print(compile_sql("SELECT missing FROM employees;"))
