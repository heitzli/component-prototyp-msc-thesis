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


# TODO: add meta field to all avro records where can send arbitraty map for customization etc


"""
Overall Idea of Query Processing:
1. ANTLR parse tree: syntax-oriented and grammar-specific.
2. Semantic analyzer: resolves names, checks types and scopes.
3. Resolved logical representation: schema-aware and independent of ANTLR.
4. Relational algebra translator: translates resolved constructs into operators.
5. Optimizer: rewrites the algebra while preserving semantics.
"""

# TODO: compare with algebra presented in DSC, missing for example aggregations / group by
# TODO: need implementation for Union and difference
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


# NOTE is transaction part of algebra? or should this happen somewhere else?
class Transaction(AbstractAlgebraNode):
    kind: Literal["Transaction"] = "Transaction"
    name: str | None = None
    statements: AbstractAlgebraNode  # TODO support for multi statement tx
    end: Literal["ROLLBACK"] | Literal["COMMIT"]

    def children(self) -> list[AbstractAlgebraNode]:
        return [self.statements]

    def label(self) -> str:
        if self.name:
            return f"Transaction(name={self.name}, end={self.end})"
        else:
            return f"Transaction(end={self.end})"


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


class InsertNode(AbstractAlgebraNode):
    kind: Literal["Insert"] = "Insert"
    table: str
    values: list[tuple[AllowedValue, ...]]

    def children(self) -> list[AbstractAlgebraNode]:
        return []

    def label(self) -> str:
        return f"Insert(table={self.table}, values={self.values}"


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


def to_avro_expr(expr: Expr) -> dict:
    if isinstance(expr, LiteralExpr):
        branch = {"value": expr.value}

    elif isinstance(expr, BinOp):
        branch = {"op": expr.op, "left": to_avro_expr(expr.left), "right": to_avro_expr(expr.right)}

    elif isinstance(expr, UnOp):
        branch = {"op": expr.op, "expr": to_avro_expr(expr.expr)}

    elif isinstance(expr, ColumnRef):
        branch = {"name": expr.name, "qualifier": expr.qualifier}

    else:
        raise ValueError(f"Unsupported expression: {type(expr).__name__}")

    return {"expr": branch}


def to_avro_node(node: AbstractAlgebraNode) -> dict:
    if isinstance(node, TableScan):
        branch = {"table": node.table, "alias": node.alias}

    elif isinstance(node, Projection):
        branch = {
            "columns": [{"table": col.table, "name": col.name, "type": col.type} for col in node.columns],
            "child": to_avro_node(node.child),
        }

    elif isinstance(node, Product):
        branch = {"left": to_avro_node(node.left), "right": to_avro_node(node.right)}

    elif isinstance(node, Selection):
        branch = {"child": to_avro_node(node.child), "expr": to_avro_expr(node.expr)}

    elif isinstance(node, Transaction):
        branch = {"name": node.name, "statements": to_avro_node(node.statements), "end": node.end}

    else:
        raise ValueError(f"Unsupported algebra node: {type(node).__name__}")

    return {"node": branch}


def from_avro_node(data: dict) -> AbstractAlgebraNode:
    branch = data.get("node", data)

    # Handle Avro union representations if present.
    if isinstance(branch, dict) and len(branch) == 1:
        branch_name = next(iter(branch))
        if branch_name in {"TableScan", "Projection", "Product", "Selection", "TransactionNode"}:
            branch = branch[branch_name]

    if "table" in branch and "alias" in branch:
        return TableScan(table=branch["table"], alias=branch.get("alias"))

    elif "columns" in branch and "child" in branch:
        return Projection(
            columns=tuple(ResolvedColumn(**col) for col in branch["columns"]), child=from_avro_node(branch["child"])
        )

    elif "left" in branch and "right" in branch:
        return Product(left=from_avro_node(branch["left"]), right=from_avro_node(branch["right"]))

    elif "child" in branch and "expr" in branch:
        return Selection(child=from_avro_node(branch["child"]), expr=from_avro_expr(branch["expr"]))

    elif "statements" in branch and "end" in branch:
        return Transaction(name=branch.get("name"), statements=from_avro_node(branch["statements"]), end=branch["end"])

    else:
        raise ValueError(f"Unsupported Avro AlgebraNode representation: {branch!r}")


def from_avro_expr(data: dict) -> Expr:
    branch = data.get("expr", data)

    if isinstance(branch, dict) and len(branch) == 1:
        branch_name = next(iter(branch))
        if branch_name in {"Literal", "BinOp", "UnOp", "ColumnRef"}:
            branch = branch[branch_name]

    if "value" in branch:
        return LiteralExpr(value=branch["value"])

    elif "op" in branch and "left" in branch and "right" in branch:
        return BinOp(op=branch["op"], left=from_avro_expr(branch["left"]), right=from_avro_expr(branch["right"]))

    elif "op" in branch and "expr" in branch:
        return UnOp(op=branch["op"], expr=from_avro_expr(branch["expr"]))

    elif "name" in branch and ("qualifier" in branch or set(branch) == {"name"}):
        return ColumnRef(name=branch["name"], qualifier=branch.get("qualifier"))

    else:
        raise ValueError(f"Unsupported Avro Expr representation: {branch!r}")


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


class UnresolvedInsert(BaseModel):
    columns: list[ColumnRef]
    values: list[tuple[AllowedValue, ...]]


class ResolvedInsert(BaseModel):
    columns: list[ResolvedColumn]
    values: list[tuple[AllowedValue, ...]]


class UnresolvedTransaction(BaseModel):
    tx_name: str | None = None
    # TODO: For the moment single dql query, in the future need to support multiple statements
    statements: UnresolvedQuery | UnresolvedInsert
    tx_end: Literal["COMMIT"] | Literal["ROLLBACK"]


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


class ResolvedTransaction(BaseModel):
    tx_name: str | None = None
    # TODO: For the moment single dql query, in the future need to support multiple statements
    statements: ResolvedQuery | ResolvedInsert
    tx_end: Literal["COMMIT"] | Literal["ROLLBACK"]


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
    def visitQuery(self, ctx: SchofSQLParser.QueryContext) -> None:
        if ctx.dql():
            return self.visit(ctx.dql())  # type: ignore

        if ctx.tx():
            return self.visit(ctx.tx())  # type: ignore

        if ctx.insert():
            return self.visit(tree=ctx.insert())  # type: ignore

        raise ValueError("Unsupported query")

    def visitInsert(self, ctx: SchofSQLParser.InsertContext):
        return UnresolvedInsert(columns=self.visit(ctx.selectList()), values=self.visit(ctx.insertList()))

    def visitInsertList(self, ctx: SchofSQLParser.InsertListContext):
        return [self.visit(item) for item in ctx.insertTuple()]

    def visitInsertTuple(self, ctx: SchofSQLParser.InsertTupleContext):
        return tuple(self.visit(item) for item in ctx.insertValue())

    def visitInsertValue(self, ctx: SchofSQLParser.InsertValueContext):
        return self.visit(ctx.valueInsert())

    def visitValueInsert(self, ctx: SchofSQLParser.ValueInsertContext):
        if ctx.STRING():
            # SQL escapes a quote by doubling it: 'it''s'
            return str(ctx.STRING().getText()[1:-1].replace("''", "'"))  # type: ignore

        if ctx.NUMBER():
            text = ctx.NUMBER().getText()  # type: ignore
            return float(text) if "." in text else int(text)

        if ctx.NULL():
            return None

        raise ValueError(f"Unsupported value: {ctx.getText()}")

    def visitDql(self, ctx: SchofSQLParser.DqlContext) -> UnresolvedQuery:  # type: ignore
        return UnresolvedQuery(
            columns=self.visit(ctx.selectList()),  # type: ignore
            tables=self.visit(ctx.tableList()),  # type: ignore
            where=(self.visit(ctx.whereClause()) if ctx.whereClause() else None),  # type: ignore
        )

    def visitTx(self, ctx: SchofSQLParser.TxContext) -> UnresolvedTransaction:  # type:ignore

        if ctx.dql():
            query: UnresolvedQuery = self.visit(ctx.dql())  # type: ignore
        elif ctx.insert():
            query: UnresolvedInsert = self.visit(ctx.insert())  # type: ignore
        else:
            raise ValueError("Transaction must contain a supported query")

        return UnresolvedTransaction(  # type: ignore
            tx_name=ctx.txName().getText() if ctx.txName() else None,  # type: ignore
            statements=query,
            tx_end=self.visit(ctx.txEnd()),  # type: ignore
        )

    def visitTxEnd(self, ctx: SchofSQLParser.TxEndContext) -> str:  # type: ignore
        return ctx.getText().upper()

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

    def visitColumnItem(self, ctx: SchofSQLParser.ColumnItemContext) -> ColumnRef:  # type: ignore
        return ColumnRef(
            name=ctx.column.text,
            qualifier=ctx.table.text if ctx.table else None,
        )

    def visitTableRef(self, ctx: SchofSQLParser.TableRefContext) -> TableRef:  # type: ignore
        return TableRef(
            name=ctx.name.text,  # type: ignore
            alias=ctx.alias.text if ctx.alias else None,
        )

    def visitWhereClause(self, ctx: SchofSQLParser.WhereClauseContext) -> None:
        return self.visit(ctx.expression())  # type: ignore

    def visitComparisonExpression(  # type: ignore
        self,
        ctx: SchofSQLParser.ComparisonExpressionContext,
    ) -> BinOp:
        return BinOp(  # type: ignore
            left=self.visit(ctx.operand(0)),  # type: ignore
            op=ctx.comparisonOperator().getText(),  # type: ignore
            right=self.visit(ctx.operand(1)),  # type: ignore
        )

    def visitColumnOperand(self, ctx: SchofSQLParser.ColumnOperandContext) -> ColumnRef:  # type: ignore
        return ColumnRef(
            name=ctx.column.text,  # type: ignore
            qualifier=ctx.table.text if ctx.table else None,
        )

    def visitValueOperand(self, ctx: SchofSQLParser.ValueOperandContext) -> None:
        return self.visit(ctx.value())  # type: ignore

    def visitAndExpression(self, ctx: SchofSQLParser.AndExpressionContext) -> BinOp:  # type: ignore
        return BinOp(  # type: ignore
            op="AND",
            left=self.visit(ctx.expression(0)),  # type: ignore
            right=self.visit(ctx.expression(1)),  # type: ignore
        )

    def visitOrExpression(self, ctx: SchofSQLParser.OrExpressionContext) -> BinOp:  # type: ignore
        return BinOp(  # type: ignore
            op="OR",
            left=self.visit(ctx.expression(0)),  # type: ignore
            right=self.visit(ctx.expression(1)),  # type: ignore
        )

    def visitNotExpression(self, ctx: SchofSQLParser.NotExpressionContext) -> UnOp:  # type: ignore
        return UnOp(op="NOT", expr=self.visit(ctx.expression()))  # type: ignore

    def visitParenExpression(self, ctx: SchofSQLParser.ParenExpressionContext) -> None:
        return self.visit(ctx.expression())  # type: ignore

    def visitValue(self, ctx: SchofSQLParser.ValueContext) -> LiteralExpr:  # type: ignore
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
        raise ValueError(f"Invalid SQL: {e}")

    return SyntaxExtractor().visit(tree)  # type:ignore


# Stage 2: Semantic analysis
# TODO this should communicate with the catalog in the future
# TODO add custom errors in the future that are returned to the client
# TODO also need to add type compatibility, alias resolution and scopes for full support
# HACK: not nice with two output types, find better way
def analyze(query: UnresolvedQuery | UnresolvedInsert) -> ResolvedQuery | ResolvedInsert:
    if isinstance(query, UnresolvedQuery):
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

        return ResolvedQuery(
            columns=resolved_columns,
            tables=query.tables,
            where=query.where,  # TODO: currently where not validated
        )

    if isinstance(query, UnresolvedInsert):
        # HACK: currently not checking just return as is
        return ResolvedInsert(
            values=query.values,
            columns=[ResolvedColumn(table="TODO", name=c.name, type="TODO") for c in query.columns],
        )


def analyze_query(
    query: UnresolvedQuery | UnresolvedTransaction | UnresolvedInsert,
) -> ResolvedQuery | ResolvedTransaction:
    if isinstance(query, UnresolvedQuery):
        return analyze(query=query)

    elif isinstance(query, UnresolvedTransaction):
        return ResolvedTransaction(
            tx_name=query.tx_name, statements=analyze(query=query.statements), tx_end=query.tx_end
        )

    else:
        raise ValueError("Unsupported Query Type for query validation")


# Stage 4: Translate resolved query to relational algebra
# TODO this should be recursive, check master project for how impl done there
# TODO: Check Algebrabuilder and translate that logic to here, this should happen in resolved query also
def translate(query: ResolvedQuery | ResolvedInsert):
    if isinstance(query, ResolvedQuery):
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
    if isinstance(query, ResolvedInsert):
        # HACK get the table properly
        return InsertNode(table=query.columns[0].table, values=query.values)


def translate_query(query: ResolvedQuery | ResolvedTransaction) -> Transaction:
    if isinstance(query, ResolvedQuery):
        # TODO: check if this is the right place to implicitly translate single statements into transactions or if this needs to be done somewhere else if multiple statements are incoming
        # or if this is part of the client issueing the queries (e.g autocommit=True in python driver)
        return Transaction(statements=translate(query=query), end="COMMIT")
    elif isinstance(query, ResolvedTransaction):
        return Transaction(name=query.tx_name, statements=translate(query=query.statements), end=query.tx_end)


# Stage 5: Optimize relational algebra
# TODO: send of to optimizer: check on how done in master project and how can be defined in avro idl / if it allows "recursive definitions"
# TODO: impl simple, and interaction with catalog snapshot
def optimize(plan):
    # No rewrites needed for this minimal query.
    return plan


# TODO transfrom catalog to static summary / snapshot use the snapshot for the query validation not queries on the catalog directly
# helps for excahnge over api, over lifetime of transaction a snapshot of the catalog is used!
# tend to put a lot of logic in the catalog


def compile_sql(sql: str) -> Transaction:
    syntax = parse_sql(sql)
    resolved = analyze_query(syntax)
    algebra = translate_query(resolved)
    # algebra.print_tree(indent=1)
    # optimized = optimize(algebra)
    # print(to_avro_node(algebra))

    return algebra


# TODO: Language features: Relational
# - Minimal DDL (Table, pk, fk??, indexes)
# - DQL (WHERE, TXStart, TXEnd)
# - DML (Insert, Update, Delete)
#

# TODO: adjust avro idl to support implemented where clauses

if __name__ == "__main__":
    plan = compile_sql(
        sql="""
        BEGIN TRANSACTION INSERT INTO employees (id, name, salary) VALUES (1, 'John', 50000), (2, 'Mary', 100000) COMMIT;
        """
    )

    # print(plan)
    plan.print_tree(indent=1)
    """
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
    """
