from dataclasses import dataclass

from antlr4 import CommonTokenStream, InputStream
from generated.SchofSQLLexer import SchofSQLLexer
from generated.SchofSQLParser import SchofSQLParser
from generated.SchofSQLVisitor import SchofSQLVisitor

"""
Overall Idea of Query Processing:
1. ANTLR parse tree: syntax-oriented and grammar-specific.
2. Semantic analyzer: resolves names, checks types and scopes.
3. Resolved logical representation: schema-aware and independent of ANTLR.
4. Relational algebra translator: translates resolved constructs into operators.
5. Optimizer: rewrites the algebra while preserving semantics.
"""


# TODO: structure this like in masterproject with nodes that inherit from an abstract algebra node etc, try to map basic relational algebra
# Stage 3: Logical representations (independent of ANTLR)
@dataclass(frozen=True)
class UnresolvedQuery:
    column: str
    table: str


@dataclass(frozen=True)
class ResolvedQuery:
    table: str
    column: str
    column_type: str


@dataclass(frozen=True)
class Relation:
    table: str


@dataclass(frozen=True)
class ColumnRef:
    table: str
    column: str
    type: str


@dataclass(frozen=True)
class Projection:
    columns: tuple[ColumnRef, ...]
    child: object


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
    def visitQuery(self, ctx: SchofSQLParser.QueryContext) -> UnresolvedQuery:  # type: ignore
        return UnresolvedQuery(
            column=ctx.column.text,  # type: ignore
            table=ctx.table.text,  # type: ignore
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
    table = query.table.lower()
    column = query.column.lower()

    if table not in CATALOG:
        raise ValueError(f"Unknown table: {query.table}")

    schema = CATALOG[table]

    if column not in schema:
        raise ValueError(f"Unknown column: {query.column} in table {table}")

    return ResolvedQuery(
        table=table,
        column=column,
        column_type=schema[column],
    )


# Stage 4: Translate resolved query to relational algebra
# TODO this should be recursive, check master project for how impl done there
def translate(query: ResolvedQuery):
    source = Relation(query.table)

    column = ColumnRef(
        table=query.table,
        column=query.column,
        type=query.column_type,
    )

    return Projection(columns=(column,), child=source)


# Stage 5: Optimize relational algebra
# TODO: send of to optimizer: check on how done in master project and how can be defined in avro idl / if it allows "recursive definitions"
def optimize(plan):
    # No rewrites needed for this minimal query.
    return plan


def compile_sql(sql: str):
    syntax = parse_sql(sql)
    resolved = analyze(syntax)
    algebra = translate(resolved)
    optimized = optimize(algebra)

    return optimized


if __name__ == "__main__":
    print(compile_sql("SELECT name FROM employees;"))
    print(compile_sql("SELECT missing FROM employees;"))
