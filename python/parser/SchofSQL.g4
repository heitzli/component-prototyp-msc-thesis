grammar SchofSQL;



dql
    : SELECT selectList FROM tableList whereClause? SEMI?
    ;


// TODO: currently tx only allows single dql query, int the future add ddl, dml and dql
// NOTE: For the moment do not support nested queries
tx 
    : BEGIN TRANSACTION txName? dql txEnd SEMI? EOF 
    ;

query 
    : dql EOF | tx
    ;

selectList
    : columnRef (COMMA columnRef)*
    ;

tableList
    : tableRef (COMMA tableRef)*
    ;

whereClause
    : WHERE expression
    ;

expression
    : expression OR expression                   # orExpression
    | expression AND expression                  # andExpression
    | NOT expression                             # notExpression
    | LPAREN expression RPAREN                   # parenExpression
    | operand comparisonOperator operand         # comparisonExpression
    ;

    operand
    : (table=ID DOT)? column=ID                  # columnOperand
    | value                                      # valueOperand
    ;

value
    : STRING
    | NUMBER
    | NULL
    ;

comparisonOperator
    : EQ
    | NEQ
    | LT
    | LTE
    | GT
    | GTE
    ;

columnRef
    : (table=ID DOT)? column=ID
    ;

tableRef
    : name=ID (AS? alias=ID)?
    ;

txName
    : name=ID
    ;

txEnd
    : COMMIT | ROLLBACK
    ;

SELECT : [sS][eE][lL][eE][cC][tT];
FROM   : [fF][rR][oO][mM];
WHERE  : [wW][hH][eE][rR][eE];
AND    : [aA][nN][dD];
OR     : [oO][rR];
NOT    : [nN][oO][tT];
NULL   : [nN][uU][lL][lL];
BEGIN  : [bB][eE][gG][iI][nN];
TRANSACTION : [tT][rR][aA][nN][sS][aA][cC][tT][iI][oO][nN];
COMMIT : [cC][oO][mM][mM][iI][tT];
ROLLBACK : [rR][oO][lL][lL][bB][aA][cC][kK];

EQ     : '=';
NEQ    : '<>' | '!=';
LTE    : '<=';
GTE    : '>=';
LT     : '<';
GT     : '>';

LPAREN : '(';
RPAREN : ')';

NUMBER : [0-9]+ ('.' [0-9]+)?;
STRING : '\'' ('\'\'' | ~'\'')* '\'';

COMMA  : ',';
DOT    : '.';
SEMI   : ';';
AS     : [aA][sS];
ID     : [a-zA-Z_][a-zA-Z_0-9]*;
WS     : [ \t\r\n]+ -> skip;
