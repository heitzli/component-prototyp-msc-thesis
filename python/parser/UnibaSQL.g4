grammar UnibaSQL;


dql
    : SELECT selectList FROM tableList whereClause? SEMI?
    ;

// TODO check if selectlist ok or need own defined
insert
    : INSERT INTO table=ID LPAREN selectList RPAREN VALUES insertList SEMI?
    ;
// TODO: currently tx only allows single dql or insert query, int the future add ddl, dml and dql and multiple per tx
// NOTE: For the moment do not support nested queries
tx
    : BEGIN TRANSACTION txName? (dql | insert) txEnd SEMI? EOF
    ;

query
    : insert EOF | dql EOF | tx
    ;

insertList
    : insertTuple (COMMA insertTuple)*
    ;

selectList
    : columnRef (COMMA columnRef)*
    ;

tableList
    : tableRef (COMMA tableRef)*
    ;

insertTuple
    : LPAREN insertValue (COMMA insertValue)* RPAREN
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

valueInsert
    : STRING
    | NUMBER
    | NULL
    ;

insertValue
    :   val=valueInsert
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
INSERT : [iI][nN][sS][eE][rR][tT];
INTO   : [iI][nN][tT][oO];
VALUES : [vV][aA][lL][uU][eE][sS];

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
