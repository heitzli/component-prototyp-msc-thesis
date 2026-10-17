grammar SchofSQL;

query
    : SELECT selectList FROM tableList whereClause? SEMI? EOF
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

SELECT : [sS][eE][lL][eE][cC][tT];
FROM   : [fF][rR][oO][mM];
WHERE  : [wW][hH][eE][rR][eE];
AND    : [aA][nN][dD];
OR     : [oO][rR];
NOT    : [nN][oO][tT];
NULL   : [nN][uU][lL][lL];

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
