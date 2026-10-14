grammar SchofSQL;

query
    : SELECT selectList FROM tableList SEMI? EOF
    ;

selectList
    : columnRef (COMMA columnRef)*
    ;

tableList
    : tableRef (COMMA tableRef)*
    ;

columnRef
    : (table=ID DOT)? column=ID
    ;

tableRef
    : name=ID (AS? alias=ID)?
    ;

SELECT : [sS][eE][lL][eE][cC][tT];
FROM   : [fF][rR][oO][mM];
COMMA  : ',';
DOT    : '.';
SEMI   : ';';
AS     : [aA][sS];
ID     : [a-zA-Z_][a-zA-Z_0-9]*;
WS     : [ \t\r\n]+ -> skip;
