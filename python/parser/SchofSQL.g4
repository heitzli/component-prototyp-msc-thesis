grammar SchofSQL;

query
    : SELECT column=ID FROM table=ID SEMI? EOF
    ;

SELECT : [sS][eE][lL][eE][cC][tT];
FROM   : [fF][rR][oO][mM];
SEMI   : ';';
ID     : [a-zA-Z_][a-zA-Z_0-9]*;
WS     : [ \t\r\n]+ -> skip;
