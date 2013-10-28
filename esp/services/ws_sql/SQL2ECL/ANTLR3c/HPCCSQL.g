grammar HPCCSQL;

options
{
  language=C;
  output=AST;
  backtrack=true;
  ASTLabelType = pANTLR3_BASE_TREE;
}

tokens
{
    TOKEN_ROOT;
    TOKEN_SELECT_STATEMENT;
    TOKEN_CALL_STATEMENT;
    TOKEN_FROM_LIST;
    TOKEN_FROM_TABLE;
    TOKEN_PROC_NAME;
    TOKEN_PROC_PARAMS;
    TOKEN_ALIAS;
    TOKEN_INNER_JOIN;
    TOKEN_OUTTER_JOIN;
    TOKEN_INDEX_HINT;
    TOKEN_AVOID_INDEX;
    TOKEN_COLUMN;
    TOKEN_LISTEXP;
    TOKEN_FUNCEXP;
    TOKEN_PARAMPLACEHOLDER;
    TOKEN_COLUMNWILDCARD;
}

@header
{
}
@members
{
}
@lexer::includes
{
}
@lexer::apifuncs
{
}
@lexer::header
{
}
@lexer::members
{
}
@parser::apifuncs
{
}

fragment Ai :  'a' | 'A';
fragment Bi :  'b' | 'B';
fragment Ci :  'c' | 'C';
fragment Di :  'd' | 'D';
fragment Ei :  'e' | 'E';
fragment Fi :  'f' | 'F';
fragment Gi :  'g' | 'G';
fragment Hi :  'h' | 'H';
fragment Ii :  'i' | 'I';
fragment Ji :  'j' | 'J';
fragment Ki :  'k' | 'K';
fragment Li :  'l' | 'L';
fragment Mi :  'm' | 'M';
fragment Ni :  'n' | 'N';
fragment Oi :  'o' | 'O';
fragment Pi :  'p' | 'P';
fragment Qi :  'q' | 'Q';
fragment Ri :  'r' | 'R';
fragment Si :  's' | 'S';
fragment Ti :  't' | 'T';
fragment Ui :  'u' | 'U';
fragment Vi :  'v' | 'V';
fragment Wi :  'w' | 'W';
fragment Xi :  'x' | 'X';
fragment Yi :  'y' | 'Y';
fragment Zi :  'z' | 'Z';

ADD_SYM     : Ai Di Di  ;
ALL         : Ai Li Li  ;
ANY         : Ai Ni Yi ;
AS_SYM      : Ai Si  ;
ASC         : Ai Si Ci  ;
AT_SYM      : Ai Ti  ;
AVG         : Ai Vi Gi;
BETWEEN     : Bi Ei Ti Wi Ei Ei Ni  ;
BOOL_SYM    : Bi Oi Oi Li  ;
BOOLEAN_SYM : Bi Oi Oi Li Ei Ai Ni  ;
BY_SYM      : Bi Yi ;
CALL_SYM    : Ci Ai Li Li  ;
COLUMN_SYM  : Ci Oi Li Ui Mi Ni  ;
CONTAINS_SYM : Ci Oi Ni Ti Ai Ii Ni Si  ;
COUNT       : Ci Oi Ui Ni Ti  ;
DESC        : Di Ei Si Ci  ;
DISTINCT    : Di Ii Si Ti Ii Ni Ci Ti ;
FALSE_SYM   : Fi Ai Li Si Ei ;
FOR_SYM     : Fi Oi Ri  ;
FROM        : Fi Ri Oi Mi  ;
GROUP_SYM   : Gi Ri Oi Ui Pi  ;
HAVING      : Hi Ai Vi Ii Ni Gi  ;
IN_SYM      : Ii Ni  ;
INDEX_SYM   : Ii Ni Di Ei Xi  ;
INNER_SYM   : Ii Ni Ni Ei Ri  ;
IS_SYM      : Ii Si  ;
JOIN_SYM    : Ji Oi Ii Ni  ;
KEY_SYM     : Ki Ei Yi  ;
KEYS        : Ki Ei Yi Si  ;
LAST_SYM    : Li Ai Si Ti  ;
LEFT        : Li Ei Fi Ti  ;
LIMIT       : Li Ii Mi Ii Ti  ;
LOWER       : (Li Oi Wi Ei Ri) | (Li Ci Ai Si Ei) ;
MAX_SYM     : Mi Ai Xi  ;
MIN_SYM     : Mi Ii Ni  ;
MOD         : Mi Oi Di  ;
NOT_SYM     : ('_'Ni Oi Ti) | (Ni Oi Ti) | ('!') ;
NULL_SYM    : Ni Ui Li Li  ;
OFFSET_SYM  : Oi Fi Fi Si Ei Ti  ;
ON          : Oi Ni  ;
ORDER_SYM   : Oi Ri Di Ei Ri  ;
OUT_SYM     : Oi Ui Ti  ;
OUTER       : Oi Ui Ti Ei Ri  ;
POWER       : Pi Oi Wi Ei Ri  ;
SELECT      : Si Ei Li Ei Ci Ti ;
SUM         : Si Ui Mi  ;
TABLE       : Ti Ai Bi Li Ei ;
TRUE_SYM    : Ti Ri Ui Ei;
UPPER       : Ui Pi Pi Ei Ri ;
USE_SYM     : Ui Si Ei  ;
WHERE       : Wi Hi Ei Ri Ei  ;
XOR         : Xi Oi Ri  ;

ISNOTNULL   : ('IS NOT NULL' | 'is not null');
ISNULL      : ('IS NULL' | 'is null');
NOT_IN      : ('NOT IN' | 'not in');

DIVIDE      : (  Di Ii Vi ) | '/' ;
MOD_SYM     : (  Mi Oi Di ) | '%' ;
OR_SYM      : (  Oi Ri ) | '||';
AND_SYM     : (  Ai Ni Di ) | '&&';

ARROW       : '=>' ;
EQ_SYM      : '=' | '<=>' ;
NOT_EQ      : '<>' | '!=' | '~='| '^=';
LET         : '<=' ;
GET         : '>=' ;
SET_VAR     : ':=' ;
SHIFT_LEFT  : '<<' ;
SHIFT_RIGHT : '>>' ;
ALL_FIELDS  : '.*' ;
SQUOTE      : '\'' ;
//DQUOTE      : '\"' ;
DQUOTE      : '"' ;
DOLLAR      : '$' ;
QUESTION    : '?' ;
SEMI        : ';' ;
COLON       : ':' ;
DOT         : '.' ;
COMMA       : ',' ;
ASTERISK    : '*' ;
RPAREN      : ')' ;
LPAREN      : '(' ;
RBRACK      : ']' ;
LBRACK      : '[' ;
LCURLY      : '{' ;
RCURLY      : '}' ;
PLUS        : '+' ;
MINUS       : '-' ;
NEGATION    : '~' ;
VERTBAR     : '|' ;
BITAND      : '&' ;
POWER_OP    : '^' ;
GTH         : '>' ;
LTH         : '<' ;

fragment LETTERS
:
    ( 'A'..'Z' | 'a'..'z')
;

INTEGER_NUM
:
  ('0'..'9')+
;

fragment HEX_DIGIT_FRAGMENT
:
  ( 'a'..'f' | 'A'..'F' | '0'..'9' )
;

HEX_DIGIT
:
  (  '0x'     (HEX_DIGIT_FRAGMENT)+  )
  |
  (  'X' '\'' (HEX_DIGIT_FRAGMENT)+ '\''  )
;

BIT_NUM
:
  (  '0b'    ('0'|'1')+  )
  |
  (  Bi '\'' ('0'|'1')+ '\''  )
;

REAL_NUMBER
:
  (  INTEGER_NUM DOT INTEGER_NUM | INTEGER_NUM DOT | DOT INTEGER_NUM | INTEGER_NUM  )
  (  ('E'|'e') ( PLUS | MINUS )? INTEGER_NUM  )?
;

TEXT_STRING
:
    SQUOTE
    (
        (SQUOTE SQUOTE)
        | ('\\''\'')
        | ~('\'')
    )*
    SQUOTE
;

quoted_id
:
    DQUOTE! ID DQUOTE!
;

//Cannot rewrite lexer rules see above parser rule
//QUOTED_ID
//:
//  DQUOTE! ID DQUOTE!
//;

ID
:
   LETTERS ( ID_FRAGMENT )*
;

fragment ID_FRAGMENT
:
  ( LETTERS | '_' | INTEGER_NUM | '::' )
;

WHITE_SPACE
:
  ( ' '|'\r'|'\t'|'\n' ) {$channel=HIDDEN;}
;

relational_op
:
  EQ_SYM
  | LTH
  | GTH
  | NOT_EQ
  | LET
  | GET
;

list_op
:
    IN_SYM
    | NOT_IN
;

string_literal
:
   TEXT_STRING
;
number_literal
:
   (PLUS | MINUS)? (INTEGER_NUM | REAL_NUMBER)
;

hex_literal
:
  HEX_DIGIT
;

boolean_literal
:
  TRUE_SYM
  | FALSE_SYM
;

bit_literal
:
    BIT_NUM
;

literal_value
:
  (
      string_literal
      | number_literal
      | hex_literal
      | boolean_literal
      | bit_literal
  )
;

functionList
:
  group_functions
  | char_functions
;

char_functions
:
  LOWER
  | UPPER
;

group_functions
:
  AVG
  | COUNT
  | MAX_SYM
  | MIN_SYM
  | SUM
;

query_set_name      : ID ;
schema_name         : ID ;
table_name          : ID ;
quoted_table_name   : quoted_id ;
engine_name         : ID ;
column_name         : ID ;
quoted_column_name  : quoted_id ;
index_name          : ID ;
user_name           : ID ;
function_name       : ID ;
procedure_name      : ID ;
alias
:
  ( AS_SYM )? ID -> ^( TOKEN_ALIAS ID)
;

column_spec
:
    ( quoted_table_name DOT )? quoted_column_name -> ^(TOKEN_COLUMN quoted_column_name quoted_table_name? )
    |( table_name DOT )? column_name -> ^(TOKEN_COLUMN column_name table_name? )
;

expression_list
:
    LPAREN expression ( COMMA expression )* RPAREN
;

expression
:
    orExpression
;

orExpression
:
    andExpression
    (
      OR_SYM^ andExpression
    )*
;

andExpression
:
    relationalExpression
    (
      AND_SYM^ relationalExpression
    )*
;

relationalExpression
:
    additionExpression
    (
      relational_op^
      additionExpression
    )*
;

additionExpression
:
    multiplyExpression
    (
      (PLUS|MINUS)^ multiplyExpression
    )?
;

multiplyExpression
:
    listExpression
    (
      (ASTERISK
      |DIVIDE
      |MOD_SYM
      |POWER_OP)^
      listExpression
    )?
;

listExpression
    :
    unaryExpression
    (
      list_op^
      literalOrPlaceholderExpressionList
    )?
;

unaryExpression
:
    (NEGATION | NOT_SYM )^ simpleExpression
    |
    simpleExpression (ISNOTNULL | ISNULL)^
    |
    simpleExpression
;

simpleExpression
:
    column_spec
    | literal_value
    | function_call
    | parenExpression
    | parameterPlaceHolder
    | literalExpressionList
;

parenExpression
:
  LPAREN^ orExpression RPAREN!
;

literalExpressionList
:
    LPAREN literal_value( COMMA literal_value )* RPAREN -> {$COMMA != NULL}? ^(TOKEN_LISTEXP literal_value+)
    -> ^(TOKEN_LISTEXP literal_value)
;

literalOrPlaceholderExpressionList
:
    LPAREN literalOrPlaceholderValue( COMMA literalOrPlaceholderValue )* RPAREN -> {$COMMA != NULL}? ^(TOKEN_LISTEXP literalOrPlaceholderValue+)
    -> ^(TOKEN_LISTEXP literalOrPlaceholderValue)
;

literalOrPlaceholderValue
:
    literal_value | parameterPlaceHolder
;

function_call
:
    (
      functionList ( LPAREN (functionParam (COMMA functionParam)*)? RPAREN ) ?
    ) -> {$COMMA != NULL}? ^(TOKEN_FUNCEXP functionList functionParam+)
      -> ^(TOKEN_FUNCEXP functionList functionParam)
;

functionParam
:
    (DISTINCT)?
    (
      literal_value
      | column_spec
      | parameterPlaceHolder
    )
;

parameterPlaceHolder
:
  (
    QUESTION
    | DOLLAR LCURLY ID? RCURLY
    | userVariable
  )
  -> ^(TOKEN_PARAMPLACEHOLDER)
;

userVariable
:
  '@' ID
;

table_references
:
  table_reference ( COMMA! table_reference )*
;

table_reference
:
  table_atom |
  table_atom (jointable)+
;

table_list:
  table_atom ( COMMA! table_atom )*
  ;

jointable
:
    INNER_SYM JOIN_SYM table_atom join_condition ->  ^( TOKEN_INNER_JOIN table_atom join_condition )
    |  OUTER JOIN_SYM table_atom join_condition  ->  ^( TOKEN_OUTTER_JOIN table_atom join_condition )
;

table_factor1
:
  table_factor2 (  INNER_SYM JOIN_SYM table_atom join_condition )?
    -> {$JOIN_SYM != NULL}? ^( table_factor2 TOKEN_INNER_JOIN table_atom join_condition )
    -> table_factor2
;

table_factor2
:
  table_factor3 ( OUTER JOIN_SYM table_atom join_condition)?
    -> {$JOIN_SYM != NULL}? ^( table_factor3 TOKEN_OUTTER_JOIN table_atom join_condition )
    -> table_atom
;

table_factor3
:
  table_atom
;

table_atom
:
  table_spec^ (alias)? (index_hint)?
;

join_condition
:
    ON^ expression
;

index_hint
:
  USE_SYM INDEX_SYM LPAREN ( index_name | v='0' ) RPAREN -> {$v != NULL}? ^(TOKEN_AVOID_INDEX index_name)
                                                         -> ^(TOKEN_INDEX_HINT index_name)
;

index_list
:
  index_name (COMMA index_name)*
;

root_statement
@init
{
}

:
  ( data_manipulation_statements)(SEMI)? -> ^(TOKEN_ROOT data_manipulation_statements)
;

data_manipulation_statements
:
    select_statement -> ^( TOKEN_SELECT_STATEMENT select_statement)
  | call_statement -> ^( TOKEN_CALL_STATEMENT call_statement)
;

select_statement
:
        select_portion
        ( from_portion
          ( where_clause )?
          ( groupby_clause )?
          ( having_clause )?
        )

        ( orderby_clause )?
        ( limit_clause )?
;

select_portion
:
    SELECT^ ( DISTINCT )? select_list
;

from_portion
:
    FROM^ table_references
;

where_clause
:
  WHERE^ expression
;

groupby_clause
:
  GROUP_SYM^ BY_SYM! column_spec (COMMA! column_spec)*
;

having_clause
:
  HAVING^ expression
;

orderby_clause
:
  ORDER_SYM^ BY_SYM! orderby_item (COMMA! orderby_item)*
;

orderby_item
:
  column_spec^ (ASC! | DESC)?
;


limit_clause
:
  LIMIT^ row_count (OFFSET_SYM! offset)?
;
offset
:
  INTEGER_NUM
;

row_count
:
  INTEGER_NUM;

select_list
:
    select_item
    (COMMA! select_item)*
;

select_item
:
    column_spec^ (alias)?
    | string_literal^ (alias)?
    | number_literal^ (alias)?
    | hex_literal^ (alias)?
    | boolean_literal^ (alias)?
    | function_call^ (alias)?
    | column_wildcard
;

column_list
:
  LPAREN column_spec (COMMA column_spec)* RPAREN
;

subquery
:
  LPAREN select_statement RPAREN
;

table_spec
:
  table_name
  | quoted_table_name
;

column_wildcard
:
    ASTERISK -> TOKEN_COLUMNWILDCARD
;

callParam
:
    string_literal
    | number_literal
    | hex_literal
    | boolean_literal
    | parameterPlaceHolder
;

call_procedure_name_part
:
     (query_set_name DOT)? procedure_name -> ^( TOKEN_PROC_NAME procedure_name query_set_name? )
;

call_procedure_params
:
    callParam ( COMMA callParam)* -> ^( TOKEN_PROC_PARAMS callParam+ )
;

call_statement
:
    CALL_SYM! call_procedure_name_part LPAREN! call_procedure_params? RPAREN!
;

