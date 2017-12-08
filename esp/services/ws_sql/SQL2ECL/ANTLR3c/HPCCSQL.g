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
    TOKEN_CREATE_LOAD_TABLE_STATEMENT;
    TOKEN_CREATE_INDEX_STATEMENT;
    TOKEN_CREATE_TABLE;
    TOKEN_DONOT_OVERWRITE;
    TOKEN_OVERWRITE;
    TOKEN_LOAD_TABLE;
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
    TOKEN_TABLE_SCHEMA;
    TOKEN_COLUMN_DEF_LIST;
    TOKEN_COLUMN_DEF;
    TOKEN_LANDING_ZONE;
    TOKEN_VARIABLE_FILE;
    TOKEN_VAR_SEPERATOR;
    TOKEN_VAR_TERMINATOR;
    TOKEN_VAR_ENCLOSED;
    TOKEN_VAR_ESCAPED;
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
ASCII_SYM   : Ai Si Ii Ii ;
AT_SYM      : Ai Ti  ;
AVG         : Ai Vi Gi ;
BETWEEN     : Bi Ei Ti Wi Ei Ei Ni  ;
BINARY_SYM  : Bi Ii Ni Ai Ri Yi ;
BIT_SYM     : Bi Ii Ti ;
BOOL_SYM    : Bi Oi Oi Li  ;
BOOLEAN_SYM : Bi Oi Oi Li Ei Ai Ni  ;
BY_SYM      : Bi Yi ;
CALL_SYM    : Ci Ai Li Li  ;
CREATE_SYM  : Ci Ri Ei Ai Ti Ei ;
COLUMN_SYM  : Ci Oi Li Ui Mi Ni  ;
COMMENT_SYM : Ci Oi Mi Mi Ei Ni Ti ;
CONTAINS_SYM : Ci Oi Ni Ti Ai Ii Ni Si  ;
COUNT       : Ci Oi Ui Ni Ti  ;
DATA_SYM    : Di Ai Ti Ai ;
DESC        : Di Ei Si Ci  ;
DISTINCT    : Di Ii Si Ti Ii Ni Ci Ti ;
EXISTS_SYM  : Ei Xi Ii Si Ti Si ;
FALSE_SYM   : Fi Ai Li Si Ei ;
FOR_SYM     : Fi Oi Ri ;
FROM        : Fi Ri Oi Mi  ;
GROUP_SYM   : Gi Ri Oi Ui Pi  ;
HAVING      : Hi Ai Vi Ii Ni Gi  ;
IF_SYM      : Ii Fi ;
IGNORE_SYM  : Ii Gi Ni Oi Ri Ei ;
IN_SYM      : Ii Ni ;
INFILE_SYM  : Ii Ni Fi Ii Li Ei ;
INTO_SYM    : Ii Ni Ti Oi ;
INT_SYM     : Ii Ni Ti ;
INTEGER_SYM : Ii Ni Ti Ei Gi Ei Ri ;
INDEX_SYM   : Ii Ni Di Ei Xi  ;
INNER_SYM   : Ii Ni Ni Ei Ri  ;
IS_SYM      : Ii Si  ;
JOIN_SYM    : Ji Oi Ii Ni  ;
KEY_SYM     : Ki Ei Yi  ;
KEYS        : Ki Ei Yi Si  ;
LAST_SYM    : Li Ai Si Ti  ;
LEFT        : Li Ei Fi Ti  ;
LIKE_SYM    : Li Ii Ki Ei  ;
LIMIT       : Li Ii Mi Ii Ti  ;
LOAD_SYM    : Li Oi Ai Di ;
LOCAL_SYM   : Li Oi Ci Ai Li ;
LOWER       : (Li Oi Wi Ei Ri) | (Li Ci Ai Si Ei) ;
MAX_SYM     : Mi Ai Xi  ;
MEDIUMINT   : Mi Ei Di Ii Ui Mi Ii Ni Ti ;
MIN_SYM     : Mi Ii Ni  ;
MOD         : Mi Oi Di  ;
NOT_SYM     : (UNDERSCORE Ni Oi Ti) | (Ni Oi Ti) | ('!') ;
NULL_SYM    : Ni Ui Li Li  ;
OFFSET_SYM  : Oi Fi Fi Si Ei Ti  ;
ON          : Oi Ni  ;
ORDER_SYM   : Oi Ri Di Ei Ri  ;
OUT_SYM     : Oi Ui Ti  ;
OUTER       : Oi Ui Ti Ei Ri  ;
POWER       : Pi Oi Wi Ei Ri  ;
REPLACE_SYM : Ri Ei Pi Li Ai Ci Ei ;
SELECT      : Si Ei Li Ei Ci Ti ;
SMALLINT    : Si Mi Ai Li Li Ii Ni Ti ;
SUM         : Si Ui Mi  ;
TABLE_SYM   : Ti Ai Bi Li Ei ;
TINYINT     : Ti Ii Ni Yi Ii Ni Ti ;
TRUE_SYM    : Ti Ri Ui Ei ;
TYPE_SYM    : Ti Yi Pi Ei ;
UPPER       : Ui Pi Pi Ei Ri ;
USE_SYM     : Ui Si Ei  ;
UTF8_SYM    : Ui Ti Fi '8' ;
UNSIGNED_SYM : Ui Ni Si Ii Gi Ni Ei Di ;
WHERE       : Wi Hi Ei Ri Ei  ;
XOR         : Xi Oi Ri  ;

BIGINT_SYM  : Bi Ii Gi Ii Ni Ti ;
REAL_SYM    : Ri Ei Ai Li ;
DOUBLE_SYM  : Di Oi Ui Bi Li Ei ;
FLOAT_SYM   : Fi Li Oi Ai Ti ;
DECIMAL_SYM : Di Ei Ci Ii Mi Ai Li ;
NUMERIC_SYM : Ni Ui Mi Ei Ri Ii Ci ;
DATE_SYM    : Di Ai Ti Ei ;
TIME_SYM    : Ti Ii Mi Ei ;
TIMESTAMP_SYM  : Ti Ii Mi Ei Si Ti Ai Mi Pi ;
DATETIME_SYM   : Di Ai Ti Ei Ti Ii Mi Ei ;
YEAR_SYM       : Yi Ei Ai Ri ;
CHAR_SYM       : Ci Hi Ai Ri ;
VARCHAR_SYM    : Vi Ai Ri Ci Hi Ai Ri ;
VARBINARY_SYM  : Vi Ai Ri Bi Ii Ni Ai Ri Yi ;
TINYBLOB_SYM   : Ti Ii Ni Yi Bi Li Oi Bi ;
BLOB_SYM       : Bi Li Oi Bi ;
MEDIUMBLOB_SYM : Mi Ei Di Ii Ui Mi Bi Li Oi Bi ;
LONGBLOB_SYM   : Li Oi Ni Gi Bi Li Oi Bi ;
TINYTEXT_SYM   : Ti Ii Ni Yi Ti Ei Xi Ti ;
TEXT_SYM       : Ti Ei Xi Ti ;
MEDIUMTEXT_SYM : Mi Ei Di Ii Ui Mi Ti Ei Xi Ti ;
LONGTEXT_SYM   : Li Oi Ni Gi Ti Ei Xi Ti ;
ENUM_SYM       : Ei Ni Ui Mi ;
SET_SYM        : Si Ei Ti ;
FLAT_SYM       : Fi Li Ai Ti ;
XML_SYM        : Xi Mi Li ;
CSV_SYM        : Ci Si Vi ;
JSON_SYM       : Ji Si Oi Ni ;
CONNECTION_SYM : Ci Oi Ni Ni Ei Ci Ti Ii Oi Ni ;
DIRECTORY_SYM  : Di Ii Ri Ei Ci Ti Oi Ri Yi ;
ENCLOSED_SYM   : Ei Ni Ci Li Oi Si Ei Di ;
LINES_SYM      : Li Ii Ni Ei Si ;
ESCAPED_SYM    : Ei Si Ci Ai Pi Ei Di ;
TERMINATED_SYM : Ti Ei Ri Mi Ii Ni Ai Ti Ei Di ;
OPTIONALLY_SYM : Oi Pi Ti Ii Oi Ni Ai Li Li Yi ;
EBCDIC_SYM     : Ei Bi Ci Di Ii Ci ;
FIELDS_SYM     : Fi Ii Ei Li Di Si ;
COLUMNS_SYM    : Ci Oi Li Ui Mi Ni Si ;

CHARACTER_SET : ('CHARACTER SET' | 'character set');
IFNOTEXISTS : ('IF NOT EXISTS' | 'if not exists');
ISNOTNULL   : ('IS NOT NULL' | 'is not null');
ISNULL      : ('IS NULL' | 'is null');
NOT_IN      : ('NOT IN' | 'not in');
NOT_LIKE    : ('NOT LIKE' | 'not like');

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
//ALL_FIELDS  : '.*' ;
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

fragment UNDERSCORE  : '_' ;

fragment DCOLON      : '::' ;

fragment DEFSCOPE    : DOT DCOLON ;

fragment LETTER_FRAGMENT
:
    ( 'A'..'Z' | 'a'..'z')
;

INTEGER_NUM
:
    DIGIT_FRAGMENT+
;

fragment DIGIT_FRAGMENT
:
    ( '0'..'9')
;

fragment HEX_DIGIT_FRAGMENT
:
   ( 'a'..'f' | 'A'..'F' | DIGIT_FRAGMENT )
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
  (  (Ei) ( PLUS | MINUS )? INTEGER_NUM  )?
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

quoted_table_id
:
    DQUOTE! (ABSOLUTE_FILE_ID | ID) DQUOTE!
;

//Cannot rewrite lexer rules see above parser rule
//QUOTED_ID
//:
//  DQUOTE! ID DQUOTE!
//;

fragment ABSOLUTE_FILE_ID_PREFIX
:
    NEGATION |
    DEFSCOPE |
    NEGATION DEFSCOPE
;

ABSOLUTE_FILE_ID
:
    ABSOLUTE_FILE_ID_PREFIX ID_FRAGMENT+
;

ID
:
    LETTER_FRAGMENT ( ID_FRAGMENT )*
;

fragment ID_FRAGMENT
:
    LETTER_FRAGMENT | UNDERSCORE | INTEGER_NUM | DCOLON
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

strcomp_op
:
  LIKE_SYM
  | NOT_LIKE
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
table_name          : ABSOLUTE_FILE_ID | ID ;
quoted_table_name   : quoted_table_id;
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
  | ( AS_SYM )? quoted_id -> ^( TOKEN_ALIAS quoted_id)
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
    stringCompExpression
    (
      relational_op^
      stringCompExpression
    )*
;

stringCompExpression
:
  additionExpression
  (
    strcomp_op^
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
      | ASTERISK
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
  USE_SYM INDEX_SYM LPAREN ( index_name | u = 'NONE' | l = 'none') RPAREN -> {$u != NULL}? ^(TOKEN_AVOID_INDEX index_name)
                                                                          -> {$l != NULL}? ^(TOKEN_AVOID_INDEX index_name)
                                                                          -> ^(TOKEN_INDEX_HINT index_name)
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
  | create_load_table_statement -> ^( TOKEN_CREATE_LOAD_TABLE_STATEMENT create_load_table_statement)
  //| create_index_statement -> ^( TOKEN_CREATE_INDEX_STATEMENT create_index_statement)
;

create_index_statement
:
    CREATE_SYM INDEX_SYM index_name ON table_spec column_list
;

create_load_table_statement
:
    create_table_statement
    (SEMI?)!
    load_table_statement
    -> create_table_statement load_table_statement
;

table_options
:
    table_option (( COMMA )? table_option)*
;

table_option:
     (  COMMENT_SYM^ (EQ_SYM?)! string_literal  )
;

create_table_statement
:
    CREATE_SYM TABLE_SYM noov=IFNOTEXISTS? table_name create_table_columns_definition
    (table_options)?
    -> {$noov != NULL}? ^( TOKEN_CREATE_TABLE table_name TOKEN_DONOT_OVERWRITE table_options? create_table_columns_definition)
    -> ^( TOKEN_CREATE_TABLE table_name TOKEN_OVERWRITE table_options? create_table_columns_definition)
;

create_table_columns_definition
:
    LPAREN create_definition (COMMA create_definition)* RPAREN -> {$COMMA != NULL}? ^(TOKEN_COLUMN_DEF_LIST create_definition+)
                                                               -> ^(TOKEN_COLUMN_DEF_LIST create_definition)
;

create_definition
:
       column_name column_definition -> ^(TOKEN_COLUMN_DEF column_name column_definition)
;

length_and_or_precision_definition
:
    LPAREN length ( COMMA number_literal)? RPAREN -> ^(length number_literal?)
;

length_and_precision_definition
:
    LPAREN length COMMA number_literal RPAREN -> ^(length number_literal)
;

length_definition
:
    LPAREN length RPAREN -> ^(length)
;

text_params
:
    LPAREN string_literal (COMMA string_literal)* RPAREN -> ^( TOKEN_PROC_PARAMS string_literal+ )
;
column_definition
:
   BIT_SYM       length_definition?                                -> ^(BIT_SYM)
   | TINYINT     length_definition? UNSIGNED_SYM?                  -> ^(TINYINT UNSIGNED_SYM? length_definition?)
   | SMALLINT    length_definition? UNSIGNED_SYM?                  -> ^(SMALLINT UNSIGNED_SYM? length_definition?)
   | MEDIUMINT   length_definition? UNSIGNED_SYM?                  -> ^(MEDIUMINT UNSIGNED_SYM? length_definition?)
   | INT_SYM     length_definition? UNSIGNED_SYM?                  -> ^(INTEGER_SYM UNSIGNED_SYM? length_definition?)
   | INTEGER_SYM length_definition? UNSIGNED_SYM?                  -> ^(INTEGER_SYM UNSIGNED_SYM? length_definition?)
   | BIGINT_SYM  length_definition? UNSIGNED_SYM?                  -> ^(BIGINT_SYM UNSIGNED_SYM? length_definition?)
   | REAL_SYM    length_and_precision_definition?    UNSIGNED_SYM? -> ^(REAL_SYM UNSIGNED_SYM? length_and_precision_definition?)
   | DOUBLE_SYM  length_and_precision_definition?    UNSIGNED_SYM? -> ^(DOUBLE_SYM UNSIGNED_SYM? length_and_precision_definition?)
   | FLOAT_SYM   length_and_precision_definition?    UNSIGNED_SYM? -> ^(FLOAT_SYM UNSIGNED_SYM? length_and_precision_definition?)
   | DECIMAL_SYM length_and_or_precision_definition? UNSIGNED_SYM? -> ^(DECIMAL_SYM UNSIGNED_SYM? length_and_or_precision_definition?)
   | NUMERIC_SYM length_and_or_precision_definition? UNSIGNED_SYM? -> ^(NUMERIC_SYM UNSIGNED_SYM? length_and_or_precision_definition?)
   | DATE_SYM
   | TIME_SYM
   | TIMESTAMP_SYM
   | DATETIME_SYM
   | YEAR_SYM
   | CHAR_SYM      length_definition? charset_declaration?  -> ^(CHAR_SYM length_definition? charset_declaration?)
   | VARCHAR_SYM   length_definition  charset_declaration?  -> ^(VARCHAR_SYM length_definition charset_declaration?)
   | BINARY_SYM    length_definition?                       -> ^(BINARY_SYM length_definition?)
   | VARBINARY_SYM length_definition                        -> ^(VARBINARY_SYM length_definition)
   | TINYBLOB_SYM
   | BLOB_SYM
   | MEDIUMBLOB_SYM
   | LONGBLOB_SYM
   | TINYTEXT_SYM   BINARY_SYM?                      -> ^(TINYTEXT_SYM BINARY_SYM?)
   | TEXT_SYM       BINARY_SYM? charset_declaration? -> ^(TEXT_SYM BINARY_SYM? charset_declaration?)
   | MEDIUMTEXT_SYM BINARY_SYM? charset_declaration? -> ^(MEDIUMTEXT_SYM BINARY_SYM? charset_declaration?)
   | LONGTEXT_SYM   BINARY_SYM? charset_declaration? -> ^(LONGTEXT_SYM BINARY_SYM? charset_declaration?)
   | ENUM_SYM       text_params charset_declaration? -> ^(ENUM_SYM text_params charset_declaration?)
   | SET_SYM        text_params charset_declaration? -> ^(SET_SYM text_params charset_declaration?)
;

charset_declaration
:
    CHARACTER_SET^ charset_name
;
charset_name
:
    ASCII_SYM
    | UTF8_SYM
;

file_data_format_type_options
:
    LPAREN! (ID EQ_SYM! string_literal) (COMMA! (ID EQ_SYM! string_literal))* RPAREN!
;

file_data_format_type
:
    FLAT_SYM
    | XML_SYM
    | CSV_SYM
    | JSON_SYM
;

file_data_format_declaration
:
    TYPE_SYM file_data_format_type file_data_format_type_options? -> ^(TYPE_SYM file_data_format_type file_data_format_type_options?)
;

variable_data_line_terminator
:
    TERMINATED_SYM BY_SYM t=TEXT_STRING -> ^(TOKEN_VAR_TERMINATOR $t)
;

variable_data_field_terminator
:
    TERMINATED_SYM BY_SYM t=TEXT_STRING -> ^(TOKEN_VAR_SEPERATOR $t)
;

variable_data_escaped
:
    ESCAPED_SYM BY_SYM t=TEXT_STRING -> ^(TOKEN_VAR_ESCAPED $t)
;

variable_data_enclosed
:
    (OPTIONALLY_SYM)? ENCLOSED_SYM BY_SYM t=TEXT_STRING -> ^(TOKEN_VAR_ENCLOSED $t)
;

variable_data_format_declaration_field
:
    (FIELDS_SYM | COLUMNS_SYM)
    variable_data_field_terminator?
    variable_data_enclosed?
    variable_data_escaped?

    -> ^(TOKEN_VARIABLE_FILE variable_data_field_terminator? variable_data_enclosed? variable_data_escaped?)
;

variable_data_format_declaration_line
:
    LINES_SYM
    //(STARTING BY_SYM TEXT_STRING)?
    variable_data_line_terminator?
    -> ^(TOKEN_VARIABLE_FILE variable_data_line_terminator?)
;

landing_zone_information
:
    CONNECTION_SYM EQ_SYM? conn=string_literal
    DIRECTORY_SYM EQ_SYM? dir=string_literal
    -> ^(TOKEN_LANDING_ZONE $conn $dir)
;

load_table_statement
:
    LOAD_SYM DATA_SYM
    landing_zone_information?
    INFILE_SYM ff=string_literal landing_zone_information? file_data_format_declaration?
    INTO_SYM TABLE_SYM table_spec
    variable_data_format_declaration_field?
    variable_data_format_declaration_line?
    -> ^( TOKEN_LOAD_TABLE table_spec $ff landing_zone_information? file_data_format_declaration? variable_data_format_declaration_field? variable_data_format_declaration_line?)
;

select_statement
:
        select_portion
        ( from_portion
          ( where_clause )?
          ( groupby_clause )?
          ( having_clause )?
        )?

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

length
:
  INTEGER_NUM
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
  schema_spec? table_fork ->  ^(table_fork  schema_spec?)
;

table_fork
:
    (table_name | quoted_table_name)^
;

schema_spec
:
    (ID|quoted_id) DOT -> ^(TOKEN_TABLE_SCHEMA ID? quoted_id?)
;

column_wildcard
:
    (ID DOT)? ASTERISK -> ^(TOKEN_COLUMNWILDCARD ID?)
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

