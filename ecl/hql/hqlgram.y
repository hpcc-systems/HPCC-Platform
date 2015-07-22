//
//    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//############################################################################## */

//Either api.pure of c++ skeleton could be used, not both (which causes an error).
//Note the c++ generation still generates a separate class for the raw processing from the HqlGram class, so whichever is
//used the productions need to use parser->... to access the context
%define api.pure
//%error-verbose
%lex-param {HqlGram* parser}
%lex-param {int * yyssp}
%parse-param {HqlGram* parser}
%name-prefix "eclyy"
//
%destructor {$$.release();} <>
//Could override destructors for all tokens e.g.,
//%destructor {} ABS
//but warnings still come out, and improvement in code is marginal.
//Only defining destructors for those productions that need them would solve it, but be open to missing items.
//Adding a comment to reference unused parameters also fails to solve it because it ignores references in comments (and a bit ugly)
//fixing bison to ignore destructor {} for need use is another alternative - but would take a long time to feed into a public build.
%{
#include "platform.h"
#include <stdio.h>
#include <stdlib.h>
#include "hql.hpp"
#include "jlib.hpp"
#include "jmisc.hpp"
#include "jexcept.hpp"
#include "hqlerrors.hpp"

#include "hqlgram.hpp"
#include "hqlfold.hpp"
#include "hqlpmap.hpp"
#include "hqlutil.hpp"
#include "hqlattr.hpp"
#include "hqlmeta.hpp"

#define REDEF_MSG(name)     StringBuffer msg;           \
                            msg.append(w"Identifier '"); \
                            msg.append(name.queryExpr()->queryName()->str()); \
                            msg.append("' before := ");                       \
                            msg.append(" is already defined");                 \
                            parser->reportError(ERR_ID_REDEFINE, name, msg.str())

#define REDEF_ERROR(name)                    \
                        {                    \
                            REDEF_MSG(name); \
                        }

#define REDEF_ERROR1(name,e1)                \
                        {                    \
                            REDEF_MSG(name); \
                            e1.release();    \
                        }

#define REDEF_ERROR2(name,e1,e2)             \
                        {                    \
                            REDEF_MSG(name); \
                            e1.release();    \
                            e2.release();    \
                        }

#define REDEF_ERROR3(name,e1,e2,e3)          \
                        {                    \
                            REDEF_MSG(name); \
                            e1.release();    \
                            e2.release();    \
                            e3.release();    \
                        }

inline int eclyylex(attribute * yylval, HqlGram* parser, const short int * yyssp)
{
    return parser->yyLex(yylval, yyssp);
}



static void eclsyntaxerror(HqlGram * parser, const char * s, short yystate, int token);
#define eclyyerror(parser, s)       eclsyntaxerror(parser, s, yystate, yychar)
#define ignoreBisonWarning(x)
#define ignoreBisonWarnings2(x,y)
#define ignoreBisonWarnings3(x,y,z)

%}

//=========================================== tokens ====================================

%token
/* remember to add any new tokens to the error reporter and lexer too! */
/* If they clash with other #defines etc then use TOK_ as a prefix */

// NB: Very occassionally the same keyword in the source (e.g., MERGE, PARTITION may return a different token
// (MERGE_ATTR, PARTITION_ATTR) depending on the context it is used in.  This is because there would be a s/r
// error, so the _ATTR form is only allowed in the situations where the attibute is valid - enabled by a
// call to enableAttributes() from a production in the grammar.

  ABS
  ACOS
  AFTER
  AGGREGATE
  ALIAS
  ALL
  ALLNODES
  AND
  ANY
  APPLY
  _ARRAY_
  AS
  ASCII
  ASIN
  TOK_ASSERT
  ASSTRING
  ATAN
  ATAN2
  ATMOST
  AVE
  BACKUP
  BEFORE
  BEST
  BETWEEN
  TOK_BITMAP
  BIG
  BLOB
  BNOT
  BUILD
  CARDINALITY
  CASE
  TOK_CATCH
  CHECKPOINT
  CHOOSE
  CHOOSEN
  CHOOSENALL
  CHOOSESETS
  CLUSTER
  CLUSTERSIZE
  COGROUP
  __COMMON__
  __COMPOUND__
  COMBINE
  COMPRESSED
  __COMPRESSED__
  TOK_CONST
  CORRELATION
  COS
  COSH
  COUNT
  COUNTER
  COVARIANCE
  CPPBODY
  TOK_CPP
  CRC
  CRON
  CSV
  DATASET
  __DEBUG__
  DEDUP
  DEFAULT
  DEFINE
  DENORMALIZE
  DEPRECATED
  DESC
  DICTIONARY
  DISTRIBUTE
  DISTRIBUTED
  DISTRIBUTION
  DYNAMIC
  EBCDIC
  ECLCRC
  ELSE
  ELSEIF
  EMBED
  EMBEDDED
  _EMPTY_
  ENCODING
  ENCRYPT
  ENCRYPTED
  END
  ENDCPP
  ENDEMBED
  ENTH
  ENUM
  TOK_ERROR
  ESCAPE
  EVALUATE
  EVENT
  EVENTEXTRA
  EVENTNAME
  EXCEPT
  EXCLUSIVE
  EXISTS
  EXP
  EXPIRE
  EXPORT
  EXTEND
  FAIL
  FAILCODE
  FAILMESSAGE
  FAILURE
  TOK_FALSE
  FEATURE
  FETCH
  FEW
  FILEPOSITION
  FILTERED
  FIRST
  TOK_FIXED
  FLAT
  FORMAT_ATTR
  FORWARD
  FROM
  FROMJSON
  FROMUNICODE
  FROMXML
  FULL
  FUNCTION
  GETENV
  GLOBAL
  GRAPH
  GROUP
  GROUPBY
  GROUPED
  __GROUPED__
  GUARD
  HASH
  HASH32
  HASH64
  HASHMD5
  HAVING
  HEADING
  HINT
  HOLE
  HTTPCALL
  HTTPHEADER
  IF
  IFF
  IFBLOCK
  TOK_IGNORE
  IMPLEMENTS
  IMPORT
  INDEPENDENT
  INLINE
  TOK_IN
  INNER
  INTERFACE
  INTERNAL
  INTFORMAT
  ISNULL
  ISVALID
  ITERATE
  JOIN
  JOINED
  JSON_TOKEN
  KEEP
  KEYDIFF
  KEYED
  KEYPATCH
  KEYUNICODE
  LABELED
  LAST
  LEFT
  LENGTH
  LIBRARY
  LIMIT
  LINKCOUNTED
  LITERAL
  LITTLE
  LN
  LOADXML
  LOCAL
  LOCALE
  LOCALFILEPOSITION
  TOK_LOG
  LOGICALFILENAME
  LOOKUP
  LOOP
  LZW
  MANY
  MAP
  MATCHED
  MATCHLENGTH
  MATCHPOSITION
  MATCHROW
  MATCHTEXT
  MATCHUNICODE
  MATCHUTF8
  MAX
  MAXCOUNT
  MAXLENGTH
  MAXSIZE
  MERGE
  MERGE_ATTR
  MERGEJOIN
  MIN
  MODULE
  MOFN
  MULTIPLE
  NAMED
  NAMEOF
  NAMESPACE
  NOBOUNDCHECK
  NOCASE
  NOFOLD
  NOHOIST
  NOLOCAL
  NONEMPTY
  NOOVERWRITE
  NORMALIZE
  NOROOT
  NOSCAN
  NOSORT
  __NOSTREAMING__
  NOT
  NOTHOR
  NOTIFY
  NOTRIM
  NOXPATH
  OF
  OMITTED
  ONCE
  ONFAIL
  ONLY
  ONWARNING
  OPT
  OR
  ORDERED
  OUTER
  OUTPUT
  TOK_OUT
  OVERWRITE
  __OWNED__
  PACKED
  PARALLEL
  PARSE
  PARTITION
  PARTITION_ATTR
  TOK_PATTERN
  PENALTY
  PERSIST
  PHYSICALFILENAME
  PIPE
  __PLATFORM__
  POWER
  PREFETCH
  PRELOAD
  PRIORITY
  PRIVATE
  PROCESS
  PROJECT
  PROXYADDRESS
  PULL
  PULLED
  QUOTE
  RANDOM
  RANGE
  RANK
  RANKED
  REALFORMAT
  RECORD
  RECORDOF
  RECOVERY
  REGEXFIND
  REGEXREPLACE
  REGROUP
  REJECTED
  RELATIONSHIP
  REMOTE
  REPEAT
  RESPONSE
  RETRY
  RETURN
  RIGHT
  RIGHT_NN
  ROLLUP
  ROUND
  ROUNDUP
  ROW
  ROWS
  ROWSET
  ROWDIFF
  RULE
  SAMPLE
  SCAN
  SECTION
  SELF
  SEPARATOR
  __SEQUENCE__
  SEQUENTIAL
  SERVICE
  SET
  SHARED
  SIMPLE_TYPE
  SIN
  SINGLE
  SINH
  SIZEOF
  SKEW
  SKIP
  SMART
  SOAPACTION
  SOAPCALL
  SORT
  SORTED
  SQL
  SQRT
  STABLE
  __STAND_ALONE__
  STEPPED
  STORED
  STREAMED
  SUBSORT
  SUCCESS
  SUM
  SWAPPED
  TABLE
  TAN
  TANH
  TERMINATOR
  THEN
  THISNODE
  THOR
  THRESHOLD
  TIMEOUT
  TIMELIMIT
  TOKEN
  TOJSON
  TOPN
  TOUNICODE
  TOXML
  TRANSFER
  TRANSFORM
  TRIM
  TRUNCATE
  TOK_TRUE
  TYPE
  TYPEOF
  UNICODEORDER
  UNGROUP
  UNORDERED
  UNSIGNED
  UNSORTED
  UNSTABLE
  UPDATE
  USE
  VALIDATE
  VARIANCE
  VIRTUAL
  WAIT
  TOK_WARNING
  WHEN
  WHICH
  WIDTH
  WILD
  WITHIN
  WHOLE
  WORKUNIT
  XML_TOKEN
  XMLDECODE
  XMLDEFAULT
  XMLENCODE
  XMLNS
  XMLPROJECT
  XMLTEXT
  XMLUNICODE
  XPATH

  
//Operators
  FIELD_REF
  FIELDS_REF
  ANDAND
  EQ
  NE
  LE
  LT
  GE
  GT
  ORDER
  ASSIGN
  GOESTO
  DOTDOT
  DIV
  SHIFTL
  SHIFTR

  DATAROW_ID
  DATASET_ID
  DICTIONARY_ID
  SCOPE_ID
  VALUE_ID
  VALUE_ID_REF
  ACTION_ID
  UNKNOWN_ID
  RECORD_ID
  ALIEN_ID
  TRANSFORM_ID
  PATTERN_ID
  FEATURE_ID
  EVENT_ID
  ENUM_ID
  LIST_DATASET_ID
  SORTLIST_ID

  TYPE_ID
  SET_TYPE_ID
  PATTERN_TYPE_ID
  DATASET_TYPE_ID
  DICTIONARY_TYPE_ID

  DATAROW_FUNCTION
  DATASET_FUNCTION
  DICTIONARY_FUNCTION
  VALUE_FUNCTION
  ACTION_FUNCTION
  PATTERN_FUNCTION
  RECORD_FUNCTION
  EVENT_FUNCTION
  SCOPE_FUNCTION
  TRANSFORM_FUNCTION
  LIST_DATASET_FUNCTION

  VALUE_MACRO
  DEFINITIONS_MACRO

  BOOL_CONST
  INTEGER_CONST
  STRING_CONST
  DATA_CONST
  REAL_CONST
  UNICODE_CONST
  TYPE_LPAREN
  TYPE_RPAREN

  MACRO
  COMPLEX_MACRO
  ENDMACRO
  SKIPPED
  HASHEND
  HASHELIF
  HASHBREAK

  INDEX
  HASH_CONSTANT
  HASH_OPTION
  HASH_WORKUNIT
  HASH_STORED
  HASH_LINK
  HASH_ONWARNING
  HASH_WEBSERVICE

  INTERNAL_READ_NEXT_TOKEN

//  __INTERNAL__HASHDEFINED_FOUND
//  __INTERNAL__HASHDEFINED_NOTFOUND

  /* add new token before this! */
  YY_LAST_TOKEN

%left LOWEST_PRECEDENCE
%left VALUE_MACRO
%left OR
%left AND

%left reduceAttrib

%left ORDER UNICODEORDER
%left SHIFTL SHIFTR
%left '+' '-'
%left '*' '/' '%' DIV
%left '|' '^'
%left '&' ANDAND

%left NOT

%left '.'
%left '('
%left '['
%left HIGHEST_PRECEDENCE

%%

//================================== begin of syntax section ==========================

hqlQuery
    : ENCRYPTED hqlQueryBody    { ignoreBisonWarnings3($$,$1,$2); }
    | hqlQueryBody
    ;

hqlQueryBody
    : definitions
    | query             
                        {   parser->addResult($1.getExpr(), $1); $$.clear(); }
    | definitions query 
                        {   
                            ignoreBisonWarning($1);
                            parser->addResult($2.getExpr(), $2); $$.clear();
                        }
    | RETURN goodObject ';'
                        {   parser->addResult($2.getExpr(), $2); $$.clear(); }
    | definitions setActiveToExpected RETURN goodObject ';'
                        {   parser->addResult($4.getExpr(), $4); $$.clear(); }
    | compoundModule ';'
                        {   parser->addResult($1.getExpr(), $1); $$.clear(); }
 //Temporary productions...
    | recordDef ';'                             
                        {   
                            ignoreBisonWarning($2);
                            parser->addResult($1.getExpr(), $1); $$.clear();
                        }
    | definitions recordDef ';'                 
                        {   
                            ignoreBisonWarnings2($1, $3);
                            parser->addResult($2.getExpr(), $2); $$.clear();
                        }
    |
 //Special production used processing template queries
    | GOESTO goodObject ';'                     
                        {   parser->addResult($2.getExpr(), $2); $$.clear(); }
    ;

setActiveToExpected
    :                   {   parser->setCurrentToExpected(); $$.clear(); }
    ;
    

importSection
    : startIMPORT importItem endIMPORT   
                        {   parser->lastpos = $3.pos.position+1; $$.clear(); }
    | startIMPORT error endIMPORT       
                        {   parser->lastpos = $3.pos.position+1; $$.clear(); }
    ;

startIMPORT
    : IMPORT            {   parser->setIdUnknown(true); $$.clear(); }
    ;

endIMPORT
    : ';'               {   parser->setIdUnknown(false); $$.clear(); }
    ;

importItem
    : importSelectorList
                        {
                            parser->processImport($1, NULL);
                            $$.clear();
                        }
    | importSelectorList FROM importSelector
                        {
                            parser->processImport($1, $3, NULL);
                            $$.clear();
                        }
    | importSelectorList AS UNKNOWN_ID
                        {
                            parser->processImport($1, $3.getId());
                            $$.clear();
                        }
    | importSelectorList FROM importSelector AS UNKNOWN_ID
                        {
                            parser->processImport($1, $3, $5.getId());
                            $$.clear();
                        }
    | '*' FROM importSelector
                        {
                            parser->processImportAll($3);
                            $$.clear();
                        }
    | importSelectorList AS '*'
                        {
                            if (queryLegacyImportSemantics())
                                parser->reportWarning(CategoryDeprecated, ERR_DEPRECATED, $1.pos, "IMPORT <module> AS * is deprecated, use IMPORT * FROM <module>");
                            else
                                parser->reportError(ERR_DEPRECATED, $1.pos, "IMPORT <module> AS * is deprecated, use IMPORT * FROM <module>");
                            parser->processImportAll($1);
                            $$.clear();
                        }
    ;

importSelectorList
    : beginList importItems
                        {
                            HqlExprArray importItems;
                            parser->endList(importItems);
                            $$.setExpr(createComma(importItems), $2);
                        }
    ;
    
importItems
    : importSelector
                        {
                            parser->addListElement($1.getExpr());
                            $$.clear();
                        }
    | importItems ',' importSelector
                        {
                            parser->addListElement($3.getExpr());
                            $$.clear();
                        }
    ;
    
importSelector
    : importId;
    

importId
    : UNKNOWN_ID        {
                            $$.setExpr(createId($1.getId()), $1);
                        }
    | '$'               {
                            $$.setExpr(createAttribute(selfAtom), $1);
                        }
    | '^'               {
                            $$.setExpr(createAttribute(_root_Atom), $1);
                        }
    | importId '.' UNKNOWN_ID
                        {
                            $$.setExpr(createAttribute(_dot_Atom, $1.getExpr(), createId($3.getId())), $1);
                        }
    | importId '.' '^'
                        {
                            $$.setExpr(createAttribute(_container_Atom, $1.getExpr()), $1);
                        }
    ;

defineType
    : typeDef
    | setType
    | explicitDatasetType
    | explicitDictionaryType
    | ROW               {
                            IHqlExpression* record = queryNullRecord();
                            $$.setType(makeRowType(record->getType()));
                            $$.setPosition($1);
                        }
    | transformType
    ;

explicitDatasetType
    : explicitDatasetType1
    | GROUPED explicitDatasetType1
                        {
                            $$.setType(makeGroupedTableType($2.getType()));
                            $$.setPosition($1);
                        }
    ;
    
explicitDatasetType1
    : DATASET
                        {
                            $$.setType(makeTableType(makeRowType(queryNullRecord()->getType())));
                            $$.setPosition($1);
                        }
    | DATASET '(' recordDef childDatasetOptions ')'
                        {
                            OwnedHqlExpr record = $3.getExpr();
                            OwnedHqlExpr options = $4.getExpr();
                            ITypeInfo * recordType = createRecordType(record);
                            Owned<ITypeInfo> tableType = makeTableType(makeRowType(recordType));
                            if (options)
                                tableType.setown(makeAttributeModifier(LINK(tableType), createExprAttribute(_childAttr_Atom, LINK(options))));
                            $$.setType(tableType.getClear());
                            $$.setPosition($1);
                        }
    | _ARRAY_ explicitDatasetType
                        {
                            $$.setType(makeOutOfLineModifier($2.getType()));
                            $$.setPosition($1);
                        }
    | LINKCOUNTED explicitDatasetType
                        {
                            Owned<ITypeInfo> dsType = $2.getType();
                            $$.setType(setLinkCountedAttr(dsType, true));
                            $$.setPosition($1);
                        }
    | STREAMED explicitDatasetType
                        {
                            Owned<ITypeInfo> dsType = $2.getType();
                            Owned<ITypeInfo> linkedType = setLinkCountedAttr(dsType, true);
                            $$.setType(setStreamedAttr(linkedType, true));
                            $$.setPosition($1);
                        }
    | EMBEDDED explicitDatasetType
                        {
                            $$.setType(makeAttributeModifier($2.getType(), getEmbeddedAttr()));
                            $$.setPosition($1);
                        }
    | userTypedefDataset
    ;

explicitDictionaryType
    : DICTIONARY
                        {
                            $$.setType(makeDictionaryType(makeRowType(queryNullRecord()->getType())));
                            $$.setPosition($1);
                        }
    | DICTIONARY '(' recordDef ')'
                        {
                            OwnedHqlExpr record = $3.getExpr();
                            ITypeInfo * recordType = createRecordType(record);
                            $$.setType(makeDictionaryType(makeRowType(recordType)));
                            $$.setPosition($1);
                        }
    | LINKCOUNTED explicitDictionaryType
                        {
                            Owned<ITypeInfo> dsType = $2.getType();
                            $$.setType(setLinkCountedAttr(dsType, true));
                            $$.setPosition($1);
                        }
    | userTypedefDictionary
    ;

explicitRowType
    : explicitRowType1
    | LINKCOUNTED explicitRowType1
                        {
                            Owned<ITypeInfo> rowType = $2.getType();
                            $$.setType(setLinkCountedAttr(rowType, true));
                            $$.setPosition($1);
                        }
    ;

explicitRowType1
    : ROW               {
                            IHqlExpression* record = queryNullRecord();
                            $$.setType(makeRowType(record->getType()));
                            $$.setPosition($1);
                        }
    | ROW '(' recordDef ')'
                        {
                            OwnedHqlExpr record = $3.getExpr();
                            $$.setType(makeRowType(record->getType()));
                            $$.setPosition($1);
                        }
    ;


transformType
    : TRANSFORM '(' recordDef ')'
                        {
                            OwnedHqlExpr record = $3.getExpr();
                            $$.setType(makeTransformType(LINK(record->queryRecordType())), $1);
                        }
    | TRANSFORM '(' dataSet ')'
                        {
                            OwnedHqlExpr ds = $3.getExpr();
                            $$.setType(makeTransformType(LINK(ds->queryRecordType())), $1);
                        }
    ;
    
propType
    : simpleType           /* only simple type is supported for service */
    | scopeFlag simpleType
                        {
                            parser->reportError(ERR_SVC_NOSCOPEMODIFIER,$1,"Function in service can not specify EXPORT or SHARED");
                            $$.setType($2.getType());
                        }
    | scopeFlag     {
                            parser->reportError(ERR_SVC_NOSCOPEMODIFIER,$1,"Function in service can not specify EXPORT or SHARED");
                            $$.setType(makeVoidType());
                        }
    ;

paramType
    : typeDef
                        {
                            OwnedITypeInfo type = $1.getType();
                            //Syntactic oddity.  A record means a row of that record type.
                            if (type->getTypeCode() == type_record)
                                type.setown(makeRowType(type.getClear()));
                            $$.setType(type.getClear());
                            $$.setPosition($1);
                        }
    | DATASET_ID        {
                            OwnedHqlExpr dataset = $1.getExpr();
//                          $$.setType(makeOriginalModifier(createRecordType(dataset), LINK(dataset)));
                            $$.setType(makeRowType(createRecordType(dataset)));
                            $$.setPosition($1);
                        }
    | moduleScopeDot DATASET_ID leaveScope
                        {
                            //slightly nasty to add two original modifiers to the record, but the first one allows
                            //us to get the record right, and the second the types of parameters on transforms.
                            $1.release();
                            OwnedHqlExpr dataset = $2.getExpr();
                            //$$.setType(makeOriginalModifier(createRecordType(dataset), LINK(dataset)));
                            $$.setType(makeRowType(createRecordType(dataset)));
                            $$.setPosition($2);
                        }
    | abstractDataset   {
                            OwnedHqlExpr record = $1.getExpr();
                            OwnedHqlExpr abstractRecord = createAbstractRecord(record);
                            OwnedITypeInfo type = makeTableType(makeRowType(abstractRecord->getType()));
                            $$.setType(type.getClear(), $1);
                            parser->setTemplateAttribute();
                        }
    | explicitDatasetType
    | explicitDictionaryType
    | explicitRowType
    | abstractModule
                        {
                            OwnedHqlExpr scope = $1.getExpr();
                            $$.setType(scope->getType());       // more what about if moduleScope is a parameter?
                            $$.setPosition($1);
                        }
    | patternParamType
    | VIRTUAL RECORD
                        {
                            IHqlExpression* record = queryNullRecord();
                            OwnedHqlExpr abstractRecord = createAbstractRecord(record);
                            $$.setType(abstractRecord->getType(), $1);
                            parser->setTemplateAttribute();
                        }
    ;

patternParamType
    : TOK_PATTERN       {
                            $$.setType(makePatternType());
                            $$.setPosition($1);
                        }
    | RULE              {
                            $$.setType(makeRuleType(NULL));
                            $$.setPosition($1);
                        }
    | TOKEN             {
                            $$.setType(makeTokenType());
                            $$.setPosition($1);
                        }
    ;

object
    : goodObject
    | badObject
    ;

goodObject
    : dataSet
    | dictionary
    | expression
                        {
                            //Remove later to allow sortlist attributes
                            parser->normalizeExpression($1);
                            $$.inherit($1);
                        }
    | dataRow
    | recordDef
    | service
    | action
    | transformDef
    | transform
    | complexType
    | macro
    | embedBody
    | eventObject
    | compoundAttribute
    | abstractModule
    | goodTypeObject optFieldAttrs
                        {
                            Owned<ITypeInfo> type = $1.getType();
                            HqlExprArray attrs;
                            $2.unwindCommaList(attrs);
                            $$.setExpr(createValue(no_typedef, type.getClear(), attrs), $1);
                        }
    | enumDef
    | enumTypeId
    | setOfDatasets
    | anyFunction
    | fieldSelectedFromRecord
                        {
                            OwnedHqlExpr value = $1.getExpr();
                            $$.setExpr(createValue(no_indirect, value->getType(), LINK(value)), $1);
                        }
    | __SEQUENCE__
                        {
                            //NB: Undocumented experimental feature
                            //This is only allowed as an object, rather than a sequence so we can correctly check whether the containing
                            //attribute (if any) is parametered, and add the implicit parameter there
                            $$.setExpr(parser->createScopedSequenceExpr(), $1);
                        }
    | DEFINE goodObject
                        {
                            //Ugly internal unsupported syntax for defining an out of line function
                            //Needs a lot more work.
                            OwnedHqlExpr value = $2.getExpr();
                            $$.setExpr(parser->convertToOutOfLineFunction($2.pos, value), $1);
                        }
    ;

goodTypeObject
    : setType
    | simpleType
    | alienTypeInstance
    | userTypedefType
    | RULE TYPE '(' recordDef ')' 
                        {
                            OwnedHqlExpr record = $4.getExpr();
                            $$.setType(makeRuleType(record->getType()));
                            $$.setPosition($1);
                        }
    | explicitDatasetType
    | explicitDictionaryType
    ;

badObject
    : error             {
                            parser->processError(false);
                            $$.setExpr(createConstant((__int64) 0), $1);
                        }
    ;

macro
    : MACRO             {
                            Owned<IFileContents> contents = $1.getContents();
                            IHqlExpression* expr = createUnknown(no_macro, makeBoolType(), macroAtom, LINK(contents));
#if defined(TRACE_MACRO)
                            PrintLog("MACRO>> verify: macro definition at %d:%d\n",yylval.startLine, yylval.startColumn);
#endif

                            //Use a named symbol to associate a line number/column
                            expr = createSymbol(macroId, NULL, expr, NULL,
                                                false, false, (object_type)0,
                                                NULL,
                                                yylval.pos.lineno, yylval.pos.column, 0, 0, 0);
                            $$.setExpr(expr, $1);
                        }
    | COMPLEX_MACRO     {
                            Owned<IFileContents> contents = $1.getContents();

                            IHqlExpression* expr = createUnknown(no_macro, makeVoidType(), macroAtom, LINK(contents));

#if defined(TRACE_MACRO)
                            PrintLog("MACRO>> verify: macro definition at %d:%d\n",yylval.startLine, yylval.startColumn);
#endif

                            //Use a named symbol to associate a line number/column
                            expr = createSymbol(macroId, NULL, expr, NULL,
                                                false, false, (object_type)0,
                                                NULL,
                                                yylval.pos.lineno, yylval.pos.column, 0, 0, 0);

                            $$.setExpr(expr, $1);
                        }
    ;

embedBody
    : CPPBODY           {
                            OwnedHqlExpr embeddedCppText = $1.getExpr();
                            $$.setExpr(parser->processEmbedBody($1, embeddedCppText, NULL, NULL), $1);
                        }
    | embedPrefix CPPBODY
                        {
                            OwnedHqlExpr language = $1.getExpr();
                            OwnedHqlExpr embedText = $2.getExpr();
                            if (language->getOperator()==no_comma)
                                $$.setExpr(parser->processEmbedBody($2, embedText, language->queryChild(0), language->queryChild(1)), $1);
                            else
                                $$.setExpr(parser->processEmbedBody($2, embedText, language, NULL), $1);
                        }
    | embedCppPrefix CPPBODY
                        {
                            OwnedHqlExpr attrs = $1.getExpr();
                            OwnedHqlExpr embedText = $2.getExpr();
                            $$.setExpr(parser->processEmbedBody($2, embedText, NULL, attrs), $1);
                        }
    | EMBED '(' abstractModule ',' expression ')'
                        {
                            parser->normalizeExpression($5, type_stringorunicode, true);
                            OwnedHqlExpr language = $3.getExpr();
                            OwnedHqlExpr embedText = $5.getExpr();
                            $$.setExpr(parser->processEmbedBody($5, embedText, language, NULL), $1);
                        }
    | IMPORT '(' abstractModule ',' expression attribs ')'
                        {
                            parser->normalizeExpression($5, type_stringorunicode, true);
                            OwnedHqlExpr language = $3.getExpr();
                            OwnedHqlExpr funcname = $5.getExpr();
                            OwnedHqlExpr attribs = createComma(createAttribute(importAtom), $6.getExpr());
                            $$.setExpr(parser->processEmbedBody($6, funcname, language, attribs), $1);
                        }
    
    ;

embedPrefix
    : EMBED '(' abstractModule attribs ')'
                        {
                            parser->getLexer()->enterEmbeddedMode();
                            $$.setExpr(createComma($3.getExpr(), $4.getExpr()), $1);
                        }
    ;

embedCppPrefix
    : EMBED '(' TOK_CPP attribs ')'
                        {
                            parser->getLexer()->enterEmbeddedMode();
                            $$.setExpr($4.getExpr(), $1);
                        }
    ;

compoundAttribute
    : startCompoundAttribute optDefinitions returnAction ';' END
                        {
                            $$.setExpr(parser->processCompoundFunction($3, false), $3);
                        }
    ;

startCompoundAttribute
    : FUNCTION
                        {
                            parser->enterScope(false);
                            parser->enterCompoundObject();
                            $$.clear();
                        }
    ;

returnAction
    : RETURN goodObject
                        {
                            $$.setExpr($2.getExpr(), $2);
                        }
    ;

compoundModule
    : startCompoundModule moduleBase moduleDefinitions END
                        {
                            $$.setExpr(parser->processModuleDefinition($1), $1);
                        }
    | PROJECT '(' abstractModule ',' abstractModule scopeProjectOpts ')'
                        {
                            OwnedHqlExpr flags = $6.getExpr();
                            IHqlExpression *expr = parser->implementInterfaceFromModule($3, $5, flags);
                            $$.setExpr(expr, $1);
                        }
    ;

startCompoundModule
    : MODULE
                        {
                            parser->enterVirtualScope();
                            parser->enterCompoundObject();
                            $$.setPosition($1);
                        }
    | INTERFACE
                        {
                            parser->enterVirtualScope();
                            parser->enterCompoundObject();
                            OwnedHqlExpr attr = createAttribute(interfaceAtom);
                            parser->appendToActiveScope(attr);
                            $$.setPosition($1);
                        }
    ;

moduleDefinitions
    :
    | moduleDefinitions moduleDefinition
    ;

moduleBase
    : '(' abstractModuleList ')' moduleOptions
    | moduleOptions
    ;

moduleOptions
    :                                                                       %prec LOWEST_PRECEDENCE     // Ensure that '(' gets shifted instead of causing a s/r error
    | moduleOptions ',' moduleOption
                        {
                            OwnedHqlExpr expr = $3.getExpr();
                            if (expr)
                                parser->appendToActiveScope(expr);
                            $$.clear();
                            $$.setPosition($3);
                        }
    ;

moduleOption
    : __NOSTREAMING__
                        {
                            //Only for internal testing...
                            $$.setExpr(createAttribute(_noStreaming_Atom), $1);
                        }
    | INTERFACE
                        {
                            $$.setExpr(createAttribute(interfaceAtom), $1);
                        }
    | VIRTUAL
                        {
                            $$.setExpr(createAttribute(virtualAtom), $1);
                        }
    | FORWARD
                        {
                            parser->processForwardModuleDefinition($1);
                            $$.setExpr(NULL, $1);
                        }
    | LIBRARY '(' scopeFunction ')'
                        {
                            $$.setExpr(createExprAttribute(libraryAtom, $3.getExpr()), $1);
                        }
    ;

abstractModuleList
    : abstractModuleItem
    | abstractModuleList ',' abstractModuleItem
    ;

abstractModuleItem
    : abstractModule
                        {
                            OwnedHqlExpr expr = $1.getExpr();
                            if (parser->checkValidBaseModule($1, expr))
                                parser->appendToActiveScope(expr);

                            $$.clear();
                            $$.setPosition($1);
                        }
    ;

//MORE: If complex type could be defined in a non assignment context then TYPE will need to become hard reserved.
complexType
    : startTYPE definitions END
                        {
                            $$.setExpr(parser->processAlienType($3),$1);
                        }
    | startTYPE END     {
                            parser->reportError(ERR_USRTYPE_EMPTYDEF,$1,"Empty user TYPE definition");
                            $$.setExpr(parser->processAlienType($2),$1);
                        }
    ;

startTYPE
    : TYPE              {
                            parser->beginAlienType($1);
                            $$.clear();
                            $$.setPosition($1);
                        }
    ;

defineid
    : UNKNOWN_ID        {
                            parser->beginDefineId($1.getId(), NULL);
                            $$.setType(NULL);
                            $$.setPosition($1);
                        }
    | SCOPE_ID          {
                            parser->beginDefineId(parser->getNameFromExpr($1), NULL);
                            $$.setType(NULL);
                            $$.setPosition($1);
                        }
    | recordDef         {
                            parser->beginDefineId(parser->getNameFromExpr($1), NULL);
                            $$.setType(NULL);
                            $$.setPosition($1);
                        }
    | TRANSFORM_ID      {
                            parser->beginDefineId(parser->getNameFromExpr($1), NULL);
                            $$.setType(NULL);
                            $$.setPosition($1);
                        }
    | TYPE_ID           {
                            parser->beginDefineId(parser->getNameFromExpr($1), NULL);
                            $$.setType(NULL);
                            $$.setPosition($1);
                        }
    | defineType knownOrUnknownId   
                        {
                            Owned<ITypeInfo> type = $1.getType();
                            if (type->getTypeCode() == type_alien)
                                type.set(type->queryPromotedType());
                            parser->beginDefineId($2.getId(), type);
                            $$.setType(type.getClear());
                            $$.setPosition($1);
                        }
    | globalScopedDatasetId knownOrUnknownId
                        {
                            OwnedHqlExpr ds = $1.getExpr();
                            parser->beginDefineId($2.getId(), ds->queryType());
                            $$.setType(ds->getType());
                            $$.setPosition($1);
                        }
    | UNKNOWN_ID UNKNOWN_ID
                        {
                            parser->reportError(ERR_UNKNOWN_TYPE, $1, "Unknown type '%s'", str($1.getId()));
                            parser->beginDefineId($2.getId(), NULL);
                            $$.setType(NULL);
                            $$.setPosition($1);
                        }
    ;


knownOrUnknownId
    : UNKNOWN_ID
    | knownId           { $$.setId(parser->getNameFromExpr($1)); $$.setPosition($1); }
    | knownFunction1    { $$.setId(parser->getNameFromExpr($1)); $$.setPosition($1); }
    ;

knownId
    : DATAROW_ID
    | DATASET_ID
    | DICTIONARY_ID
    | VALUE_ID
    | ACTION_ID
    | RECORD_ID
    | ALIEN_ID
    | TYPE_ID
    | TRANSFORM_ID
    | FEATURE_ID
    | SCOPE_ID
    | PATTERN_ID
    | LIST_DATASET_ID
    ;

knownFunction1
    : DATAROW_FUNCTION
    | DATASET_FUNCTION
    | DICTIONARY_FUNCTION
    | VALUE_FUNCTION
    | ACTION_FUNCTION
    | PATTERN_FUNCTION
    | EVENT_FUNCTION
    | TRANSFORM_FUNCTION
    | LIST_DATASET_FUNCTION
//  Following cause s/r problems

//  | RECORD_FUNCTION
//  | SCOPE_FUNCTION
    ;


scopeFlag
    : EXPORT            {   $$.setInt(EXPORT_FLAG); $$.setPosition($1); }
    | SHARED            {   $$.setInt(SHARED_FLAG); $$.setPosition($1); }
    | LOCAL             {   $$.setInt(0); $$.setPosition($1); }
    | EXPORT VIRTUAL    {   $$.setInt(EXPORT_FLAG|VIRTUAL_FLAG); $$.setPosition($1); }
    | SHARED VIRTUAL    {   $$.setInt(SHARED_FLAG|VIRTUAL_FLAG); $$.setPosition($1); }
    ;

// scopeflags needs to be explicitly included, rather than using an optScopeFlags production, otherwise you get shift reduce errors - since it is the first item on a line.

defineidWithOptScope
    : defineid          {
                            $$.setDefineId(parser->createDefineId(0, $1.getType()));
                            $$.setPosition($1);
                        }
    | scopeFlag defineid
                        {
                            $$.setDefineId(parser->createDefineId((int)$1.getInt(), $2.getType()));
                            $$.setPosition($1);
                        }
    ;

definePatternIdWithOptScope
    : definePatternId   {
                            $$.setDefineId(parser->createDefineId(0, $1.getType()));
                            $$.setPosition($1);
                        }
    | scopeFlag definePatternId  
                        {
                            $$.setDefineId(parser->createDefineId((int)$1.getInt(), $2.getType()));
                            $$.setPosition($1);
                        }
    ;

definePatternId
    : TOK_PATTERN knownOrUnknownId  
                        {
                            ITypeInfo *type = makePatternType();
                            parser->beginDefineId($2.getId(), type);
                            $$.setType(type);
                            $$.setPosition($1);
                        }
    | RULE knownOrUnknownId 
                        {
                            ITypeInfo *type = makeRuleType(NULL);
                            parser->beginDefineId($2.getId(), type);
                            $$.setType(type);
                            $$.setPosition($1);
                        }
    | RULE '(' recordDef ')' knownOrUnknownId
                        {
                            OwnedHqlExpr record = $3.getExpr();
                            ITypeInfo *type = makeRuleType(record->getType());
                            parser->beginDefineId($5.getId(), type);
                            $$.setType(type);
                            $$.setPosition($1);
                        }
    | TOKEN knownOrUnknownId    
                        {
                            ITypeInfo *type = makeTokenType();
                            parser->beginDefineId($2.getId(), type);
                            $$.setType(type);
                            $$.setPosition($1);
                        }
    | userTypedefPattern knownOrUnknownId
                        {
                            ITypeInfo *type = $1.getType();
                            parser->beginDefineId($2.getId(), type);
                            $$.setType(type);
                            $$.setPosition($1);
                        }
    ;


optDefinitions
    :
    | optDefinitions definition 
    ;

definitions
    : definition
    | definitions definition
    ;

attributeDefinition
    : DEFINITIONS_MACRO definition
    | moduleScopeDot DEFINITIONS_MACRO leaveScope definition
                        {
                            $1.release();
                            $$.clear();
                        }

    | defineidWithOptScope parmdef ASSIGN object optfailure ';'
                        {
                            if ($5.queryExpr())
                                parser->normalizeExpression($4);
                            $$.clear($1);
                            parser->defineSymbolProduction($1, $2, $3, &$4, &$5, $6);
                        }
    | definePatternIdWithOptScope parmdef featureParameters ASSIGN pattern optfailure ';'
                        {
                            parser->definePatternSymbolProduction($1, $4, $5, $6, $7);
                            $$.clear();
                        }
    | defineFeatureIdWithOptScope ';'
                        {
                            DefineIdSt* defineid = $1.getDefineId();
                            IHqlExpression *expr = createValue(no_null, makeFeatureType());
                            expr = createValue(no_pat_featuredef, expr->getType(), expr);

                            parser->doDefineSymbol(defineid, expr, NULL, $1, $2.pos.position, $2.pos.position, false);
                            $$.clear();
                        }
    | defineFeatureIdWithOptScope ASSIGN featureDefine ';'
                        {
                            DefineIdSt* defineid = $1.getDefineId();
                            IHqlExpression *expr = $3.getExpr();
                            expr = createValue(no_pat_featuredef, expr->getType(), expr);

                            parser->doDefineSymbol(defineid, expr, NULL, $1, $2.pos.position, $4.pos.position, false);
                            $$.clear();
                        }
    ;


definition
    : simpleDefinition
    | ';'               {
                            //Extra ';' are ignored, partly to reduce problems with trailing ';' inside macros
                            $$.clear();
                        }
    ;

simpleDefinition
    : attributeDefinition
    | query ';'         {
                            parser->addResult($1.getExpr(), $1);
                            $$.clear();
                        }
 /* general error */
    | error   ';'       {
                            yyerrok;
                            parser->processError(true);
                            $$.clear();
                        }
    | importSection
    | metaCommandWithNoSemicolon simpleDefinition
    ;

metaCommandWithNoSemicolon
    : setMetaCommand
                        {
                            //These are really treated like actions now, this is here for backward compatibility
                            parser->reportWarning(CategoryDeprecated, ERR_DEPRECATED, $1.pos, "#command with no trailing semicolon is deprecated");
                            parser->addResult($1.getExpr(), $1);
                            $$.clear();
                        }
   ;

moduleDefinition
    : defineidWithOptScope parmdef ';'
                        {
                            parser->defineSymbolProduction($1, $2, $3, NULL, NULL, $3);
                            $$.clear();
                            $$.setPosition($1);
                        }
    | definition
    ;


setMetaCommand
    : HASH_OPTION '(' expression ',' expression ')'
                        {
                            parser->normalizeExpression($3, type_string, true);
                            parser->normalizeExpression($5, type_any, true);
                            $$.setExpr(createValue(no_setmeta, makeVoidType(), createAttribute(debugAtom), $3.getExpr(), $5.getExpr()), $1);
                        }
    | HASH_WORKUNIT '(' expression ',' expression ')'
                        {
                            parser->normalizeExpression($3, type_string, true);
                            parser->normalizeExpression($5, type_any, true);
                            $$.setExpr(createValue(no_setmeta, makeVoidType(), createAttribute(workunitAtom), $3.getExpr(), $5.getExpr()), $1);
                        }
    | HASH_STORED '(' expression ',' hashStoredValue ')'
                        {
                            parser->normalizeStoredNameExpression($3);
                            $$.setExpr(createValue(no_setmeta, makeVoidType(), createAttribute(storedAtom), $3.getExpr(), $5.getExpr()), $1);
                        }
    | HASH_CONSTANT '(' expression ',' hashStoredValue ')'
                        {
                            parser->normalizeStoredNameExpression($3);
                            $$.setExpr(createValue(no_setmeta, makeVoidType(), createAttribute(constAtom), $3.getExpr(), $5.getExpr()), $1);
                        }
    | HASH_LINK '(' expression ')'
                        {
                            parser->normalizeExpression($3, type_string, true);
                            $$.setExpr(createValue(no_setmeta, makeVoidType(), createAttribute(linkAtom), $3.getExpr()), $1);
                        }
    | HASH_ONWARNING '(' expression ',' warningAction ')'
                        {
                            if (isNumericType($3.queryExprType()))
                            {
                                parser->normalizeExpression($3, type_int, false);
                            }
                            else
                            {
                                parser->normalizeExpression($3, type_string, false);
                            }
                            $$.setExpr(createValue(no_setmeta, makeVoidType(), createAttribute(onWarningAtom), $3.getExpr(), $5.getExpr()), $1);
                        }
    | HASH_WEBSERVICE '(' hintList ')'
                        {
                            HqlExprArray args;
                            $3.unwindCommaList(args);
                            $$.setExpr(createValue(no_setmeta, makeVoidType(), createExprAttribute(webserviceAtom, args)), $1);
                        }
    ;

hashStoredValue
    : expression
                        {
                            parser->normalizeExpression($1, type_any, false);
                            $$.inherit($1);
                        }
    | dataSet
    | dataRow
    ;

optfailure
    : ':' failclause        
                        {
                            $$.setExpr($2.getExpr(), $1); 
                        }
    |                   {   $$.setNullExpr(); $$.clearPosition(); }
    ;

failclause
    : failure
    | failclause ',' failure
                        {
                            IHqlExpression * previousWorkflow = $1.getExpr();
                            IHqlExpression * newWorkflow = $3.getExpr();
                            parser->checkWorkflowMultiples(previousWorkflow, newWorkflow, $3);
                            $$.setExpr(createComma(previousWorkflow, newWorkflow), $1);
                        }
    ;

failure
    : FAILURE '(' action ')'
                        {
                            $$.setExpr(createValue(no_failure, $3.getExpr(), NULL), $1);
                        }
    | SUCCESS '(' action ')'
                        {
                            $$.setExpr(createValue(no_success, $3.getExpr(), NULL), $1);
                        }
    | RECOVERY '(' action ')'
                        {
                            $$.setExpr(createValue(no_recovery, $3.getExpr(), createConstant(1)), $1);
                        }
    | RECOVERY '(' action ',' expression ')'
                        {
                            parser->normalizeExpression($5, type_int, true);
                            $$.setExpr(createValue(no_recovery, $3.getExpr(), $5.getExpr()), $1);
                        }
    | WHEN '(' event ')'
                        {
                            parser->checkConstantEvent($3);
                            $$.setExpr(createValue(no_when, $3.getExpr()), $1);
                        }
    | WHEN '(' event ',' COUNT '(' expression ')'  ')'
                        {
                            parser->checkConstantEvent($3);
                            parser->normalizeExpression($7, type_int, true);
                            $$.setExpr(createValue(no_when, $3.getExpr(), $7.getExpr()), $1);
                        }
    | PRIORITY '(' expression ')'
                        {
                            parser->normalizeExpression($3, type_int, true);
                            $$.setExpr(createValue(no_priority, $3.getExpr()), $1);
                        }
    | PERSIST '(' expression ')'
                        {
                            parser->normalizeExpression($3, type_string, true);
                            $$.setExpr(createValueF(no_persist, makeVoidType(), $3.getExpr(), NULL), $1);
                        }
    | PERSIST '(' expression ',' persistOpts ')'
                        {
                            parser->normalizeExpression($3, type_string, true);
                            $$.setExpr(createValueF(no_persist, makeVoidType(), $3.getExpr(), $5.getExpr(), NULL), $1);
                        }
    | PERSIST '(' expression ',' expression optPersistOpts ')'
                        {
                            parser->normalizeExpression($3, type_string, true);
                            parser->normalizeExpression($5, type_string, true);
                            $$.setExpr(createValueF(no_persist, makeVoidType(), $3.getExpr(), $5.getExpr(), $6.getExpr(), NULL), $1);
                        }
    | STORED '(' startStoredAttrs expression ',' fewMany optStoredFieldFormat ')'
                        {
                            parser->normalizeStoredNameExpression($4);
                            $$.setExpr(createValue(no_stored, makeVoidType(), $4.getExpr(), $6.getExpr(), $7.getExpr()), $1);
                        }
    | STORED '(' startStoredAttrs expression optStoredFieldFormat ')'
                        {
                            parser->normalizeStoredNameExpression($4);
                            $$.setExpr(createValue(no_stored, makeVoidType(), $4.getExpr(), $5.getExpr()), $1);
                        }
    | CHECKPOINT '(' constExpression ')'
                        {
                            parser->normalizeStoredNameExpression($3);
                            $$.setExpr(createValue(no_checkpoint, makeVoidType(), $3.getExpr()), $1);
                        }
    | GLOBAL                    
                        {
                            $$.setExpr(createValue(no_global), $1);
                        }
    | GLOBAL '(' fewMany ')'
                        {
                            $$.setExpr(createValue(no_global, $3.getExpr()), $1);
                        }
    | GLOBAL '(' expression optFewMany ')'
                        {
                            parser->normalizeExpression($3, type_string, false);
                            $$.setExpr(createValue(no_global, $3.getExpr(), $4.getExpr()), $1);
                        }
    | INDEPENDENT               
                        {
                            $$.setExpr(createValue(no_independent), $1);
                        }
    | INDEPENDENT '(' fewMany ')'
                        {
                            $$.setExpr(createValue(no_independent, $3.getExpr()), $1);
                        }
    | INDEPENDENT '(' expression optFewMany ')'
                        {
                            $$.setExpr(createValue(no_independent, $3.getExpr(), $4.getExpr()), $1);
                        }
    | DEFINE '(' stringConstExpr ')'
                        {
                            $$.setExpr(createExprAttribute(defineAtom, $3.getExpr()), $1);
                        }
    | DEPRECATED
                        {
                            $$.setExpr(createAttribute(deprecatedAtom), $1);
                        }
    | DEPRECATED '(' stringConstExpr ')'
                        {
                            $$.setExpr(createExprAttribute(deprecatedAtom, $3.getExpr()), $1);
                        }
    | SECTION '(' constExpression sectionArguments ')'
                        {
                            parser->normalizeExpression($3, type_string, false);
                            HqlExprArray args;
                            args.append(*$3.getExpr());
                            $4.unwindCommaList(args);
                            $$.setExpr(createExprAttribute(sectionAtom, args), $1);
                        }
    | ONWARNING '(' constExpression ',' warningAction ')'
                        {
                            parser->normalizeExpression($3, type_int, false);
                            $$.setExpr(createExprAttribute(onWarningAtom, $3.getExpr(), $5.getExpr()), $1);
                        }
    | LABELED '(' expression ')'
                        {
                            parser->normalizeStoredNameExpression($3);
                            $$.setExpr(createExprAttribute(labeledAtom, $3.getExpr()), $1);
                        }
    | ONCE
                        {
                            $$.setExpr(createValue(no_once, makeVoidType()), $1);
                        }
    | ONCE '(' fewMany ')'
                        {
                            $$.setExpr(createValue(no_once, $3.getExpr()), $1);
                        }
    ;

warningAction
    : TOK_LOG           {   $$.setExpr(createAttribute(logAtom), $1); }
    | TOK_IGNORE        {   $$.setExpr(createAttribute(ignoreAtom), $1); }
    | TOK_WARNING       {   $$.setExpr(createAttribute(warningAtom), $1); }
    | TOK_ERROR         {   $$.setExpr(createAttribute(errorAtom), $1); }
    | FAIL              {   $$.setExpr(createAttribute(failAtom), $1); }
    ;

optPersistOpts
    :                   {   $$.setNullExpr();   }
    | ',' persistOpts   {
                            $$.setExpr($2.getExpr(), $2);
                        }
    ;

persistOpts
    : persistOpt
    | persistOpts ',' persistOpt
                        {
                            $$.setExpr(createComma($1.getExpr(), $3.getExpr()), $1);
                        }
    ;

persistOpt
    : fewMany
    | expireAttr
    | clusterAttr
    | SINGLE            {   $$.setExpr(createAttribute(singleAtom), $1); }
    | MULTIPLE          {   $$.setExpr(createExprAttribute(multipleAtom), $1); }
    | MULTIPLE '(' expression ')'
                        {
                            parser->normalizeExpression($3, type_int, true);
                            $$.setExpr(createExprAttribute(multipleAtom, $3.getExpr()), $1);
                        }
    ;

optStoredFieldFormat
    :                   {
                            $$.setNullExpr();
                        }
    | ',' FORMAT_ATTR '(' hintList ')'
                        {
                            HqlExprArray args;
                            $4.unwindCommaList(args);
                            $$.setExpr(createExprAttribute(storedFieldFormatAtom, args), $2);
                        }
    ;

globalOpts
    :                   {   $$.setNullExpr(); }
    | ',' globalOpts2   {   $$.inherit($2); }
    ;

globalOpts2
    : globalOpt
    | globalOpts2 ',' globalOpt
                        {
                            $$.setExpr(createComma($1.getExpr(), $3.getExpr()), $1);
                        }
    ;

globalOpt
    : FEW               {   $$.setExpr(createAttribute(fewAtom), $1); }
    | MANY              {   $$.setExpr(createAttribute(manyAtom), $1); }
    | OPT               {   $$.setExpr(createAttribute(optAtom), $1); }
    ;

optFewMany
    :                   {   $$.setNullExpr(); }
    | ',' FEW           {   $$.setExpr(createAttribute(fewAtom), $1); }
    | ',' MANY          {   $$.setExpr(createAttribute(manyAtom), $1); }
    ;

fewMany
    : FEW               {   $$.setExpr(createAttribute(fewAtom), $1); }
    | MANY              {   $$.setExpr(createAttribute(manyAtom), $1); }
    ;

optKeyedDistributeAttrs
    :                   { $$.setNullExpr(); }
    | ',' keyedDistributeAttribute optKeyedDistributeAttrs
                        {
                            $$.setExpr(createComma($2.getExpr(), $3.getExpr()), $2);
                        }
    ;

keyedDistributeAttribute
    : FIRST             {   $$.setExpr(createAttribute(firstAtom), $1); }           // leading components of the key
    | hintAttribute
    ;

optDistributeAttrs
    :                   { $$.setNullExpr(); }
    | ',' distributeAttribute optDistributeAttrs
                        {
                            $$.setExpr(createComma($2.getExpr(), $3.getExpr()), $2);
                        }
    ;

distributeAttribute
    : PULLED
                        {
                            $$.setExpr(createAttribute(pulledAtom), $1);
                        }
    | hintAttribute
    | MERGE_ATTR '(' beginList sortList ')'
                        {
                            HqlExprArray sortItems;
                            parser->endList(sortItems);
                            IHqlExpression * sortlist = parser->processSortList($4, no_sortlist, NULL, sortItems, NULL, NULL);
                            $$.setExpr(createExprAttribute(mergeAtom, sortlist), $1);
                        }
    ;

transformDef
    : startTransform transformOptions transformations END
                        {
                            $$.setExpr(parser->closeTransform($4), $1);
                            parser->leaveCompoundObject();
                        }
    | startTransform transformOptions END
                        {
                            parser->reportError(ERR_TRANSFORM_EMPTYDEF,$1,"Empty transform definition");
                            $$.setExpr(parser->closeTransform($3), $1);
                            parser->leaveCompoundObject();
                        }
    ;

transform
    : TRANSFORM_ID
    | moduleScopeDot TRANSFORM_ID leaveScope
                        {
                            $1.release();
                            $$.setExpr($2.getExpr(), $1);
                        }
    | transformFunction '('
                        {
                            parser->beginFunctionCall($1);
                        }
    actualParameters ')'
                        {
                            $$.setExpr(parser->bindParameters($1, $4.getExpr()), $1);
                        }
    | startInlineTransform transformOptions semiComma transformations ')'
                        {
                            $$.setExpr(parser->closeTransform($4), $1);
                            parser->leaveCompoundObject();
                        }
    | TRANSFORM '(' dataRow ')'
                        {
                            OwnedHqlExpr value = $3.getExpr();
                            IHqlExpression * record = value->queryRecord();
                            $$.setExpr(parser->createDefaultAssignTransform(record, value, $1), $1);
                        }
    | startCompoundExpression beginInlineFunctionToken optDefinitions RETURN transform ';' endInlineFunctionToken
                        {
                            Owned<ITypeInfo> retType = $1.getType();
                            $$.setExpr(parser->leaveLamdaExpression($5), $7);
                        }
    ;


startInlineTransform
    : TRANSFORM '(' recordDef
                        {
                            OwnedHqlExpr record = $3.getExpr();
                            Owned<ITypeInfo> type = createRecordType(record);
                            parser->openTransform(type);
                            $$.clear($1);
                        }
    ;

opt_join_transform_flags
    : ',' transform optJoinFlags
                        {
                            IHqlExpression* flags = $3.getExpr();
                            IHqlExpression* trans_expr = $2.getExpr();

                            // some fatal error happened
                            if (!trans_expr || !trans_expr->isTransform())
                            {
                                ::Release(trans_expr);
                                ::Release(flags);
                                $$.setNullExpr();
                            }
                            else
                                $$.setExpr(createComma(trans_expr, flags));
                            $$.setPosition($2);
                        }
    | optJoinFlags
    ;

startTransform
    :   TRANSFORM       {
                            parser->processStartTransform($1);
                            $$.clear($1);
                        }
    ;

transformOptions
    :
    | transformOptions semiComma transformOption
                        {
                            parser->appendTransformOption($3.getExpr());
                            $$.setPosition($1);
                        }
    ;

transformOption
    : SKIP '(' booleanExpr ')'  
                        {
                            $$.setExpr(createValue(no_skip, makeVoidType(), $3.getExpr()), $1);
                        }
    ;

transformations
    : transformation
    | transformations semiComma transformation
    | transformations semiComma
                        {
                            $$.setPosition($1);
                        }
    ;

transformation
    : transformPrefix transformation1
                        {
                            $$.setPosition($2);
                        }
    ;

transformPrefix
    :
    | transformPrefixList
    ;

transformPrefixList
    : transformPrefixItem
    | transformPrefixList transformPrefixItem
    | transformPrefixList ';'
    ;

transformPrefixItem
    : DEFINITIONS_MACRO
    | moduleScopeDot DEFINITIONS_MACRO leaveScope
                        {
                            $1.release();
                            $$.clear();
                        }
    | defineTransformObject
    | conditionalAttributeAssignment ';'
    | importSection
    ;

conditionalAttributeAssignment      // not a conditional field assignment.....
    : IF expression THEN conditionalAssignments conditionalAssignmentElseClause
                        {
                            //Allow IF integer to ease SAS translation
                            parser->normalizeExpression($2);
                            parser->ensureBoolean($2);
                            OwnedHqlExpr cond = $2.getExpr();
                            OwnedHqlExpr trueScope = $4.getExpr();
                            OwnedHqlExpr falseScope = $5.getExpr();
                            parser->processIfScope($1,cond, trueScope, falseScope);
                            $$.clear();
                        }
    ;


conditionalAssignments
    : beginConditionalScope transformPrefix
                        {
                            $$.setExpr(queryExpression(parser->closeLeaveScope($2)), $2);
                        }
    ;


beginConditionalScope
    :                   {   parser->enterScope(false); $$.clear(); }
    ;

conditionalAssignmentElseClause
    : END               {   $$.setNullExpr(); }
    | ELSE conditionalAssignments END
                        {   $$.setExpr($2.getExpr(), $2); }
    | ELSEIF expression THEN conditionalAssignments conditionalAssignmentElseClause
                        {
                            parser->normalizeExpression($2);
                            parser->ensureBoolean($2);
                            //normalizeExpression($2, type_boolean, false);
                            OwnedHqlExpr map = createValue(no_mapto, $2.getExpr(), $4.getExpr());
                            $$.setExpr(createComma(map.getClear(), $5.getExpr()), $1);
                        }
    ;


defineTransformObject
    : beginDefineTransformObject parmdef ASSIGN object optfailure ';'
                        {
                            parser->defineSymbolProduction($1, $2, $3, &$4, &$5, $6);
                            $$.clear($1);

                            parser->restoreTypeFromActiveTransform();
                        }
    ;

beginDefineTransformObject
    : defineid
                        {
                            $$.setDefineId(parser->createDefineId(0, $1.getType()));
                            $$.setPosition($1);
                        }
    ;

transformation1
    : transformDst ASSIGN expression
                        {
                            parser->normalizeExpression($3);
                            ITypeInfo * type = $1.queryExprType();
                            if (!type || type->getTypeCode() != type_set)
                            {
                                IHqlExpression * arg = $3.queryExpr();
                                if ((arg->getOperator() == no_list) && (arg->numChildren() == 0))
                                {
                                    $3.release().setExpr(createValue(no_null));
                                }
                            }
                            parser->addAssignment($1, $3);
                            $$.clear($1);
                        }
    | transformDst ASSIGN dataRow
                        {
                            IHqlExpression * value = $3.queryExpr();
                            if (value)
                            {
                                IHqlExpression * target = $1.queryExpr();
                                if (target->isDataset())
                                {
                                    value = createDatasetFromRow(LINK(value));
                                    $3.release();
                                    $3.setExpr(value);
                                    parser->addAssignment($1, $3);
                                }
                                else
                                    parser->addAssignall($1.getExpr(), $3.getExpr(), $1);
                            }
                            else /* this happens when an error C2022 occurred */
                                $1.release();
                            $$.clear($1);
                        }
    | transformDst ASSIGN dataSet
                        {
                            parser->addAssignment($1, $3);
                            $$.clear($1);
                        }
    | transformDst ASSIGN dictionary
                        {
                            parser->addAssignment($1, $3);
                            $$.clear($1);
                        }
    | transformDst ASSIGN error ';'
                        {
                            $1.release();
                            $$.clear();
                        }
    | assertAction      {
                            parser->appendTransformOption($1.getExpr());
                            $$.clear($1);
                        }
    ;

//----------------------------------------------------------------------------
transformDst
    : transformDstRecord leaveScope
                        {
                            $$.setExpr($1.getExpr(), $1);
                        }
    | transformDstRecord '.' transformDstField
                        {
                            OwnedHqlExpr lhs = $1.getExpr();
                            if (lhs->isDataset())
                                parser->reportError(ERR_ASSIGN_MEMBER_DATASET, $1, "Cannot directly assign to field %s within a child dataset", str($3.queryExpr()->queryName()));
                            $$.setExpr(parser->createSelect(lhs.getClear(), $3.getExpr(), $3), $1);
                        }
    ;

transformDstRecord
    : startSelf
    | transformDstRecord '.' transformDstSelect
                        {
                            OwnedHqlExpr lhs = $1.getExpr();
                            if (lhs->isDataset())
                                parser->reportError(ERR_ASSIGN_MEMBER_DATASET, $1, "Cannot directly assign to field %s within a child dataset", str($3.queryExpr()->queryName()));
                            $$.setExpr(parser->createSelect(lhs.getClear(), $3.getExpr(), $3), $1);
                        }
    | transformDstRecord '.' transformDstSelect leaveScope '[' expression ']'
                        {
                            parser->normalizeExpression($6, type_int, false);
                            parser->reportError(ERR_ASSIGN_MEMBER_DATASET, $1, "Cannot assign to individual elements of a list or dataset");
                            parser->setDotScope($3.queryExpr());
                            // Of course this is not the way arrays are going to work,
                            // need to adjust to whatever is necessary when we implement it - YMA.
                            $$.setExpr(parser->createSelect($1.getExpr(), $3.getExpr(), $3), $1);
                            $6.release();
                        }
    ;

startSelf
    : SELF              {
                            OwnedHqlExpr self = parser->getSelfScope();
                            if (!self)
                                self.set(queryNullRecord());
                            parser->setDotScope(self);
                            $$.setExpr(getSelf(self), $1);
                        }
    ;

transformDstSelect
    : DATAROW_ID
                        {
                            OwnedHqlExpr scope = $1.getExpr();
                            parser->setDotScope(scope);
                            $$.setExpr(scope.getClear(), $1);
                        }
    | RECORD_ID
                        {
                            OwnedHqlExpr scope = $1.getExpr();
                            parser->setDotScope(scope);
                            $$.setExpr(scope.getClear(), $1);
                        }
    | DATASET_ID
                        {
                            OwnedHqlExpr scope = $1.getExpr();
                            parser->setDotScope(scope);
                            $$.setExpr(scope.getClear(), $1);
                        }
    | DICTIONARY_ID
                        {
                            OwnedHqlExpr scope = $1.getExpr();
                            parser->setDotScope(scope);
                            $$.setExpr(scope.getClear(), $1);
                        }
    ;

transformDstField
    : VALUE_ID leaveScope
    | VALUE_ID leaveScope '[' expression ']'
                        {
                            parser->normalizeExpression($4, type_int, false);
                            parser->reportError(ERR_ASSIGN_MEMBER_DATASET, $1, "Cannot assign to individual elements of a list or dataset");
                            $$.setExpr($1.getExpr(), $1);
                            $4.release();
                        }
    | startPointerToMember leaveScope VALUE_ID_REF endPointerToMember
                        {
                            OwnedHqlExpr expr = $3.getExpr();
                            $$.setExpr(createValue(no_indirect, expr->getType(), LINK(expr)), $1);
                        }
    ;

//----------------------------------------------------------------------------

dotScope
    : dataSet '.'       {
                            IHqlExpression *e = $1.getExpr();
                            parser->setDotScope(e);
                            $$.setExpr(e, $1);
                        }
    | SELF '.'          {
                            $$.setExpr(parser->getSelfDotExpr($1), $1);
                        }
    | dataRow '.'       {
                            IHqlExpression *e = $1.getExpr();
                            parser->setDotScope(e);
                            $$.setExpr(e, $1);
                        }
    | enumTypeId '.'    {
                            IHqlExpression *e = $1.getExpr();
                            parser->setDotScope(e);
                            $$.setExpr(e, $1);
                        }
    ;

recordScope
    : globalRecordId '.'
                        {
                            IHqlExpression *e = $1.getExpr();
                            parser->setDotScope(e);
                            $$.setExpr(e, $1);
                        }
    | recordScope DATAROW_ID '.'
                        {
                            IHqlExpression * scope = $1.getExpr();
                            IHqlExpression * e = $2.getExpr();
                            IHqlExpression * select = createSelectExpr(scope, e);
                            parser->setDotScope(select);
                            $$.setExpr(select, $1);
                        }
    | recordScope DATASET_ID '.'
                        {
                            IHqlExpression * scope = $1.getExpr();
                            IHqlExpression * e = $2.getExpr();
                            IHqlExpression * select = createSelectExpr(scope, e);
                            parser->setDotScope(select);
                            $$.setExpr(select, $1);
                        }
    ;

simpleRecord
    : recordScope DATAROW_ID leaveScope
                        {
                            OwnedHqlExpr e = $2.getExpr();
                            $1.release();
                            $$.setExpr(LINK(queryOriginalRecord(e)), $1);
                        }
    | globalRecordId
    ;


globalRecordId
    : RECORD_ID
    | moduleScopeDot RECORD_ID leaveScope
                        {
                            $1.release();
                            $$.setExpr($2.getExpr(), $1);
                        }
    | recordFunction '('
                        {
                            parser->beginFunctionCall($1);
                        }
      actualParameters ')'
                        {
                            $$.setExpr(parser->bindParameters($1, $4.getExpr()), $1);
                        }
    ;


semiComma
    : ';'
    | ','
    ;

actionlist
    : action
                        {
                            parser->addListElement($1.getExpr());
                            $$.clear();
                        }
    | actionlist semiComma action
                        {
                            parser->addListElement($3.getExpr());
                            $$.clear();
                        }
    ;

sequentialActionlist
    : sequentialAction
                        {
                            parser->addListElement($1.getExpr());
                            $$.clear();
                        }
    | sequentialActionlist semiComma sequentialAction
                        {
                            parser->addListElement($3.getExpr());
                            $$.clear();
                        }
    ;

sequentialAction
    : action
    | expression
    ;
    
action
    : actionStmt
                        {
                            $1.annotateExprWithLocation();
                            $$.inherit($1);
                        }
    | setMetaCommand    {
                            $1.annotateExprWithLocation();
                            $$.inherit($1);
                        }
    ;

actionStmt
    : scopedActionId
    | LOADXML '(' expression ')'
                        {
                            parser->processLoadXML($3, NULL);
                            // use an expr other than NULL to distinguish from error
                            $$.setExpr(createValue(no_loadxml, makeVoidType()), $1);
                        }
    | LOADXML '(' expression ',' expression ')'
                        {
                            parser->processLoadXML($3, &$5);
                            // use an expr other than NULL to distinguish from error
                            $$.setExpr(createValue(no_loadxml, makeVoidType()), $1);
                        }
    | UPDATE '(' startLeftSeqFilter ',' transform ')' endSelectorSequence
                        {
                            $$.setExpr(createValue(no_update, makeVoidType(), $3.getExpr(), $5.getExpr(), $7.getExpr()), $1);
                        }
    | BUILD '(' startTopFilter ',' ',' thorFilenameOrList optBuildFlags ')' endTopFilter
                        {
                            $$.setExpr(parser->processIndexBuild($3, NULL, NULL, $6, $7), $1);
                            parser->processUpdateAttr($$);
                        }
    | BUILD '(' startTopFilter ',' recordDef ',' thorFilenameOrList optBuildFlags ')' endTopFilter
                        {
                            $$.setExpr(parser->processIndexBuild($3, &$5, NULL, $7, $8), $1);
                            parser->processUpdateAttr($$);
                        }
    | BUILD '(' startTopFilter ',' recordDef ',' nullRecordDef ',' thorFilenameOrList optBuildFlags ')' endTopFilter
                        {
                            $$.setExpr(parser->processIndexBuild($3, &$5, &$7, $9, $10), $1);
                            parser->processUpdateAttr($$);
                        }
    | BUILD '(' startTopFilter optBuildFlags ')' endTopFilter
                        {
                            $$.setExpr(parser->createBuildIndexFromIndex($3, $4, $5), $1);
                            parser->processUpdateAttr($$);
                        }
    | OUTPUT '(' startTopFilter ',' optRecordDef endTopFilter optOutputFlags ')'
                        {
                            IHqlExpression *dataset = $3.getExpr();
                            parser->checkOutputRecord($5, true);
                            IHqlExpression *record = $5.getExpr();
                            HqlExprArray options;
                            $7.unwindCommaList(options);

                            OwnedHqlExpr select = createDatasetF(no_selectfields, dataset, record, NULL);
                            IHqlExpression * filename = options.ordinality() ? &options.item(0) : NULL;
                            if (!filename || filename->isAttribute())
                            {
                                if (queryAttribute(extendAtom, options) && !queryAttribute(namedAtom, options))
                                    parser->reportError(ERR_EXTEND_NOT_VALID, $7, "EXTEND is only valid on NAMED outputs");
                            }
                            else
                            {
                                if (queryAttribute(extendAtom, options))
                                    parser->reportError(ERR_NOLONGER_SUPPORTED,$7,"EXTEND is no longer supported on OUTPUT to file");

                                if (filename->isPure())
                                {
                                    DependenciesUsed dependencies(true);
                                    gatherDependencies(dataset, dependencies, GatherFileRead);

                                    OwnedHqlExpr normalized = getNormalizedFilename(filename);
                                    if (dependencies.tablesRead.find(*normalized) != NotFound)
                                        parser->reportError(ERR_OUTPUT_TO_INPUT, $7, "Cannot OUTPUT to a file used as an input");
                                }
                                parser->warnIfRecordPacked(select, $1);
                            }

                            HqlExprArray args;
                            args.append(*select.getClear());
                            appendArray(args, options);
                            $$.setExpr(createValue(no_output, makeVoidType(), args), $1);
                            parser->processUpdateAttr($$);
                        }
    | OUTPUT '(' startTopFilter ',' optRecordDef endTopFilter ',' pipe optCommonAttrs ')'
                        {
                            IHqlExpression *dataset = $3.getExpr();
                            parser->checkOutputRecord($5, true);

                            OwnedHqlExpr pipe = $8.getExpr();
                            OwnedHqlExpr record = $5.getExpr();
                            HqlExprArray args;
                            if (record->getOperator() == no_null)
                                args.append(*dataset);
                            else
                            {
                                IHqlExpression * arg = pipe;
                                if (arg->getOperator() == no_comma)
                                    arg = arg->queryChild(0);
                                assertex(arg->getOperator() == no_pipe);
                                arg = arg->queryChild(0);
                                OwnedHqlExpr mapped = replaceSelector(arg, dataset, queryActiveTableSelector());
                                if (mapped != arg)
                                    parser->reportError(ERR_ROWPIPE_AND_PROJECT, $8, "OUTPUT to PIPE with a projecting record doesn't currently work when the command is dependant on the current row");
                                args.append(*createDatasetF(no_selectfields, dataset, LINK(record), NULL)); //createUniqueId(), NULL));
                            }
                            pipe->unwindList(args, no_comma);
                            $9.unwindCommaList(args);
                            IHqlExpression * output = queryAttribute(outputAtom, args);
                            if (output)
                            {
                                unwindChildren(args, output);
                                args.zap(*output);
                            }
                            if (queryAttribute(csvAtom, args))
                                parser->checkValidCsvRecord($3, args.item(0).queryRecord());

                            parser->warnIfRecordPacked(&args.item(0), $1);
                            $$.setExpr(createValue(no_output, makeVoidType(), args), $1);
                        }
    | OUTPUT '(' startTopFilter optOutputWuFlags ')' endTopFilter
                        {
                            IHqlExpression *dataset = $3.getExpr();
                            IHqlExpression *record = createValue(no_null);
                            OwnedHqlExpr select = createDatasetF(no_selectfields, dataset, record, NULL); //createUniqueId(), NULL);
                            OwnedHqlExpr flags = $4.getExpr();

                            if (queryAttributeInList(extendAtom, flags) && !queryAttributeInList(namedAtom, flags))
                                parser->reportError(ERR_EXTEND_NOT_VALID, $4, "EXTEND is only valid on NAMED outputs");

                            HqlExprArray args;
                            args.append(*select.getClear());
                            if (flags)
                                flags->unwindList(args, no_comma);
                            $$.setExpr(createValue(no_output, makeVoidType(), args), $1);
                            parser->processUpdateAttr($$);
                        }
    | OUTPUT '(' expression optOutputWuFlags ')'
                        {
                            parser->normalizeExpression($3);
                            OwnedHqlExpr flags = $4.getExpr();
                            if (queryAttributeInList(extendAtom, flags))
                                parser->reportError(ERR_EXTEND_NOT_VALID, $4, "EXTEND is only valid on a dataset");
                            HqlExprArray args;
                            args.append(*$3.getExpr());
                            if (flags)
                                flags->unwindList(args, no_comma);
                            $$.setExpr(createValue(no_outputscalar, makeVoidType(), args), $1);
                        }
    | OUTPUT '(' dictionary optOutputWuFlags ')'
                        {
                            parser->normalizeExpression($3);
                            OwnedHqlExpr flags = $4.getExpr();
                            if (queryAttributeInList(extendAtom, flags))
                                parser->reportError(ERR_EXTEND_NOT_VALID, $4, "EXTEND is only valid on a dataset");
                            HqlExprArray args;
                            args.append(*createDataset(no_datasetfromdictionary, $3.getExpr()));
                            if (flags)
                                flags->unwindList(args, no_comma);
                            $$.setExpr(createValue(no_output, makeVoidType(), args), $1);
                            parser->processUpdateAttr($$);
                        }
    | OUTPUT '(' dataRow optOutputWuFlags ')'
                        {
                            OwnedHqlExpr flags = $4.getExpr();
                            if (queryAttributeInList(extendAtom, flags))
                                parser->reportError(ERR_EXTEND_NOT_VALID, $4, "EXTEND is only valid on a dataset");
                            HqlExprArray args;
                            args.append(*$3.getExpr());
                            if (flags)
                                flags->unwindList(args, no_comma);
                            $$.setExpr(createValue(no_outputscalar, makeVoidType(), args), $1);
                        }
    | APPLY '(' startTopFilter ',' applyActions ')' endTopFilter
                        {
                            OwnedHqlExpr actions = $5.getExpr();
                            HqlExprArray args;
                            args.append(*$3.getExpr());
                            actions->unwindList(args, no_comma);
                            $$.setExpr(createValue(no_apply, makeVoidType(), args), $1);
                        }
    | IF '(' booleanExpr ',' action ',' action ')'
                        {
                            $$.setExpr(createValue(no_if, makeVoidType(), $3.getExpr(), $5.getExpr(), $7.getExpr()), $1);
                        }
    | IF '(' booleanExpr ',' action ')'
                        {
                            $$.setExpr(createValue(no_if, makeVoidType(), $3.getExpr(), $5.getExpr()), $1);
                        }
    | IFF '(' booleanExpr ',' action ',' action ')'
                        {
                            $$.setExpr(createValue(no_if, makeVoidType(), $3.getExpr(), $5.getExpr(), $7.getExpr()), $1);
                        }
    | IFF '(' booleanExpr ',' action ')'
                        {
                            $$.setExpr(createValue(no_if, makeVoidType(), $3.getExpr(), $5.getExpr()), $1);
                        }
    | MAP '(' mapActionSpec ',' action ')'
                        {
                            HqlExprArray args;
                            $3.unwindCommaList(args);
                            args.append(*$5.getExpr());
                            $$.setExpr(createValue(no_map, makeVoidType(), args), $1);
                        }
    | MAP '(' mapActionSpec ')'
                        {
                            HqlExprArray args;
                            $3.unwindCommaList(args);
                            args.append(*createValue(no_null, makeVoidType()));
                            $$.setExpr(createValue(no_map, makeVoidType(), args), $1);
                        }
    | CASE '(' expression ',' beginList caseActionSpec ',' action ')'
                        {
                            parser->normalizeExpression($3);
                            HqlExprArray args;
                            parser->endList(args);
                            parser->checkCaseForDuplicates(args, $6);
                            args.add(*$3.getExpr(),0);
                            args.append(*$8.getExpr());
                            $$.setExpr(createValue(no_case, makeVoidType(), args), $1);
                        }
    | CASE '(' expression ',' beginList caseActionSpec ')'
                        {
                            parser->normalizeExpression($3);
                            HqlExprArray args;
                            parser->endList(args);
                            parser->checkCaseForDuplicates(args, $6);
                            args.add(*$3.getExpr(),0);
                            args.append(*createValue(no_null, makeVoidType()));
                            $$.setExpr(createValue(no_case, makeVoidType(), args), $1);
                        }
    | CASE '(' expression ',' beginList action ')'
                        {
                            parser->normalizeExpression($3);
                            // change error to warning.
                            parser->reportWarning(CategoryUnusual, WRN_CASENOCONDITION, $1.pos, "CASE does not have any conditions");
                            HqlExprArray list;
                            parser->endList(list);
                            ::Release($3.getExpr());
                            $$.setExpr($6.getExpr(), $1);
                        }
    | WAIT '(' event ')'
                        {
                            $$.setExpr(createValue(no_wait, makeVoidType(), $3.getExpr()), $1);
                        }
    | WAIT '(' event ',' IF '(' booleanExpr ')' ')' 
                        {
                            $$.setExpr(createValue(no_wait, makeVoidType(), $3.getExpr(), $7.getExpr()), $1);
                        }
    | NOTIFY '(' expression ',' expression ')'  
                        {
                            parser->normalizeExpression($3, type_string, false);
                            parser->normalizeExpression($5, type_string, false);
                            OwnedHqlExpr event = createValue(no_event, makeEventType(), $3.getExpr(), $5.getExpr());
                            $$.setExpr(createValue(no_notify, makeVoidType(), event.getClear()), $1);
                        }
    | NOTIFY '(' expression ',' expression ',' expression ')'   
                        {
                            parser->normalizeExpression($3, type_string, false);
                            parser->normalizeExpression($5, type_string, false);
                            parser->normalizeExpression($7, type_string, false);
                            OwnedHqlExpr event = createValue(no_event, makeEventType(), $3.getExpr(), $5.getExpr());
                            $$.setExpr(createValue(no_notify, makeVoidType(), event.getClear(), $7.getExpr()), $1);
                        }
    | NOTIFY '(' eventObject ')'
                        {
                            $$.setExpr(createValue(no_notify, makeVoidType(), $3.getExpr()), $1);
                        }
    | NOTIFY '(' eventObject ',' expression ')' 
                        {
                            parser->normalizeExpression($5, type_string, false);
                            $$.setExpr(createValue(no_notify, makeVoidType(), $3.getExpr(), $5.getExpr()), $1);
                        }
    | NOFOLD '(' action ')'
                        {
                            $$.setExpr(createValue(no_nofold, makeVoidType(), $3.getExpr()), $1);
                        }
    | NOTHOR '(' action ')'
                        {
                            $$.setExpr(createValue(no_nothor, makeVoidType(), $3.getExpr()), $1);
                        }
    | failAction
    | SEQUENTIAL '(' beginList sequentialActionlist optSemiComma ')'
                        {
                            HqlExprArray actions;
                            parser->endList(actions);
                            $$.setExpr(createValue(no_sequential, makeVoidType(), actions), $1);
                        }
    | PARALLEL '(' beginList sequentialActionlist optSemiComma ')'
                        {
                            HqlExprArray actions;
                            parser->endList(actions);
                            $$.setExpr(createValue(no_parallel, makeVoidType(), actions), $1);
                        }
    | ORDERED '(' beginList sequentialActionlist optSemiComma ')'
                        {
                            HqlExprArray actions;
                            parser->endList(actions);
                            $$.setExpr(createValue(no_orderedactionlist, makeVoidType(), actions), $1);
                        }
    | SOAPCALL '(' expression ',' expression ',' recordDef ')'
                        {
                            parser->normalizeExpression($3, type_stringorunicode, false);
                            parser->normalizeExpression($5, type_stringorunicode, false);
                            parser->checkSoapRecord($7);
                            $$.setExpr(createValue(no_soapcall, makeVoidType(), $3.getExpr(), $5.getExpr(), $7.getExpr()), $1);
                        }
    | SOAPCALL '(' expression ',' expression ',' recordDef ',' soapFlags ')'
                        {
                            parser->normalizeExpression($3, type_stringorunicode, false);
                            parser->normalizeExpression($5, type_stringorunicode, false);
                            parser->checkSoapRecord($7);
                            $$.setExpr(createValueF(no_soapcall, makeVoidType(), $3.getExpr(), $5.getExpr(), $7.getExpr(), $9.getExpr(), NULL), $1);
                        }
    | SOAPCALL '(' expression ',' expression ',' recordDef ',' transform ')'
                        {
                            parser->normalizeExpression($3, type_stringorunicode, false);
                            parser->normalizeExpression($5, type_stringorunicode, false);
                            $$.setExpr(createValue(no_newsoapcall, makeVoidType(), $3.getExpr(), $5.getExpr(), $7.getExpr(), $9.getExpr()), $1);
                        }
    | SOAPCALL '(' expression ',' expression ',' recordDef ',' transform ',' soapFlags ')'
                        {
                            parser->normalizeExpression($3, type_stringorunicode, false);
                            parser->normalizeExpression($5, type_stringorunicode, false);
                            $$.setExpr(createValueF(no_newsoapcall, makeVoidType(), $3.getExpr(), $5.getExpr(), $7.getExpr(), $9.getExpr(), $11.getExpr(), NULL), $1);
                        }
    | SOAPCALL '(' startTopLeftSeqFilter ',' expression ',' expression ',' recordDef ')' endTopLeftFilter endSelectorSequence
                        {
                            parser->normalizeExpression($5, type_stringorunicode, false);
                            parser->normalizeExpression($7, type_stringorunicode, false);
                            parser->checkSoapRecord($9);
                            $$.setExpr(createValueF(no_soapaction_ds, makeVoidType(), $3.getExpr(), $5.getExpr(), $7.getExpr(), $9.getExpr(), $12.getExpr(), NULL), $1);
                        }
    | SOAPCALL '(' startTopLeftSeqFilter ',' expression ',' expression ',' recordDef ',' soapFlags ')' endTopLeftFilter endSelectorSequence
                        {
                            parser->normalizeExpression($5, type_stringorunicode, false);
                            parser->normalizeExpression($7, type_stringorunicode, false);
                            parser->checkSoapRecord($9);
                            $$.setExpr(createValueF(no_soapaction_ds, makeVoidType(), $3.getExpr(), $5.getExpr(), $7.getExpr(), $9.getExpr(), $11.getExpr(), $14.getExpr(), NULL), $1);
                        }
    | SOAPCALL '(' startTopLeftSeqFilter ',' expression ',' expression ',' recordDef ',' transform ')' endTopLeftFilter endSelectorSequence
                        {
                            parser->normalizeExpression($5, type_stringorunicode, false);
                            parser->normalizeExpression($7, type_stringorunicode, false);
                            $$.setExpr(createValueF(no_newsoapaction_ds, makeVoidType(), $3.getExpr(), $5.getExpr(), $7.getExpr(), $9.getExpr(), $11.getExpr(), $14.getExpr(), NULL));
                            $$.setPosition($1);
                        }
    | SOAPCALL '(' startTopLeftSeqFilter ',' expression ',' expression ',' recordDef ',' transform ',' soapFlags ')' endTopLeftFilter endSelectorSequence
                        {
                            parser->normalizeExpression($5, type_stringorunicode, false);
                            parser->normalizeExpression($7, type_stringorunicode, false);
                            $$.setExpr(createValueF(no_newsoapaction_ds, makeVoidType(), $3.getExpr(), $5.getExpr(), $7.getExpr(), $9.getExpr(), $11.getExpr(), $13.getExpr(), $16.getExpr(), NULL));
                            $$.setPosition($1);
                        }
    | KEYDIFF '(' dataSet ',' dataSet ',' expression keyDiffFlags ')'
                        {
                            if (!isKey($3.queryExpr()))
                                parser->reportError(ERR_EXPECTED_INDEX,$3,"Expected an index");
                            if (!isKey($5.queryExpr()))
                                parser->reportError(ERR_EXPECTED_INDEX,$5,"Expected an index");
                            if (!recordTypesMatch($3.queryExpr(), $5.queryExpr()))
                                parser->reportError(ERR_TYPEMISMATCH_RECORD, $4, "Indexes must have the same structure");
                            parser->normalizeExpression($7, type_string, false);
                            $$.setExpr(createValueFromCommaList(no_keydiff, makeVoidType(), createComma($3.getExpr(), $5.getExpr(), $7.getExpr(), $8.getExpr())));
                            $$.setPosition($1);
                        }
    | KEYPATCH '(' dataSet ',' expression ',' expression keyDiffFlags ')'
                        {
                            if (!isKey($3.queryExpr()))
                                parser->reportError(ERR_EXPECTED_INDEX,$3,"Expected an index");
                            parser->normalizeExpression($5, type_string, false);
                            parser->normalizeExpression($7, type_string, false);
                            $$.setExpr(createValueFromCommaList(no_keypatch, makeVoidType(), createComma($3.getExpr(), $5.getExpr(), $7.getExpr(), $8.getExpr())));
                            $$.setPosition($1);
                        }
    | EVALUATE '(' expression ')'
                        {
                            parser->normalizeExpression($3);
                            $$.setExpr(createValue(no_evaluate_stmt, makeVoidType(), $3.getExpr()));
                            $$.setPosition($1);
                        }
    | EVALUATE '(' action ')'
                        {
                            $$.inherit($3);
                        }
    | EVALUATE '(' abstractModule ')'
                        {
                            OwnedHqlExpr abstract = $3.getExpr();
                            OwnedHqlExpr concrete = parser->checkConcreteModule($3, abstract);
                            $$.setExpr(parser->createEvaluateOutputModule($3, concrete, concrete, no_evaluate_stmt, NULL), $1);
                        }
    | EVALUATE '(' abstractModule ',' knownOrUnknownId ')'
                        {
                            OwnedHqlExpr abstract = $3.getExpr();
                            OwnedHqlExpr concrete = parser->checkConcreteModule($3, abstract);
                            $$.setExpr(parser->createEvaluateOutputModule($3, concrete, concrete, no_evaluate_stmt, $5.getId()), $1);
                        }
    | DISTRIBUTION '(' startTopFilter beginList optDistributionFlags ignoreDummyList ')' endTopFilter
                        {
                            $$.setExpr(createValue(no_distribution, makeVoidType(), $3.getExpr(), $5.getExpr()), $1);
                        }
    | DISTRIBUTION '(' startTopFilter beginList ',' sortList optDistributionFlags ')' endTopFilter
                        {
                            HqlExprArray sortItems;
                            parser->endList(sortItems);
                            IHqlExpression * dataset = $3.getExpr();
                            IHqlExpression * fields = parser->processSortList($6, no_sortlist, dataset, sortItems, NULL, NULL);
                            $$.setExpr(createValue(no_distribution, makeVoidType(), dataset, fields, $7.getExpr()));
                        }
    | assertAction
    | GLOBAL '(' action ')'
                        {
                            $$.setExpr($3.getExpr());
                            $$.setPosition($1);
                        }
    | GLOBAL '(' action ',' expression ')'
                        {
                            parser->normalizeExpression($5, type_string, true);
                            if (isBlankString($5.queryExpr()))
                            {
                                $5.release();
                                $$.setExpr($3.getExpr());
                            }
                            else
                                $$.setExpr(createValueF(no_cluster, makeVoidType(), $3.getExpr(), $5.getExpr(), NULL));
                            $$.setPosition($1);
                        }
    | OUTPUT '(' abstractModule ')'
                        {
                            OwnedHqlExpr abstract = $3.getExpr();
                            OwnedHqlExpr concrete = parser->checkConcreteModule($3, abstract);
                            $$.setExpr(parser->createEvaluateOutputModule($3, concrete, concrete, no_output, NULL));
                            $$.setPosition($1);
                        }
    | OUTPUT '(' abstractModule ',' abstractModule ')'
                        {
                            OwnedHqlExpr abstract = $3.getExpr();
                            OwnedHqlExpr concrete = parser->checkConcreteModule($3, abstract);
                            OwnedHqlExpr iface = $5.getExpr();
                            $$.setExpr(parser->createEvaluateOutputModule($3, concrete, iface, no_output, NULL));
                            $$.setPosition($1);
                        }
    | ALLNODES '(' beginList actionlist ')'
                        {
                            HqlExprArray actions;
                            parser->endList(actions);
                            $$.setExpr(createValue(no_allnodes, makeVoidType(), createCompound(actions)));
                            $$.setPosition($1);
                        }
    | '[' beginList actionlist ']'
                        {
                            HqlExprArray actions;
                            parser->endList(actions);
                            $$.setExpr(createActionList(no_orderedactionlist, actions), $1);
                        }
    | OUTPUT '(' action ')'
                        {
                            parser->reportError(ERR_EXPECTED, $3, "OUTPUT cannot be applied to an action");
                            $$.inherit($3);
                        }
    ;



failAction
    : FAIL '(' expression ',' expression ')'
                        {
                            parser->normalizeExpression($3, type_int, false);
                            parser->normalizeExpression($5, type_string, false);
                            $$.setExpr(createValue(no_fail, makeVoidType(), $3.getExpr(), $5.getExpr()));
                            $$.setPosition($1);
                        }
    | FAIL '(' expression ')'
                        {
                            parser->normalizeExpression($3);
                            parser->checkIntegerOrString($3);
                            $$.setExpr(createValue(no_fail, makeVoidType(), $3.getExpr()));
                            $$.setPosition($1);
                        }
    | FAIL '(' ')'      
                        {
                            $$.setExpr(createValue(no_fail, makeVoidType()));
                            $$.setPosition($1);
                        }
    | FAIL              {
                            $$.setExpr(createValue(no_fail, makeVoidType()));
                            $$.setPosition($1);
                        }
    | startCompoundExpression beginInlineFunctionToken optDefinitions RETURN action ';' endInlineFunctionToken
                        {
                            Owned<ITypeInfo> retType = $1.getType();
                            $$.setExpr(parser->leaveLamdaExpression($5), $7);
                        }
    | WHEN '(' action ',' action sideEffectOptions ')'
                        {
                            OwnedHqlExpr options = $6.getExpr();
                            if (options)
                                $$.setExpr(createValueF(no_executewhen, makeVoidType(), $3.getExpr(), $5.getExpr(), options.getClear(), NULL), $1);
                            else
                                $$.setExpr(createCompound($5.getExpr(), $3.getExpr()), $1);
                        }
    ;

assertActions
    : assertAction
    | assertActions ',' assertAction
                        {
                            $$.setExpr(createComma($1.getExpr(), $3.getExpr()));
                            $$.setPosition($1);
                        }
    ;

assertAction
    : TOK_ASSERT '(' expression assertFlags ')'
                        {
                            parser->normalizeExpression($3, type_boolean, false);
                            $$.setExpr(parser->createAssert($3, NULL, $4));
                            $$.setPosition($1);
                        }
    | TOK_ASSERT '(' expression ',' expression assertFlags ')'
                        {
                            parser->normalizeExpression($3, type_boolean, false);
                            parser->normalizeExpression($5);
                            $$.setExpr(parser->createAssert($3, &$5, $6));
                            $$.setPosition($1);
                        }
    ;

assertFlags
    :                   { $$.setNullExpr(); }
    | ',' FAIL
                        {
                            $$.setExpr(createAttribute(failAtom));
                            $$.setPosition($2);
                        }
    | ',' TOK_CONST
                        {
                            $$.setExpr(createAttribute(constAtom));
                            $$.setPosition($2);
                        }
    ;

optBuildFlags
    :                   { $$.setNullExpr(); $$.clearPosition(); }
    | ',' buildFlags    { $$.setExpr($2.getExpr(), $1); }
    ;

buildFlags
    : buildFlag
    | buildFlag ',' buildFlags
                        {
                            $$.setExpr(createComma($1.getExpr(), $3.getExpr()));
                            $$.setPosition($1);
                        }
    ;

buildFlag
    : PARALLEL          { $$.setExpr(createAttribute(parallelAtom)); $$.setPosition($1); }
    | OVERWRITE         { $$.setExpr(createAttribute(overwriteAtom)); $$.setPosition($1); }
    | NOOVERWRITE       { $$.setExpr(createAttribute(noOverwriteAtom)); $$.setPosition($1); }
    | BACKUP            { $$.setExpr(createAttribute(backupAtom)); $$.setPosition($1); }
    | NAMED '(' constExpression ')'
                        {
                            parser->normalizeStoredNameExpression($3);
                            $$.setExpr(createExprAttribute(namedAtom, $3.getExpr()));
                            $$.setPosition($1);
                        }
    | DATASET '(' dataSet ')'
                        {
                            parser->reportWarning(CategoryDeprecated, ERR_DEPRECATED, $1.pos, "DATASET attribute on index is deprecated, and has no effect");
                            OwnedHqlExpr ds = $3.getExpr();
                            if (ds->getOperator() != no_table)
                                parser->reportError(ERR_EXPECTED_DATASET, $3, "Expected parameter to be a DATASET definition");
                            $$.setExpr(NULL, $1);
                        }
    | commonAttribute
    | SORTED            {   $$.setExpr(createAttribute(sortedAtom)); $$.setPosition($1); }
    | dataSet
                        {
                            //Ugly, but special case DISTRIBUTE '(' dataSet ')'
                            OwnedHqlExpr ds = $1.getExpr();
                            if (ds->getOperator() == no_distribute)
                            {
                                IHqlExpression * arg = ds->queryChild(0);
                                if (!isKey(arg))
                                    parser->reportError(ERR_EXPECTED_INDEX,$1,"Expected an index");
                                ds.setown(createValue(no_distributer, makeNullType(), LINK(arg)));
                            }
                            $$.setExpr(ds.getClear(), $1);
                        }
    | MERGE             {
                            $$.setExpr(createAttribute(mergeAtom));
                            $$.setPosition($1);
                        }
    | skewAttribute
    | THRESHOLD '(' expression ')'
                        {
                            parser->normalizeExpression($3, type_numeric, true);
                            $$.setExpr(createExprAttribute(thresholdAtom, $3.getExpr()));
                        }
    | FEW               {
                            $$.setExpr(createAttribute(fewAtom));
                            $$.setPosition($1);
                        }
    | PERSIST           {
                            $$.setExpr(createAttribute(persistAtom));
                            $$.setPosition($1);
                        }
    | UPDATE            {
                            $$.setExpr(createComma(createAttribute(updateAtom), createAttribute(overwriteAtom)));
                            $$.setPosition($1);
                        }
    | expireAttr
    | NOROOT            {
                            $$.setExpr(createComma(createAttribute(noRootAtom), createLocalAttribute()));
                            $$.setPosition($1);
                        }
    | SORT KEYED        {
                            $$.setExpr(createAttribute(sort_KeyedAtom));
                            $$.setPosition($1);
                        }
    | SORT ALL          {
                            $$.setExpr(createAttribute(sort_AllAtom));
                            $$.setPosition($1);
                        }
    | clusterAttr
    | WIDTH '(' expression ')'
                        {
                            parser->normalizeExpression($3, type_numeric, false);
                            $$.setExpr(createExprAttribute(widthAtom, $3.getExpr()), $1);
                        }
    | SET '(' expression ',' expression ')'
                        {
                            parser->normalizeExpression($3, type_stringorunicode, false);
                            parser->normalizeExpression($5);
                            $$.setExpr(createExprAttribute(setAtom, $3.getExpr(), $5.getExpr()));
                            $$.setPosition($1);
                        }
    | DISTRIBUTED       {
                            $$.setExpr(createComma(createAttribute(noRootAtom), createLocalAttribute()));
                            $$.setPosition($1);
                        }
    | DISTRIBUTED '(' expression ')'
                        {
                            parser->normalizeExpression($3, type_int, false);
                            $$.setExpr(createComma(createAttribute(noRootAtom), createExprAttribute(distributedAtom, $3.getExpr()), createLocalAttribute()));
                            $$.setPosition($1);
                        }
    | COMPRESSED '(' compressMode ')'
                        {
                            $$.setExpr(createExprAttribute(compressedAtom, $3.getExpr()));
                            $$.setPosition($1);
                        }
    | DEDUP
                        {
                            $$.setExpr(createAttribute(dedupAtom), $1);
                        }
    | FILEPOSITION optConstBoolArg
                        {
                            $$.setExpr(createExprAttribute(filepositionAtom, $2.getExpr()), $1);
                        }
    | MAXLENGTH
                        {
                            $$.setExpr(createExprAttribute(maxLengthAtom), $1);
                        }
    | MAXLENGTH '(' constExpression ')'
                        {
                            parser->normalizeExpression($3, type_numeric, false);
                            $$.setExpr(createExprAttribute(maxLengthAtom, $3.getExpr()), $1);
                        }
    | expression
    ;

localAttribute
    : LOCAL             {
                            $$.setExpr(createLocalAttribute());
                            $$.setPosition($1);
                        }
    | NOLOCAL           {
                            $$.setExpr(createAttribute(noLocalAtom));
                            $$.setPosition($1);
                        }
    ;

optCommonAttrs
    :                   { $$.setNullExpr(); }
    | ',' commonAttribute optCommonAttrs
                        {
                            $$.setExpr(createComma($2.getExpr(), $3.getExpr()));
                            $$.setPosition($3);
                        }
    ;

commonAttribute
    : localAttribute
    | hintAttribute
    ;

hintAttribute
    : HINT '(' hintList ')'
                        {
                            HqlExprArray args;
                            $3.unwindCommaList(args);
                            $$.setExpr(createExprAttribute(hintAtom, args));
                            $$.setPosition($1);
                        }
    ;

hintList
    : hintItem
    | hintList ',' hintItem
                        {
                            $$.setExpr(createComma($1.getExpr(), $3.getExpr()), $1);
                        }
    ;

hintItem
    : hintName
                        {
                            $$.setExpr(createExprAttribute(lower($1.getId())));
                            $$.setPosition($1);
                        }
    | hintName '(' beginList hintExprList ')'
                        {
                            HqlExprArray args;
                            parser->endList(args);
                            $$.setExpr(createExprAttribute(lower($1.getId()), args));
                            $$.setPosition($1);
                        }
    ;

hintName
    : UNKNOWN_ID
    | OUTPUT            {   $$.setId(outputId); }
    ;

hintExprList
    : hintExpr
                        {
                            parser->addListElement($1.getExpr());
                            $$.clear();
                        }
    | hintExprList ',' hintExpr
                        {
                            parser->addListElement($3.getExpr());
                            $$.clear();
                        }
    ;

hintExpr
    : expression
                        {
                            parser->normalizeExpression($1);
                            $$.inherit($1);
                        }
    | expression DOTDOT expression
                        {
                            parser->normalizeExpression($1);
                            parser->normalizeExpression($3);
                            $$.setExpr(createValue(no_range, makeNullType(), $1.getExpr(), $3.getExpr()));
                        }
    | DOTDOT expression
                        {
                            parser->normalizeExpression($2);
                            $$.setExpr(createValue(no_rangeto, makeNullType(), $2.getExpr()));
                        }
    | expression DOTDOT {
                            parser->normalizeExpression($1);
                            $$.setExpr(createValue(no_rangefrom, makeNullType(), $1.getExpr()));
                        }
    | UNKNOWN_ID
                        {
                            $$.setExpr(createAttribute(lower($1.getId())), $1);
                        }
    ;

expireAttr
    : EXPIRE            {
                            $$.setExpr(createAttribute(expireAtom));
                            $$.setPosition($1);
                        }
    | EXPIRE '(' expression ')'
                        {
                            parser->normalizeExpression($3, type_int, true);
                            $$.setExpr(createExprAttribute(expireAtom, $3.getExpr()));
                            $$.setPosition($1);
                        }
    ;

optDatasetFlags
    :                   { $$.setNullExpr(); }
    | ',' datasetFlags    { $$.inherit($2); }
    ;

datasetFlags
    : datasetFlag
    | datasetFlag ',' datasetFlags
                        { $$.setExpr(createComma($1.getExpr(), $3.getExpr()), $1); }
    ;

datasetFlag
    : DISTRIBUTED       {
                            $$.setExpr(createExprAttribute(distributedAtom));
                            $$.setPosition($1);
                        }
    | localAttribute
    ;

optIndexFlags
    :                   { $$.setNullExpr(); $$.clearPosition(); }
    | ',' indexFlags    { $$.setExpr($2.getExpr()); $$.setPosition($1); }
    ;

indexFlags
    : indexFlag
    | indexFlag ',' indexFlags
                        { $$.setExpr(createComma($1.getExpr(), $3.getExpr())); $$.setPosition($1); }
    ;

indexFlag
    : SORTED            {   $$.setExpr(createAttribute(sortedAtom)); $$.setPosition($1); }
    | STEPPED           {   $$.setExpr(createExprAttribute(steppedAtom)); $$.setPosition($1); }
    | STEPPED '(' expressionList ')'
                        {
                            HqlExprArray args;
                            $3.unwindCommaList(args);
                            $$.setExpr(createExprAttribute(steppedAtom, args));
                            $$.setPosition($1);
                        }
    | PRELOAD           {   $$.setExpr(createAttribute(preloadAtom)); $$.setPosition($1); }
    | OPT               {   $$.setExpr(createAttribute(optAtom)); $$.setPosition($1); }
    | SORT KEYED        {
                            $$.setExpr(createAttribute(sort_KeyedAtom));
                            $$.setPosition($1);
                        }
    | SORT ALL          {
                            $$.setExpr(createAttribute(sort_AllAtom));
                            $$.setPosition($1);
                        }
    | REMOTE            {
                            $$.setExpr(createExprAttribute(distributedAtom));
                            $$.setPosition($1);
                        }
    | DISTRIBUTED       {
                            $$.setExpr(createExprAttribute(distributedAtom));
                            $$.setPosition($1);
                        }
    | DISTRIBUTED '(' expression ')'
                        {
                            parser->normalizeExpression($3, type_numeric, false);
                            $$.setExpr(createExprAttribute(distributedAtom, $3.getExpr()));
                            $$.setPosition($1);
                        }
    | TOK_FIXED
                        {
                            $$.setExpr(createAttribute(fixedAtom));
                            $$.setPosition($1);
                        }
    | COMPRESSED '(' compressMode ')'
                        {
                            $$.setExpr(createExprAttribute(compressedAtom, $3.getExpr()));
                            $$.setPosition($1);
                        }
    | DYNAMIC
                        {
                            $$.setExpr(createAttribute(dynamicAtom));
                            $$.setPosition($1);
                        }
    | UNORDERED         {   $$.setExpr(createAttribute(unorderedAtom)); $$.setPosition($1); }
    | FILEPOSITION optConstBoolArg
                        {
                            $$.setExpr(createExprAttribute(filepositionAtom, $2.getExpr()), $1);
                        }
    | MAXLENGTH
                        {
                            $$.setExpr(createExprAttribute(maxLengthAtom), $1);
                        }
    | MAXLENGTH '(' constExpression ')'
                        {
                            parser->normalizeExpression($3, type_numeric, false);
                            $$.setExpr(createExprAttribute(maxLengthAtom, $3.getExpr()), $1);
                        }
    | commonAttribute
    ;

compressMode
    : FIRST             {
                            $$.setExpr(createAttribute(firstAtom));
                            $$.setPosition($1);
                        }
    | LZW
                        {
                            $$.setExpr(createAttribute(lzwAtom));
                            $$.setPosition($1);
                        }
    | ROW
                        {
                            $$.setExpr(createAttribute(rowAtom));
                            $$.setPosition($1);
                        }
    ;

optOutputFlags
    :                   { $$.setNullExpr(); $$.clearPosition(); }
    | ',' outputFlags   { $$.setExpr($2.getExpr()); $$.setPosition($1); }
    ;

outputFlags
    : outputFlag
    | outputFlags ',' outputFlag
                        { $$.setExpr(createComma($1.getExpr(), $3.getExpr())); $$.setPosition($1); }
    ;

outputFlag
    : EXTEND            {
                            $$.setExpr(createAttribute(extendAtom));
                        }
    | CSV               {
                            $$.setExpr(createAttribute(csvAtom));
                            $$.setPosition($1);
                        }
    | CSV '(' csvOptions ')'
                        {
                            HqlExprArray args;
                            $3.unwindCommaList(args);
                            $$.setExpr(createExprAttribute(csvAtom, args));
                            $$.setPosition($1);
                        }
    | COMPRESSED        {
                            $$.setExpr(createAttribute(compressedAtom));
                            $$.setPosition($1);
                        }
    | __COMPRESSED__    {
                            $$.setExpr(createAttribute(__compressed__Atom));
                            $$.setPosition($1);
                        }
    | __GROUPED__       {
                            $$.setExpr(createAttribute(groupedAtom));
                            $$.setPosition($1);
                        }
    | OVERWRITE         {
                            $$.setExpr(createAttribute(overwriteAtom));
                            $$.setPosition($1);
                        }
    | NOOVERWRITE       {
                            $$.setExpr(createAttribute(noOverwriteAtom));
                            $$.setPosition($1);
                        }
    | BACKUP            {
                            $$.setExpr(createAttribute(backupAtom));
                            $$.setPosition($1);
                        }
    | PERSIST           {
                            $$.setExpr(createAttribute(persistAtom));
                            $$.setPosition($1);
                        }
    | XML_TOKEN         {
                            $$.setExpr(createAttribute(xmlAtom));
                            $$.setPosition($1);
                        }
    | XML_TOKEN '(' xmlOptions ')'
                        {
                            HqlExprArray args;
                            $3.unwindCommaList(args);
                            $$.setExpr(createExprAttribute(xmlAtom, args), $1);
                        }
    | JSON_TOKEN        {
                            $$.setExpr(createAttribute(jsonAtom));
                            $$.setPosition($1);
                        }
    | JSON_TOKEN '(' xmlOptions ')' //exact same options as XML for now
                        {
                            HqlExprArray args;
                            $3.unwindCommaList(args);
                            $$.setExpr(createExprAttribute(jsonAtom, args), $1);
                        }
    | UPDATE            {
                            $$.setExpr(createComma(createAttribute(updateAtom), createAttribute(overwriteAtom)), $1);
                        }
    | expireAttr
    | ENCRYPT '(' expression ')'
                        {
                            parser->normalizeExpression($3, type_data, false);
                            $$.setExpr(createExprAttribute(encryptAtom, $3.getExpr()));
                            $$.setPosition($1);
                        }
    | clusterAttr
    | WIDTH '(' expression ')'
                        {
                            parser->normalizeExpression($3, type_numeric, false);
                            $$.setExpr(createExprAttribute(widthAtom, $3.getExpr()), $1);
                        }
    | commonAttribute
    | __OWNED__
                        {
                            //$$.setExpr(createAttribute(jobOwnedAtom), $1);
                            $$.setExpr(createAttribute(ownedAtom), $1);
                        }
    | thorFilenameOrList
                        {
                            //Careful with dynamic...
                            if (($1.getOperator() != no_comma) && !$1.queryExpr()->isAttribute())
                                parser->normalizeExpression($1, type_string, false);
                            $$.inherit($1);
                        }
    | FIRST '(' constExpression ')'
                        {
                            parser->normalizeExpression($3, type_int, false);
                            $$.setExpr(createExprAttribute(firstAtom, $3.getExpr()));
                            $$.setPosition($1);
                        }
    | THOR              {
                            $$.setExpr(createAttribute(diskAtom));
                            $$.setPosition($1);
                        }
    | NAMED '(' constExpression ')'
                        {
                            parser->normalizeStoredNameExpression($3);
                            $$.setExpr(createExprAttribute(namedAtom, $3.getExpr()));
                            $$.setPosition($1);
                        }
    | STORED            {
                            $$.setExpr(createAttribute(workunitAtom));              // need a better keyword, but WORKUNIT is no good
                            $$.setPosition($1);
                        }
    | NOXPATH           {
                            $$.setExpr(createAttribute(noXpathAtom));
                            $$.setPosition($1);
                        }
    ;

soapFlags
    : soapFlag
    | soapFlags ',' soapFlag
                        { $$.setExpr(createComma($1.getExpr(), $3.getExpr())); $$.setPosition($1); }
    ;

soapFlag
    : HEADING '(' expression optCommaExpression ')'
                        {
                            parser->normalizeExpression($3, type_string, false);
                            if ($4.queryExpr())
                                parser->normalizeExpression($4, type_string, false);
                            $$.setExpr(createExprAttribute(headingAtom, $3.getExpr(), $4.getExpr()));
                            $$.setPosition($1);
                        }
    | SEPARATOR '(' expression ')'
                        {
                            parser->normalizeExpression($3, type_string, true);
                            $$.setExpr(createExprAttribute(separatorAtom, $3.getExpr()));
                            $$.setPosition($1);
                        }
    | XPATH '(' expression ')'
                        {
                            //MORE: Really type_utf8 - and in lots of other places!
                            parser->normalizeExpression($3, type_string, false);
                            parser->validateXPath($3);
                            $$.setExpr(createExprAttribute(xpathAtom, $3.getExpr()));
                            $$.setPosition($1);
                        }
    | GROUP             {
                            $$.setExpr(createAttribute(groupAtom));
                            $$.setPosition($1);
                        }
    | MERGE '(' expression ')'
                        {
                            parser->normalizeExpression($3, type_int, false);
                            $$.setExpr(createExprAttribute(mergeAtom, $3.getExpr()));
                            $$.setPosition($1);
                        }
    | PARALLEL '(' expression ')'
                        {
                            parser->normalizeExpression($3, type_int, false);
                            $$.setExpr(createExprAttribute(parallelAtom, $3.getExpr()));
                            $$.setPosition($1);
                        }
    | RETRY '(' expression ')'
                        {
                            parser->normalizeExpression($3, type_int, false);
                            $$.setExpr(createExprAttribute(retryAtom, $3.getExpr()));
                            $$.setPosition($1);
                        }
    | TIMEOUT '(' expression ')'
                        {
                            parser->normalizeExpression($3, type_real, false);
                            $$.setExpr(createExprAttribute(timeoutAtom, $3.getExpr()));
                            $$.setPosition($1);
                        }
    | TIMELIMIT '(' expression ')'
                        {
                            parser->normalizeExpression($3, type_real, false);
                            $$.setExpr(createExprAttribute(timeLimitAtom, $3.getExpr()));
                            $$.setPosition($1);
                        }
    | onFailAction
    | TOK_LOG
                        {
                            $$.setExpr(createAttribute(logAtom));
                            $$.setPosition($1);
                        }
    | TRIM              {
                            $$.setExpr(createAttribute(trimAtom));
                            $$.setPosition($1);
                        }
    | SOAPACTION '(' expression ')'
                        {
                            parser->normalizeExpression($3, type_string, false);
                            $$.setExpr(createExprAttribute(soapActionAtom, $3.getExpr()));
                            $$.setPosition($1);
                        }
    | HTTPHEADER '(' expression optCommaExpression ')'
                        {
                            parser->normalizeExpression($3, type_string, false);
                            if ($4.queryExpr())
                                parser->normalizeExpression($4, type_string, false);
                            $$.setExpr(createExprAttribute(httpHeaderAtom, $3.getExpr(), $4.getExpr()));
                            $$.setPosition($1);
                        }

    | PROXYADDRESS '(' expression optCommaExpression ')'
                        {
                            parser->normalizeExpression($3, type_string, false);
                            if ($4.queryExpr())
                                parser->normalizeExpression($4, type_string, false);
                            $$.setExpr(createExprAttribute(proxyAddressAtom, $3.getExpr(), $4.getExpr()));
                            $$.setPosition($1);
                        }

    | LITERAL           
                        {
                            $$.setExpr(createAttribute(literalAtom));
                            $$.setPosition($1);
                        }

    | ENCODING          
                        {
                            $$.setExpr(createAttribute(encodingAtom));
                            $$.setPosition($1);
                        }

    | NAMESPACE '(' expression ')'
                        {
                            parser->normalizeExpression($3, type_string, false);
                            $$.setExpr(createExprAttribute(namespaceAtom, $3.getExpr()));
                            $$.setPosition($1);
                        }
    | NAMESPACE '(' expression ',' expression ')'
                        {
                            parser->normalizeExpression($3, type_string, false);
                            parser->normalizeExpression($5, type_string, false);
                            $$.setExpr(createExprAttribute(namespaceAtom, $3.getExpr(), $5.getExpr()));
                            $$.setPosition($1);
                        }
    | RESPONSE '(' NOTRIM ')'
                        {
                            $$.setExpr(createExprAttribute(responseAtom, createAttribute(noTrimAtom)), $1);
                        }
    | commonAttribute
    | TOK_LOG '(' MIN ')'
                        {
                            $$.setExpr(createExprAttribute(logAtom, createAttribute(minAtom)), $1);
                        }
    | TOK_LOG '(' expression ')'
                        {
                            parser->normalizeExpression($3, type_string, false);
                            $$.setExpr(createExprAttribute(logAtom, $3.getExpr()), $1);
                        }
    ;

onFailAction
    : ONFAIL '(' SKIP ')'
                        {
                            $$.setExpr(createExprAttribute(onFailAtom, createValue(no_skip, makeVoidType())));
                            $$.setPosition($1);
                        }
    | ONFAIL '(' transform ')'
                        {
                            $$.setExpr(createExprAttribute(onFailAtom, $3.getExpr()));
                            $$.setPosition($1);
                        }
    ;

clusterAttr
    : CLUSTER '(' stringExpressionList ')'
                        {
                            HqlExprArray args;
                            $3.unwindCommaList(args);
                            $$.setExpr(createExprAttribute(clusterAtom, args));
                            $$.setPosition($1);
                        }
    ;

stringExpressionList
    : expression
                        {
                            parser->normalizeExpression($1, type_string, false);
                            $$.inherit($1);
                        }
    | stringExpressionList ',' expression
                        {
                            parser->normalizeExpression($3, type_string, false);
                            $$.setExpr(createComma($1.getExpr(), $3.getExpr()));
                            $$.setPosition($1);
                        }
    ;

expressionList
    : expression
                        {
                            parser->normalizeExpression($1);
                            $$.inherit($1);
                        }
    | expressionList ',' expression
                        {
                            parser->normalizeExpression($3);
                            $$.setExpr(createComma($1.getExpr(), $3.getExpr()));
                            $$.setPosition($1);
                        }
    ;

optOutputWuFlags
    :                   { $$.setNullExpr(); $$.clearPosition(); }
    | ',' outputWuFlags {
                            $$.setExpr($2.getExpr());
                            $$.setPosition($1);
                        }
    ;

outputWuFlags
    : outputWuFlag
    | outputWuFlags ',' outputWuFlag
                        {
                            $$.setExpr(createComma($1.getExpr(), $3.getExpr()));
                            $$.setPosition($1);
                        }
    ;

outputWuFlag
    : ALL               {
                            $$.setExpr(createAttribute(allAtom));
                            $$.setPosition($1);
                        }
    | XMLNS '(' expression ',' expression ')'
                        {
                            parser->normalizeExpression($3, type_stringorunicode, true);
                            parser->normalizeExpression($5, type_stringorunicode, true);
                            $$.setExpr(createExprAttribute(xmlnsAtom, $3.getExpr(), $5.getExpr()));
                            $$.setPosition($1);
                        }
    | FIRST '(' constExpression ')'
                        {
                            parser->normalizeExpression($3, type_int, true);
                            $$.setExpr(createAttribute(firstAtom, $3.getExpr()));
                            $$.setPosition($1);
                        }
    | THOR              {
                            $$.setExpr(createAttribute(diskAtom));
                            $$.setPosition($1);
                        }
    | NAMED '(' constExpression ')'
                        {
                            parser->normalizeStoredNameExpression($3);
                            $$.setExpr(createExprAttribute(namedAtom, $3.getExpr()));
                            $$.setPosition($1);
                        }
    | STORED            {
                            $$.setExpr(createAttribute(workunitAtom));              // need a better keyword, but WORKUNIT is no good
                            $$.setPosition($1);
                        }
    | EXTEND            {
                            $$.setExpr(createAttribute(extendAtom));
                            $$.setPosition($1);
                        }
    | OVERWRITE         {
                            $$.setExpr(createAttribute(overwriteAtom));
                            $$.setPosition($1);
                        }
    | NOOVERWRITE       {
                            $$.setExpr(createAttribute(noOverwriteAtom));
                            $$.setPosition($1);
                        }
    | UPDATE            {
                            $$.setExpr(createComma(createAttribute(updateAtom), createAttribute(overwriteAtom)));
                            $$.setPosition($1);
                        }
    | NOXPATH           {
                            $$.setExpr(createAttribute(noXpathAtom));
                            $$.setPosition($1);
                        }
    | commonAttribute
    | MAXSIZE '(' constExpression ')'
                        {
                            parser->normalizeExpression($3, type_int, false);
                            $$.setExpr(createExprAttribute(maxSizeAtom, $3.getExpr()));
                            $$.setPosition($1);
                        }
    ;

optCommaTrim
    :                   {   $$.setExpr(NULL); }
    | ',' TRIM          {
                            $$.setExpr(createAttribute(trimAtom), $1);
                        }
    ;
    
applyActions
    : action
    | applyActions',' applyOption
                        {
                            $$.setExpr(createComma($1.getExpr(), $3.getExpr()));
                            $$.setPosition($1);
                        }
    ;

applyOption
    : BEFORE '(' beginList actionlist ')'
                        {
                            HqlExprArray actions;
                            parser->endList(actions);
                            $$.setExpr(createExprAttribute(beforeAtom, createActionList(actions)));
                            $$.setPosition($1);
                        }
    | AFTER '(' beginList actionlist ')'
                        {
                            HqlExprArray actions;
                            parser->endList(actions);
                            $$.setExpr(createExprAttribute(afterAtom, createActionList(actions)));
                            $$.setPosition($1);
                        }
    | action
    ;

keyDiffFlags
    :                   {   $$.setNullExpr(); $$.clearPosition(); }
    | keyDiffFlags ',' keyDiffFlag
                        {
                            $$.setExpr(createComma($1.getExpr(), $3.getExpr()));
                            $$.setPosition($1);
                        }
    ;

keyDiffFlag
    : OVERWRITE         {
                            $$.setExpr(createAttribute(overwriteAtom));
                            $$.setPosition($1);
                        }
    | NOOVERWRITE       {
                            $$.setExpr(createAttribute(noOverwriteAtom));
                            $$.setPosition($1);
                        }
    | expireAttr
    | commonAttribute
    ;

optRecordDef
    : recordDef
    |                   {
                            $$.setExpr(createValue(no_null));
                            $$.clearPosition();
                        }
    ;

scopedActionId
    : ACTION_ID
    | moduleScopeDot ACTION_ID leaveScope
                        {
                            $1.release();
                            $$.setExpr($2.getExpr());
                            $$.setPosition($2);
                        }
    | actionFunction '('
                        {
                            parser->beginFunctionCall($1);
                        }
    actualParameters ')'
                        {
                            $$.setExpr(parser->bindParameters($1, $4.getExpr()));
                            $$.setPosition($1);
                        }
    | VALUE_MACRO action ENDMACRO
                        {
                            $$.setExpr($2.getExpr());
                            $$.setPosition($1);
                        }
    | moduleScopeDot VALUE_MACRO leaveScope action ENDMACRO
                        {
                            $1.release();
                            $$.setExpr($4.getExpr());
                            $$.setPosition($2);
                        }
    ;



eventObject
    : EVENT '(' expression ',' expression ')'
                        {
                            parser->normalizeExpression($3, type_string, false);
                            parser->normalizeExpression($5, type_string, false);
                            $$.setExpr(createValue(no_event, makeEventType(), $3.getExpr(), $5.getExpr()));
                            $$.setPosition($1);
                        }
    | CRON '(' expression ')'
                        {
                            parser->normalizeExpression($3, type_string, true);
                            $$.setExpr(createValue(no_event, makeEventType(), createConstant("CRON"), $3.getExpr()));
                            $$.setPosition($1);
                        }

    | EVENT_ID
    | moduleScopeDot EVENT_ID leaveScope
                        {
                            $1.release();
                            $$.setExpr($2.getExpr());
                            $$.setPosition($2);
                        }
    | eventFunction '('
                        {
                            parser->beginFunctionCall($1);
                        }
    actualParameters ')'
                        {
                            $$.setExpr(parser->bindParameters($1, $4.getExpr()));
                            $$.setPosition($1);
                        }
    | startCompoundExpression beginInlineFunctionToken optDefinitions RETURN eventObject ';' endInlineFunctionToken
                        {
                            Owned<ITypeInfo> retType = $1.getType();
                            $$.setExpr(parser->leaveLamdaExpression($5), $7);
                        }
    ;

event
    : eventObject
    | expression        {
                            parser->normalizeExpression($1, type_string, true);
                            $$.setExpr(createValue(no_event, makeEventType(), $1.getExpr()));
                            $$.setPosition($1);
                        }
    ;

parmdef
    : realparmdef       {   parser->setParametered(true); $$.clear(); }
    |                   {   parser->setParametered(false); $$.clear(); }
    ;

reqparmdef
    : realparmdef       {   parser->setParametered(true); $$.clear(); }
    ;

realparmdef
    : '(' params ')'
    | '(' ')'
    ;

params
    : param
    | params ',' param
    ;


// NB: the beginList is processed in the addParameter calls
//Also duplicating the line rather than having formalQualifiers have a null entry allows the canFollowCurrentState() function to work better
param
    : beginList paramDefinition
    | beginList formalQualifiers paramDefinition
    ;

formalQualifiers
    : formalQualifier
                        {
                            parser->addListElement($1.getExpr());
                            $$.clear();
                        }
    | formalQualifiers formalQualifier
                        {
                            parser->addListElement($2.getExpr());
                            $$.clear();
                        }
    ;

formalQualifier
    : TOK_CONST         {   $$.setExpr(createAttribute(constAtom), $1); }
    | TOK_ASSERT TOK_CONST  
                        {   $$.setExpr(createAttribute(assertConstAtom), $1); }
    | FIELD_REF         {
                            parser->setTemplateAttribute();
                            $$.setExpr(createAttribute(fieldAtom), $1);
                        }
    | FIELDS_REF        {
                            parser->setTemplateAttribute();
                            $$.setExpr(createAttribute(fieldsAtom), $1);
                        }
    | OPT               {   $$.setExpr(createAttribute(optAtom), $1); }
    | TOK_OUT           {   $$.setExpr(createAttribute(outAtom), $1); }
    ;


paramDefinition
    : setType knownOrUnknownId defvalue 
                        {   
                            $$.clear(); 
                            parser->addParameter($1, $2.getId(), $1.getType(), $3.getExpr());
                        }
    | paramType knownOrUnknownId defvalue   
                        {   
                            $$.clear();
                            parser->addParameter($1, $2.getId(), $1.getType(), $3.getExpr());
                        }
    | UNKNOWN_ID defvalue
                        {
                           $$.clear();
                           parser->addListElement(createAttribute(noTypeAtom));
                           parser->addParameter($1, $1.getId(), LINK(parser->defaultIntegralType), $2.getExpr());
                        }
    | anyFunction defvalue
                        {
                           //So that new action format doesn't break existing code.
                           $$.clear();
                           parser->addListElement(createAttribute(noTypeAtom));
                           OwnedHqlExpr func = $1.getExpr();
                           parser->addParameter($1, func->queryId(), LINK(parser->defaultIntegralType), $2.getExpr());
                        }
    | ANY DATASET knownOrUnknownId
                        {
                            $$.clear();
                            parser->addParameter($1, $3.getId(), makeTableType(makeRowType(queryNullRecord()->getType())), NULL);
                        }
    | ANY knownOrUnknownId defvalue
                        {
                            $$.clear();
                            parser->setTemplateAttribute();
                            parser->addParameter($1, $2.getId(), makeAnyType(), $3.getExpr());
                        }
    | paramType knownOrUnknownId nestedParmdef defFuncValue
                        {
                            $$.clear();
                            parser->addFunctionParameter($1, $2.getId(), $1.getType(), $4.getExpr());
                        }
    | setType knownOrUnknownId nestedParmdef defFuncValue
                        {
                            $$.clear();
                            parser->addFunctionParameter($1, $2.getId(), $1.getType(), $4.getExpr());
                        }
 // Use a function as a prototype for the argument type - kind of a substitute for a typedef
    | anyFunction UNKNOWN_ID defFuncValue
                        {
                            $$.clear();
                            parser->addFunctionProtoParameter($1, $2.getId(), $1.getExpr(), $3.getExpr());
                        }
    ;

nestedParmdef
    : beginNestedParamDef params ')'
    | beginNestedParamDef ')'
    ;

beginNestedParamDef
    : '('               {   
                            parser->enterScope(true); 
                            parser->setParametered(true); 
                            $$.clear(); 
                        }       // Enter type to save parameters
    ;

defvalue
    : EQ expression     
                        {
                            parser->normalizeExpression($2);
                            $$.inherit($2); 
                        }
    | EQ dataSet        {   $$.inherit($2); }
    | EQ dataRow        {   $$.inherit($2); }
    | EQ abstractModule
                        {
                            $$.inherit($2);
                        }
    |                   {   $$.setNullExpr(); }
    ;

defFuncValue
    :                   {   $$.setNullExpr(); }
    | EQ anyFunction    {   $$.setExpr($2.getExpr()); }
    ;

service
    : startService funcDefs END
                        {
                            $$.setExpr(parser->leaveService($3), $1);
                        }
    | startService error
                        {
                            $$.setExpr(parser->leaveService($2), $1);
                        }
    ;

startService
    : SERVICE attribs
                        {
                            parser->enterService($2);
                            $$.clear();
                        }
    ;

funcDefs
    : funcDef
    | funcDefs funcDef
    ;

funcDef
    : funcRetType knownOrUnknownId realparmdef attribs ';'
                        {
                            $$.clear($1);
                            IIdAtom * name = $2.getId();
                            OwnedITypeInfo type = $1.getType();
                            OwnedHqlExpr attrs = $4.getExpr();
                            parser->processServiceFunction($2, name, attrs, type);
                        }
    | knownOrUnknownId realparmdef attribs ';'
                        {
                            $$.clear($1);
                            IIdAtom * name = $1.getId();
                            OwnedITypeInfo type = makeVoidType();
                            OwnedHqlExpr attrs = $3.getExpr();
                            parser->processServiceFunction($1, name, attrs, type);
                        }
    ;

attribs
    :  ':' attriblist   {   $$.setExpr($2.getExpr()); }
    |                   {   $$.setNullExpr(); }
    ;

attriblist
    : attrib
    | attriblist ',' attrib        
                        {   $$.setExpr(createComma($1.getExpr(), $3.getExpr()));    }
    ;

attrib
    : knownOrUnknownId EQ UNKNOWN_ID        
                        {
                            parser->reportWarning(CategoryDeprecated, SeverityError, WRN_OBSOLETED_SYNTAX,$1.pos,"Syntax obsoleted; use alternative: id = '<string constant>'");
                            $$.setExpr(createAttribute(lower($1.getId()), createConstant(str($3.getId()))));
                        }
    | knownOrUnknownId EQ expr %prec reduceAttrib
                        {
                            //NOTE %prec is there to prevent a s/r error from the "SERVICE : attrib" production
                            $$.setExpr(createExprAttribute(lower($1.getId()), $3.getExpr()), $1);
                        }
    | knownOrUnknownId                  
                        {   $$.setExpr(createAttribute(lower($1.getId())));  }
    | knownOrUnknownId '(' expr ')'
                        {
                            $$.setExpr(createExprAttribute(lower($1.getId()), $3.getExpr()), $1);
                        }
    | knownOrUnknownId '(' expr ',' expr ')'
                        {
                            $$.setExpr(createExprAttribute(lower($1.getId()), $3.getExpr(), $5.getExpr()), $1);
                        }
    ;

funcRetType
    : TOK_CONST propType
                        {
                            $$.setType(makeConstantModifier($2.getType()));
                            $$.setPosition($1);
                        }
    | propType
    | setType
    | explicitDatasetType
    | explicitRowType
    | explicitDictionaryType
    | transformType
 // A plain record would be better, but that then causes a s/r error in knownOrUnknownId because scope
    | recordDef         {
                            OwnedHqlExpr expr = $1.getExpr();
//                          $$.setType(makeOriginalModifier(makeRowType(expr->getType()), LINK(expr)));
                            $$.setType(makeRowType(expr->getType()));
                            $$.setPosition($1);
                        }
    ;

payloadPart
    :  		            {
                            // NOTE - this reduction happens as soon as the GOESTO is seen,
                            // so it ensures that the following fields go into the payload record def
                            $$.setExpr(parser->endRecordDef());
                            parser->beginRecord();
                        }
      GOESTO fieldDefs optSemiComma
    ;

recordDef
    : startrecord fieldDefs optSemiComma endrecord
                        {
                            OwnedHqlExpr record = $4.getExpr();
                            parser->checkRecordIsValid($1, record);
                            $$.setExpr(record.getClear(), $1);
                        }

    | startrecord fieldDefs payloadPart endrecord
                        {
                            OwnedHqlExpr record = $3.getExpr();
                            OwnedHqlExpr payload = $4.getExpr();
                            parser->mergeDictionaryPayload(record, payload, $1);
                            $$.setExpr(record.getClear(), $1);
                        }

    | startrecord recordOptions fieldDefs optSemiComma endrecord
                        {
                            OwnedHqlExpr record = $5.getExpr();
                            parser->checkRecordIsValid($1, record);
                            $$.setExpr(record.getClear(), $1);
                        }

    | startrecord recordOptions fieldDefs payloadPart endrecord
                        {
                            OwnedHqlExpr record = $4.getExpr();
                            OwnedHqlExpr payload = $5.getExpr();
                            parser->mergeDictionaryPayload(record, payload, $1);
                            $$.setExpr(record.getClear(), $1);
                        }

    | startrecord recordBase optFieldDefs endrecord
                        {
                            OwnedHqlExpr record = $4.getExpr();
                            parser->checkRecordIsValid($1, record);
                            $$.setExpr(record.getClear(), $1);
                        }

    | startrecord recordBase optFieldDefs payloadPart endrecord
                        {
                            OwnedHqlExpr record = $4.getExpr();
                            OwnedHqlExpr payload = $5.getExpr();
                            parser->mergeDictionaryPayload(record, payload, $1);
                            $$.setExpr(record.getClear(), $1);
                        }

    | startrecord recordBase recordOptions optFieldDefs endrecord
                        {
                            OwnedHqlExpr record = $5.getExpr();
                            parser->checkRecordIsValid($1, record);
                            $$.setExpr(record.getClear(), $1);
                        }

    | startrecord recordBase recordOptions optFieldDefs payloadPart endrecord
                        {
                            OwnedHqlExpr record = $5.getExpr();
                            OwnedHqlExpr payload = $6.getExpr();
                            parser->mergeDictionaryPayload(record, payload, $1);
                            $$.setExpr(record.getClear(), $1);
                        }
    | simpleRecord
    | recordDef AND recordDef
                        {
                            OwnedHqlExpr left = $1.getExpr();
                            OwnedHqlExpr right = $3.getExpr();
                            $$.setExpr(parser->createRecordIntersection(left, right, $1));
                            $$.setPosition($1);
                            parser->checkRecordIsValid($1, $$.queryExpr());
                        }
    | recordDef OR recordDef
                        {
                            OwnedHqlExpr left = $1.getExpr();
                            OwnedHqlExpr right = $3.getExpr();
                            $$.setExpr(parser->createRecordUnion(left, right, $1));
                            $$.setPosition($1);
                        }
    | recordDef '-' recordDef
                        {
                            OwnedHqlExpr left = $1.getExpr();
                            OwnedHqlExpr right = $3.getExpr();
                            $$.setExpr(parser->createRecordDifference(left, right, $1));
                            $$.setPosition($1);
                            parser->checkRecordIsValid($1, $$.queryExpr());
                        }
    | recordDef '-' UNKNOWN_ID
                        {
                            OwnedHqlExpr left = $1.getExpr();
                            OwnedHqlExpr right = createId($3.getId());
                            $$.setExpr(parser->createRecordExcept(left, right, $1));
                            $$.setPosition($1);
                            parser->checkRecordIsValid($1, $$.queryExpr());
                        }
    | recordDef '-' '[' UnknownIdList ']'
                        {
                            OwnedHqlExpr left = $1.getExpr();
                            OwnedHqlExpr right = $4.getExpr();
                            $$.setExpr(parser->createRecordExcept(left, right, $1));
                            $$.setPosition($1);
                            parser->checkRecordIsValid($1, $$.queryExpr());
                        }
    | recordDef AND NOT recordDef
                        {
                            OwnedHqlExpr left = $1.getExpr();
                            OwnedHqlExpr right = $4.getExpr();
                            $$.setExpr(parser->createRecordDifference(left, right, $1));
                            $$.setPosition($1);
                            parser->checkRecordIsValid($1, $$.queryExpr());
                        }
    | recordDef AND NOT UNKNOWN_ID
                        {
                            OwnedHqlExpr left = $1.getExpr();
                            OwnedHqlExpr right = createId($4.getId());
                            $$.setExpr(parser->createRecordExcept(left, right, $1));
                            $$.setPosition($1);
                            parser->checkRecordIsValid($1, $$.queryExpr());
                        }
    | recordDef AND NOT '[' UnknownIdList ']'
                        {
                            OwnedHqlExpr left = $1.getExpr();
                            OwnedHqlExpr right = $5.getExpr();
                            $$.setExpr(parser->createRecordExcept(left, right, $1));
                            $$.setPosition($1);
                            parser->checkRecordIsValid($1, $$.queryExpr());
                        }
    | RECORDOF '(' goodObject ')'
                        {
                            OwnedHqlExpr ds = $3.getExpr();
                            IHqlExpression * record = queryOriginalRecord(ds);
                            if (!record)
                            {
                                parser->reportError(ERR_EXPECTED, $3, "The argument does not have a associated record");
                                record = queryNullRecord();
                            }
                            else if (ds->isFunction() && !record->isFullyBound())
                                parser->reportError(ERR_EXPECTED, $1, "RECORDOF(function-definition), result record depends on the function parameters");

                            $$.setExpr(LINK(record));
                            $$.setPosition($1);
                        }
    | VALUE_MACRO recordDef ENDMACRO
                        {
                            $$.setExpr($2.getExpr());
                            $$.setPosition($1);
                        }
    | moduleScopeDot VALUE_MACRO leaveScope recordDef ENDMACRO
                        {
                            $1.release();
                            $$.setExpr($4.getExpr());
                            $$.setPosition($2);
                        }
    | startCompoundExpression beginInlineFunctionToken optDefinitions RETURN recordDef ';' endInlineFunctionToken
                        {
                            Owned<ITypeInfo> retType = $1.getType();
                            $$.setExpr(parser->leaveLamdaExpression($5), $7);
                        }
    ;

dsRecordDef
    : recordDef         {
                            IHqlExpression *r = $1.getExpr();
                            parser->pushSelfScope(LINK(r));
                            $$.setExpr(r);
                            $$.setPosition($1);
                        }
    ;

dsEnd
    : ')'               {
                            parser->popSelfScope();
                            $$.clear();
                            $$.setPosition($1);
                        }
    ;


UnknownIdList
    : UNKNOWN_ID        {
                            $$.setExpr(createId($1.getId()));
                            $$.setPosition($1);
                        }
    | UnknownIdList ',' UNKNOWN_ID
                        {
                            $$.setExpr(createComma($1.getExpr(), createId($3.getId())));
                            $$.setPosition($1);
                        }
    ;

//This needs to be in a separate production because of the side-effects, but that prevents
startrecord
    : RECORD            {
                            parser->beginRecord();
                            $$.clear();
                        }
    | '{'               {
                            parser->beginRecord();
                            $$.clear();
                        }
    ;

recordBase
    : '(' recordDef ')'
                        {
                            //MORE: May want to add this after all attributes.
                            OwnedHqlExpr base = $2.getExpr();
                            parser->activeRecords.tos().addOperand(LINK(base->queryBody()));
                            $$.clear();
                        }
    ;

recordOptions
    : ',' recordOption
                        {
                            parser->addRecordOption($2);
                            $$.clear($1);
                        }

    | recordOptions ',' recordOption
                        {
                            parser->addRecordOption($3);
                            $$.clear($1);
                        }
    ;

recordOption
    : LOCALE '(' STRING_CONST ')'
                        {
                            OwnedHqlExpr lExpr = $3.getExpr();
                            StringBuffer lstr;
                            getStringValue(lstr, lExpr);
                            StringBuffer locale;
                            if(!getNormalizedLocaleName(lstr.length(), lstr.str(), locale))
                                parser->reportError(ERR_BAD_LOCALE, $3, "Bad locale name");
                            $$.setExpr(createExprAttribute(localeAtom, createConstant(locale.str())));
                            $$.setPosition($1);
                        }
    | MAXLENGTH '(' constExpression ')'
                        {
                            parser->normalizeExpression($3, type_numeric, false);
                            $$.setExpr(createExprAttribute(maxLengthAtom, $3.getExpr()));
                            $$.setPosition($1);
                        }
    | MAXSIZE '(' constExpression ')'
                        {
                            parser->normalizeExpression($3, type_numeric, false);
                            $$.setExpr(createExprAttribute(maxLengthAtom, $3.getExpr()));
                            $$.setPosition($1);
                        }
    | PACKED
                        {
                            $$.setExpr(createAttribute(packedAtom));
                            $$.setPosition($1);
                        }
    ;

endrecord
    : endOfRecordMarker
                        {
                            $$.setExpr(parser->endRecordDef());
                            parser->popLocale();
                            $$.setPosition($1);
                        }
    ;
    
endOfRecordMarker
    : END
    | '}'
    ;

abstractDataset
    : VIRTUAL DATASET '(' recordDef ')'
                        {
                            $$.setExpr($4.getExpr(), $1);
                        }
    | VIRTUAL DATASET   {
                            $$.setExpr(LINK(queryNullRecord()), $1);
                        }
    ;

optSemiComma
    : semiComma
    |
    ;

optFieldDefs
    :
    | fieldDefs
    | fieldDefs ';'
    | fieldDefs ','
    ;

fieldDefs
    : fieldDef
    | fieldDefs ';' fieldDef
    | fieldDefs ',' fieldDef
    ;

fieldDef
    : expression        {
                            parser->addFieldFromValue($1, $1);
                            $$.clear();
                        }
    | DATASET '(' dataSet ')' {
                            parser->addFieldFromValue($1, $3);
                            $$.clear();
                        }
    | VALUE_ID_REF      {
                            OwnedHqlExpr value = $1.getExpr();
                            IHqlExpression * field = queryFieldFromExpr(value);
                            if (field && (field->getOperator() == no_field))
                            {
                                //The following adds the field instead of creating a new field based on the old one.
                                //More efficient, but changes a few CRCs, so disabled for moment.
                                parser->checkFieldnameValid($1, field->queryId());
                                parser->addToActiveRecord(LINK(field));
                            }
                            else
                            {
                                IIdAtom * name = parser->createFieldNameFromExpr(value);
                                IHqlExpression * attrs = extractAttrsFromExpr(value);

                                parser->addField($1, name, value->getType(), NULL, attrs);
                            }
                            $$.clear();
                        }   
    | fieldSelectedFromRecord
                        {
                            OwnedHqlExpr value = $1.getExpr();
                            if (false)
                            {
                                //The following adds the field instead of creating a new field based on the old one.
                                //More efficient, but changes a few CRCs, so disabled for moment.
                                IHqlExpression * field = queryFieldFromSelect(value);
                                parser->checkFieldnameValid($1, field->queryId());
                                parser->addToActiveRecord(LINK(field));
                            }
                            else
                            {
                                IIdAtom * name = parser->createFieldNameFromExpr(value);
                                IHqlExpression * attrs = extractAttrsFromExpr(value);

                                parser->addField($1, name, value->getType(), NULL, attrs);
                            }
                            $$.clear();
                        }
    | UNKNOWN_ID optFieldAttrs ASSIGN expression
                        {
                            parser->normalizeExpression($4);
                            $$.clear($1);
                            // NOTE - you might expect the default value to be optional, to declare an integer....
                            // But that might be too error-prone
                            IHqlExpression *value = $4.getExpr();
                            IHqlExpression *attrs = $2.getExpr();
                            if (!attrs)
                                attrs = extractAttrsFromExpr(value);
                            parser->addField($1, $1.getId(), value->getType(), value, attrs);
                            $$.clear();
                        }
    | UNKNOWN_ID optFieldAttrs ASSIGN dataRow
                        {
                            IHqlExpression *value = $4.getExpr();
                            IHqlExpression *attrs = $2.getExpr();
                            if (!attrs)
                                attrs = extractAttrsFromExpr(value);
                            parser->addField($1, $1.getId(), makeRowType(LINK(value->queryRecordType())), value, attrs);
                            $$.clear();
                        }
    | typeDef knownOrUnknownId optFieldAttrs optDefaultValue
                        {
                            $$.clear($1);
                            IHqlExpression *value = $4.getExpr();

                            //Syntactic oddity.  A record means a row of that record type.
                            OwnedITypeInfo type = $1.getType();
                            if (type->getTypeCode() == type_record)
                                type.setown(makeRowType(type.getClear()));
                            parser->addField($2, $2.getId(), type.getClear(), value, $3.getExpr());
                        }
    | ANY knownOrUnknownId optFieldAttrs optDefaultValue
                        {
                            $$.clear($1);
                            IHqlExpression *value = $4.getExpr();

                            //Syntactic oddity.  A record means a row of that record type.
                            OwnedITypeInfo type = makeAnyType();
                            parser->addField($2, $2.getId(), type.getClear(), value, $3.getExpr());
                        }
    | typeDef knownOrUnknownId '[' expression ']' optFieldAttrs optDefaultValue
                        {
                            $$.clear($1);
                            parser->normalizeExpression($4, type_int, false);

                            OwnedHqlExpr attrs = $6.getExpr();
                            OwnedITypeInfo type = $1.getType();
                            IHqlExpression * record = queryNullRecord();
                            if (type->getTypeCode() != type_record)
                                parser->reportError(WRN_UNSUPPORTED_FEATURE, $3, "Only arrays of records are supported");
                            else
                            {
                                record = queryOriginalRecord(type);
                                attrs.setown(createComma(createLinkAttribute(countAtom, $4.getExpr()), attrs.getClear()));
                            }

                            IHqlExpression *value = $7.getExpr();
                            Owned<ITypeInfo> datasetType = makeTableType(makeRowType(createRecordType(record)));
                            parser->addDatasetField($2, $2.getId(), datasetType, value, attrs.getClear());
                        }
    | setType knownOrUnknownId optFieldAttrs optDefaultValue
                        {
                            $$.clear($1);
                            IHqlExpression *value = $4.getExpr();

                            ITypeInfo *type = $1.getType();
                            parser->addField($2, $2.getId(), type, value, $3.getExpr());
                        }
    | explicitDatasetType knownOrUnknownId optFieldAttrs optDefaultValue
                        {
                            $$.clear($1);
                            Owned<ITypeInfo> type = $1.getType();
                            parser->addDatasetField($2, $2.getId(), type, $4.getExpr(), $3.getExpr());
                        }
    | UNKNOWN_ID optFieldAttrs ASSIGN dataSet
                        {
                            IHqlExpression * value = $4.getExpr();
                            parser->addDatasetField($1, $1.getId(), NULL, value, $2.getExpr());
                            $$.clear();
                        }
    | explicitDictionaryType knownOrUnknownId optFieldAttrs optDefaultValue
                        {
                            $$.clear($1);
                            Owned<ITypeInfo> type = $1.getType();
                            parser->addDictionaryField($2, $2.getId(), type, $4.getExpr(), $3.getExpr());
                        }
    | UNKNOWN_ID optFieldAttrs ASSIGN dictionary
                        {
                            IHqlExpression * value = $4.getExpr();
                            parser->addDictionaryField($1, $1.getId(), value->queryType(), value, $2.getExpr());
                            $$.clear();
                        }
    | alienTypeInstance knownOrUnknownId optFieldAttrs optDefaultValue
                        {
                            $$.clear($1);
                            parser->addField($2, $2.getId(), $1.getType(), $4.getExpr(), $3.getExpr());
                        }
    | recordDef         {
                            //This distinguish between an inline record definition, and an out-of-line definition
                            //The inline shouldn't clone, but should just add the fields from the record.
                            OwnedHqlExpr e = $1.getExpr();
                            parser->addFields($1, e, NULL, hasNamedSymbol(e));
                            $$.clear();
                        }
    | dictionary        {
                            parser->addFieldFromValue($1, $1);
                            $$.clear();
                        }
    | dataSet               {
                            OwnedHqlExpr e = $1.getExpr();
                            parser->addFields($1, e->queryRecord(), e, true);
                            $$.clear();
                        }
    | dataRow           {
                            parser->addFieldFromValue($1, $1);
                            $$.clear();
                        }
    | ifblock
    | error             {
                            $$.clear();
                        }
    | expandedSortListByReference
                        {
                            OwnedHqlExpr list = $1.getExpr();
                            if (list && (list->queryType()->getTypeCode() != type_sortlist))
                            {
                                HqlExprArray items;
                                list->unwindList(items, no_comma);
                                ForEachItemIn(i, items)
                                {
                                    IHqlExpression * value = &items.item(i);
                                    IIdAtom * name = parser->createFieldNameFromExpr(value);
                                    IHqlExpression * attrs = extractAttrsFromExpr(value);
                                    parser->addField($1, name, value->getType(), LINK(value), attrs);
                                }
                            }
                            $$.clear();
                        }
    ;

optFieldAttrs
    :                   { $$.setNullExpr(); }
    | '{' '}'           { $$.setNullExpr(); }
    | '{' fieldAttrs '}'
                        { $$.setExpr($2.getExpr()); }
    ;

fieldAttrs
    : fieldAttr
    | fieldAttr ',' fieldAttrs      
                        { $$.setExpr(createComma($1.getExpr(), $3.getExpr())); }
    ;

fieldAttr
    : BLOB                  
                        { 
                            $$.setExpr(createAttribute(blobAtom)); 
                        }
    | CARDINALITY '(' expression ')'    
                        {
                            parser->normalizeExpression($3, type_numeric, false);
                            $$.setExpr(createExprAttribute(cardinalityAtom, $3.getExpr()));
                        }
    | CASE '(' expression ')'
                        { 
                            parser->normalizeExpression($3, type_set, false);
                            $$.setExpr(createExprAttribute(caseAtom, $3.getExpr()));
                        }
    | MAXCOUNT '(' expression ')' 
                        {
                            parser->normalizeExpression($3, type_int, true);
                            $$.setExpr(createExprAttribute(maxCountAtom, $3.getExpr()));
                        }
    | CHOOSEN '(' expression ')' 
                        {
                            parser->normalizeExpression($3, type_int, true);
                            $$.setExpr(createExprAttribute(choosenAtom, $3.getExpr()));
                        }
    | MAXLENGTH '(' expression ')' 
                        {
                            parser->normalizeExpression($3, type_int, true);
                            $$.setExpr(createExprAttribute(maxLengthAtom, $3.getExpr()));
                        }
    | MAXSIZE '(' expression ')' 
                        {
                            parser->normalizeExpression($3, type_int, true);
                            $$.setExpr(createExprAttribute(maxSizeAtom, $3.getExpr()));
                        }
    | NAMED '(' expression ')'  
                        {
                            parser->normalizeExpression($3, type_any, true);
                            $$.setExpr(createExprAttribute(namedAtom, $3.getExpr()));
                        }
    | RANGE '(' rangeExpr ')'           
                        {
                            $$.setExpr(createExprAttribute(rangeAtom, $3.getExpr()));
                        }
    | VIRTUAL '(' LOGICALFILENAME ')'       
                        {
                            $$.setExpr(createExprAttribute(virtualAtom, createAttribute(logicalFilenameAtom)));
                        }
    | VIRTUAL '(' FILEPOSITION ')'  
                        {
                            $$.setExpr(createExprAttribute(virtualAtom, createAttribute(filepositionAtom)));
                        }
    | VIRTUAL '(' LOCALFILEPOSITION ')' 
                        {
                            $$.setExpr(createExprAttribute(virtualAtom, createAttribute(localFilePositionAtom)));
                        }
    | VIRTUAL '(' SIZEOF ')'            
                        {
                            $$.setExpr(createExprAttribute(virtualAtom, createAttribute(sizeofAtom)));
                        }
    | XPATH '(' constExpression ')'
                        {
                            parser->normalizeExpression($3, type_string, false);
                            parser->validateXPath($3);
                            $$.setExpr(createExprAttribute(xpathAtom, $3.getExpr()));
                        }
    | XMLDEFAULT '(' constExpression ')'
                        {
                            $$.setExpr(createExprAttribute(xmlDefaultAtom, $3.getExpr()), $1);
                        }
    | DEFAULT '(' constExpression ')'
                        {
                            $$.setExpr(createExprAttribute(defaultAtom, $3.getExpr()), $1);
                        }
    | STRING_CONST
                        {
                            OwnedHqlExpr expr = $1.getExpr();
                            StringBuffer text;
                            getStringValue(text, expr);
                            $$.setExpr(createAttribute(createAtom(text.str())), $1);
                        }
    | STRING_CONST '(' expression ')'
                        {
                            parser->normalizeExpression($3);
                            OwnedHqlExpr expr = $1.getExpr();
                            StringBuffer text;
                            getStringValue(text, expr);
                            $$.setExpr(createExprAttribute(createAtom(text), $3.getExpr()), $1);
                        }
    | LINKCOUNTED     
                        {
                            $$.setExpr(getLinkCountedAttr());
                            $$.setPosition($1);
                        }
    | EMBEDDED      
                        {
                            $$.setExpr(getEmbeddedAttr());
                            $$.setPosition($1);
                        }
    ;


ifblock
    : beginIfBlock fieldDefs optSemiComma END
                        {
                            IHqlExpression * record = parser->endIfBlock();
                            OwnedHqlExpr expr = createValue(no_ifblock, makeNullType(), $1.getExpr(), record);
                            parser->addIfBlockToActive($1, expr);
                            $$.clear();
                        }
    | beginIfBlock optSemiComma END
                        {
                            parser->reportError(ERR_IFBLOCK_EMPTYDEF,$1,"Empty ifblock body");
                            ::Release(parser->endIfBlock());
                            $1.release();
                            $$.clear();
                        }
    ;

beginIfBlock
    : IFBLOCK '(' booleanExpr ')'
                        {
                            parser->beginIfBlock();
                            $$.setExpr($3.getExpr());
                            $$.setPosition($1);
                        }
    ;

qualifiedTypeId
    : ALIEN_ID
    | moduleScopeDot ALIEN_ID leaveScope
                        {
                            $1.release();
                            $$.setExpr($2.getExpr());
                            $$.setPosition($2);
                        }
    ;

enumTypeId
    : ENUM_ID
    | moduleScopeDot ENUM_ID leaveScope
                        {
                            $1.release();
                            $$.setExpr($2.getExpr());
                            $$.setPosition($2);
                        }
    ;
optParams
    : '(' actualParameters ')'  
                        {   $$.setExpr($2.getExpr());   }
    |                   {   $$.setNullExpr();   }
    ;

optDefaultValue
    : defaultValue
    |                       
                        {
                            $$.setNullExpr();
                        }
    ;
    
defaultValue
    : ASSIGN expression 
                        {
                            parser->normalizeExpression($2);
                            $$.inherit($2);
                        }
    | ASSIGN dataRow        
                        {
                            $$.inherit($2);
                        }
    | ASSIGN dataSet        
                        {
                            $$.inherit($2);
                        }
    | ASSIGN dictionary        
                        {
                            $$.inherit($2);
                        }
    | ASSIGN abstractModule
                        {
                            $$.inherit($2);
                        }
    ;

setType
    : SET                   
                        {
                            $$.setType(makeSetType(LINK(parser->defaultIntegralType)));
                        }
    | SET OF ANY            
                        {
                            $$.setType(makeSetType(NULL));
                        }
    | SET OF scalarType 
                        {
                            $$.setType(makeSetType($3.getType()));
                        }
    | SET OF explicitDatasetType    
                        {
                            $$.setType(makeSetType($3.getType()));
                        }
    | userTypedefSet
    ;

simpleType
    : SIMPLE_TYPE
    | UNSIGNED          {
                            $$.setType(makeIntType(8, false));
                            $$.setPosition($1);
                        }
    | PACKED simpleType 
                        {
                            ITypeInfo *type = $2.getType();
                            switch (type->getTypeCode())
                            {
                            case type_int:
                                $$.setType(makePackedIntType(type->getSize(), type->isSigned()));
                                type->Release();
                                break;
                            default:
                                parser->reportError(ERR_TYPEERR_INTDECIMAL, $2, "Integer type expected");
                                $$.setType(type);
                                break;
                            }
                            $$.setPosition($1);
                        }
    | UNSIGNED SIMPLE_TYPE  
                        {
                            ITypeInfo *type = $2.getType();
                            switch (type->getTypeCode())
                            {
                            case type_int:
                                $$.setType(makeIntType(type->getSize(), false));
                                type->Release();
                                break;
                            case type_swapint:
                                $$.setType(makeSwapIntType(type->getSize(), false));
                                type->Release();
                                break;
                            case type_packedint:
                                $$.setType(makePackedIntType(type->getSize(), false));
                                type->Release();
                                break;
                            case type_decimal:
                                $$.setType(makeDecimalType(type->getDigits(), type->getPrecision(), false));
                                type->Release();
                                break;
                            default:
                                parser->reportError(ERR_TYPEERR_INTDECIMAL, $2, "Integer or decimal type expected");
                                $$.setType(type);
                                break;
                            }
                            $$.setPosition($1);
                        }
    | BIG simpleType    {
                            ITypeInfo *type = $2.getType();
                            switch (type->getTypeCode())
                            {
                            case type_int:
#if __BYTE_ORDER == __LITTLE_ENDIAN
                                $$.setType(makeSwapIntType(type->getSize(), type->isSigned()));
                                type->Release();
#else
                                $$.setType(type);
#endif
                                break;
                            default:
                                parser->reportError(ERR_TYPEERR_INT, $2, "Integer type expected");
                                $$.setType(type);
                                break;
                            }
                            $$.setPosition($1);
                        }
    | LITTLE simpleType {
                            ITypeInfo *type = $2.getType();
                            switch (type->getTypeCode())
                            {
                            case type_int:
#if __BYTE_ORDER == __LITTLE_ENDIAN
                                $$.setType(type);
#else
                                $$.setType(makeSwapIntType(type->getSize(), type->isSigned()));
                                type->Release();
#endif
                                break;
                            default:
                                parser->reportError(ERR_TYPEERR_INT, $2, "Integer type expected");
                                $$.setType(type);
                                break;
                            }
                            $$.setPosition($1);
                        }
    | ASCII SIMPLE_TYPE {
                            Owned<ITypeInfo> type = $2.getType();
                            if (type->getTypeCode() != type_string)
                                parser->reportError(ERR_TYPEERR_STRING, $2, "String type expected");
                            $$.setType(makeStringType(type->getSize(), getCharset(asciiAtom), NULL));
                            $$.setPosition($1);
                        }
    | EBCDIC SIMPLE_TYPE
                        {
                            Owned<ITypeInfo> type = $2.getType();
                            if (type->getTypeCode() != type_string)
                                parser->reportError(ERR_TYPEERR_STRING, $2, "String type expected");
                            $$.setType(makeStringType(type->getSize(), getCharset(ebcdicAtom), NULL));
                            $$.setPosition($1);
                        }
    | TYPEOF '(' goodObject ')'
                        {
                            OwnedHqlExpr arg = $3.getExpr();
                            ITypeInfo *type = arg->getType();
                            if (!type)
                            {
                                parser->reportError(ERR_TYPEOF_ILLOPERAND, $3, "Illegal operand for TYPEOF");
                                type = LINK(parser->defaultIntegralType);
                            }
                            else
                            {
//                              type = preserveTypeQualifiers(type, arg);
                            }
                            $$.setType(type);
                            $$.setPosition($1);
                        }
    ;


userTypedefType
    : TYPE_ID           {
                            OwnedHqlExpr typedefExpr = $1.getExpr();
                            $$.setType(getTypedefType(typedefExpr));
                            $$.setPosition($1);
                        }

    | moduleScopeDot TYPE_ID leaveScope
                        {
                            $1.release();
                            OwnedHqlExpr typedefExpr = $2.getExpr();
                            $$.setType(getTypedefType(typedefExpr));
                            $$.setPosition($1);
                        }
    ;

userTypedefSet
    : SET_TYPE_ID       {
                            OwnedHqlExpr typedefExpr = $1.getExpr();
                            $$.setType(getTypedefType(typedefExpr));
                            $$.setPosition($1);
                        }

    | moduleScopeDot SET_TYPE_ID leaveScope
                        {
                            $1.release();
                            OwnedHqlExpr typedefExpr = $2.getExpr();
                            $$.setType(getTypedefType(typedefExpr));
                            $$.setPosition($1);
                        }
    ;

userTypedefPattern
    : PATTERN_TYPE_ID   {
                            OwnedHqlExpr typedefExpr = $1.getExpr();
                            $$.setType(getTypedefType(typedefExpr));
                            $$.setPosition($1);
                        }

    | moduleScopeDot PATTERN_TYPE_ID leaveScope
                        {
                            $1.release();
                            OwnedHqlExpr typedefExpr = $2.getExpr();
                            $$.setType(getTypedefType(typedefExpr));
                            $$.setPosition($1);
                        }
    ;


userTypedefDataset
    : DATASET_TYPE_ID   {
                            OwnedHqlExpr typedefExpr = $1.getExpr();
                            $$.setType(getTypedefType(typedefExpr));
                            $$.setPosition($1);
                        }

    | moduleScopeDot DATASET_TYPE_ID leaveScope
                        {
                            $1.release();
                            OwnedHqlExpr typedefExpr = $2.getExpr();
                            $$.setType(getTypedefType(typedefExpr));
                            $$.setPosition($1);
                        }
    ;


userTypedefDictionary
    : DICTIONARY_TYPE_ID   {
                            OwnedHqlExpr typedefExpr = $1.getExpr();
                            $$.setType(getTypedefType(typedefExpr));
                            $$.setPosition($1);
                        }

    | moduleScopeDot DICTIONARY_TYPE_ID leaveScope
                        {
                            $1.release();
                            OwnedHqlExpr typedefExpr = $2.getExpr();
                            $$.setType(getTypedefType(typedefExpr));
                            $$.setPosition($1);
                        }
    ;


childDatasetOptions
    :
                        { $$.setNullExpr(); }
    | childDatasetOptions ',' childDatasetOption
                        {
                            $$.setExpr(createComma($1.getExpr(), $3.getExpr()));
                        }
    ;

childDatasetOption
    : COUNT '(' expression ')'
                        {
                            parser->normalizeExpression($3);
                            $$.setExpr(createLinkAttribute(countAtom, $3.getExpr()));
                            $$.setPosition($1);
                        }
    | SIZEOF '(' expression ')'
                        {
                            parser->normalizeExpression($3);
                            $$.setExpr(createLinkAttribute(sizeofAtom, $3.getExpr()));
                            $$.setPosition($1);
                        }
    | LENGTH '(' expression ')'
                        {
                            parser->normalizeExpression($3);
                            $$.setExpr(createLinkAttribute(sizeofAtom, $3.getExpr()));
                            $$.setPosition($1);
                        }
    | CHOOSEN '(' expression ')'
                        {
                            //theoretically possible to cope with variables, but serialization of self cause problems in code generator
                            parser->normalizeExpression($3, type_any, true);
                            $$.setExpr(createLinkAttribute(choosenAtom, $3.getExpr()));
                            $$.setPosition($1);
                        }
    ;

typeDef
    : scalarType
    | recordDef         {
                            OwnedHqlExpr e1 = $1.getExpr();
                            $$.setType(createRecordType(e1));
                            $$.setPosition($1);
                        }
    ;

scalarType
    : simpleType
    | userTypedefType
    | enumTypeId
                        {
                            OwnedHqlExpr expr = $1.getExpr();
                            $$.setType(expr->getType());
                        }
    ;

query
    : expression optfailure 
                        {
                            parser->normalizeExpression($1);
                            IHqlExpression * expr = $1.getExpr();
                            IHqlExpression * failure = $2.getExpr();
                            if (failure)
                            {
                                if (expr->isDataset())
                                    $$.setExpr(createDataset(no_colon, expr, failure));
                                else
                                {
                                    //If a string value is stored, its type is a string of unknown length
                                    //(because a different length string might be stored...)
                                    Linked<ITypeInfo> type = expr->queryType();
#ifdef STORED_CAN_CHANGE_LENGTH
                                    switch (type->getTypeCode())
                                    {
                                    case type_varstring:
                                    case type_qstring:
                                    case type_string:
                                    case type_data:
                                    case type_unicode:
                                    case type_varunicode:
                                    case type_utf8:
                                        type.setown(getStretchedType(UNKNOWN_LENGTH, type));
                                        break;
                                    }
#endif
                                    $$.setExpr(createValueF(no_colon, type.getClear(), expr, failure, NULL));
                                }
                            }
                            else
                                $$.setExpr(expr);
                        }
    | dataSet optfailure 
                        {
                            IHqlExpression * expr = $1.getExpr();
                            OwnedHqlExpr failure = $2.getExpr();

                            HqlExprArray meta;
                            expr = attachWorkflowOwn(meta, expr, failure, NULL);
                            expr = parser->attachPendingWarnings(expr);
                            expr = parser->attachMetaAttributes(expr, meta);

                            IHqlExpression *record = createValue(no_null);
                            OwnedHqlExpr select = createDatasetF(no_selectfields, expr, record, NULL);
                            HqlExprArray args;
                            args.append(*select.getClear());
                            IHqlExpression * output = createValue(no_output, makeVoidType(), args);

                            $$.setExpr(output, $1);
                        }
    | dictionary optfailure
                        {
                            IHqlExpression * expr = $1.getExpr();
                            OwnedHqlExpr failure = $2.getExpr();

                            HqlExprArray meta;
                            expr = attachWorkflowOwn(meta, expr, failure, NULL);
                            expr = parser->attachPendingWarnings(expr);
                            expr = parser->attachMetaAttributes(expr, meta);

                            IHqlExpression * output = createValue(no_output, makeVoidType(), createDataset(no_datasetfromdictionary, expr));

                            $$.setExpr(output, $1);
                        }
    | dataRow optfailure
                        {
                            IHqlExpression * expr = $1.getExpr();
                            OwnedHqlExpr failure = $2.getExpr();

                            HqlExprArray meta;
                            expr = attachWorkflowOwn(meta, expr, failure, NULL);
                            expr = parser->attachPendingWarnings(expr);
                            expr = parser->attachMetaAttributes(expr, meta);

                            $$.setExpr(createValue(no_outputscalar, makeVoidType(), expr), $1);
                        }
    | action optfailure {
                            IHqlExpression * expr = $1.getExpr();
                            OwnedHqlExpr failure = $2.getExpr();

                            HqlExprArray meta;
                            expr = attachWorkflowOwn(meta, expr, failure, NULL);
                            expr = parser->attachPendingWarnings(expr);
                            expr = parser->attachMetaAttributes(expr, meta);

                            $$.setExpr(expr);
                        }
    | BUILD '(' scopeFunction ')'
                        {
                            OwnedHqlExpr expr = $3.getExpr();
                            assertex(expr->getOperator() == no_funcdef);
                            IHqlExpression * moduleExpr = expr->queryChild(0);
                            parser->checkExportedModule($3, moduleExpr);
                            IHqlScope * concrete = moduleExpr->queryScope()->queryConcreteScope();
                            if (!concrete)
                                parser->reportError(ERR_ABSTRACT_MODULE, $3, "Library modules cannot be abstract");
                            $$.setExpr(expr.getClear());
                            $$.setPosition($1);
                        }

    ;

optCommaExpression
    :                   {   $$.setNullExpr(); }
    | ',' expression    {   $$.inherit($2); }
    ;

optExpression
    :                   {   $$.setNullExpr(); }
    | expression
    ;

optConstBoolArg
    :                   {   $$.setNullExpr(); }
    | '(' expression ')'
                        {
                            parser->normalizeExpression($2, type_boolean, true);
                            $$.inherit($2);
                        }
    ;

booleanExpr
    : expression        {
                            parser->normalizeExpression($1, type_boolean, false);
                            $$.inherit($1);
                        }
    ;

constExpression
    : expression        {
                            parser->normalizeExpression($1, type_any, true);
                            $$.inherit($1);
                        }
    ;

expression
    : scalarExpression
    | startCompoundExpression beginInlineFunctionToken optDefinitions RETURN expression ';' endInlineFunctionToken
                        {
                            Owned<ITypeInfo> retType = $1.getType();
                            $$.setExpr(parser->leaveLamdaExpression($5), $7);
                        }
    ;

startCompoundExpression
    : '@'
                        {
                            //Currently this is only used inside functionmacro to define an inline function.
                            parser->enterScope(false);
                            parser->enterCompoundObject();
                            parser->beginDefineId(NULL, NULL);
                            $$.setType(NULL);
                        }
    ;
    
beginInlineFunctionToken
    : FUNCTION          // Will always work
    | '{'               // No so sure about this syntax - more concise, but not sure if in keeping with the rest of the language
    ;

endInlineFunctionToken
    : END
    | '}'               // see above
    ;

condList
    : booleanExpr       {
                            parser->normalizeExpression($1, type_boolean, false);
                            $$.inherit($1);
                        }
    | condList ',' booleanExpr
                        {
                            parser->normalizeExpression($3, type_boolean, false);
                            $$.setExpr(createComma($1.getExpr(), $3.getExpr()), $1);
                        }
    ;

chooseList
    : chooseItem
    | chooseList ',' chooseItem
                        {
                            $$.setExpr(createComma($1.getExpr(), $3.getExpr()));
                            $$.setPosition($1);
                        }
    ;

chooseItem
    : expression        {
                            parser->normalizeExpression($1);
                            parser->applyDefaultPromotions($1, true);
                            $$.inherit($1);
                        }
    ;

scalarExpression
    : compareExpr
    | NOT scalarExpression
                        {
                            ITypeInfo * type = $2.queryExprType();
                            if (type->getTypeCode() == type_boolean)
                            {
                                $$.setExpr(createBoolExpr(no_not, $2.getExpr()), $1);
                            }
                            else
                            {
                                parser->normalizeExpression($2, type_int, false);
                                IHqlExpression *e2 = $2.getExpr();
                                $$.setExpr(createValue(no_bnot, e2->getType(), e2), $1);
                            }
                        }
    | scalarExpression AND scalarExpression     
                        {
                            parser->normalizeExpression($1, type_boolean, false);
                            parser->normalizeExpression($3, type_boolean, false);
                            $$.setExpr(createBoolExpr(no_and, $1.getExpr(), $3.getExpr()));
                            $$.setPosition($1);
                        }
    | scalarExpression OR  scalarExpression     
                        {
                            parser->normalizeExpression($1, type_boolean, false);
                            parser->normalizeExpression($3, type_boolean, false);
                            $$.setExpr(createBoolExpr(no_or, $1.getExpr(), $3.getExpr()));
                            $$.setPosition($1);
                        }
/*
    | scalarExpression XOR scalarExpression     
                        {
                            parser->normalizeExpression($1, type_boolean, false);
                            parser->normalizeExpression($3, type_boolean, false);
                            $$.setExpr(createBoolExpr(no_ne, $1.getExpr(), $3.getExpr()));
                            $$.setPosition($1);
                        }
*/
    | WITHIN dataSet    {
                            OwnedHqlExpr ds = $2.getExpr();
                            $$.setExpr(createBoolExpr(no_within, ds.getClear()));
                            $$.setPosition($1);
                        }
    ;



heterogeneous_expr_list
    : heterogeneous_expr_list_open      
                        {   $$.setExpr($1.getExpr()->closeExpr()); }
    ;

heterogeneous_expr_list_open
    : expression        {
                            parser->normalizeExpression($1);
                            IHqlExpression *elem = $1.getExpr();
                            IHqlExpression *list = createOpenValue(no_comma, elem->getType());
                            list->addOperand(elem);
                            $$.setExpr(list);
                        }
    | heterogeneous_expr_list_open ',' expression
                        {
                            parser->normalizeExpression($3);
                            IHqlExpression *list = $1.getExpr();
                            list->addOperand($3.getExpr());
                            $$.setExpr(list);
                        }
    ;

compareExpr
    : expr compareOp expr
                        {
                            parser->normalizeExpression($1);
                            parser->normalizeExpression($3);
                            node_operator op = (node_operator)$2.getInt();
                            parser->promoteToSameCompareType($1, $3, op);
                            $$.setExpr(createBoolExpr(op, $1.getExpr(), $3.getExpr()), $1);
                        }
    | expr BETWEEN expr AND expr
                        {
                            parser->normalizeExpression($1);
                            parser->normalizeExpression($3);
                            parser->normalizeExpression($5);
                            parser->promoteToSameCompareType($1, $3, $5);
                            $$.setExpr(createBoolExpr(no_between, $1.getExpr(), $3.getExpr(), $5.getExpr()));
                        }
    | expr NOT BETWEEN expr AND expr
                        {
                            parser->normalizeExpression($1);
                            parser->normalizeExpression($4);
                            parser->normalizeExpression($6);
                            parser->promoteToSameCompareType($1, $4, $6);
                            $$.setExpr(createValue(no_not, makeBoolType(), createBoolExpr(no_between, $1.getExpr(), $4.getExpr(), $6.getExpr())));
                        }
    | expr NOT TOK_IN expr
                        {
                            parser->normalizeExpression($1);
                            parser->normalizeExpression($4);
                            parser->normalizeExpression($4, type_set, false);
                            IHqlExpression *set = $4.getExpr();
                            IHqlExpression *expr = $1.getExpr();
                            $$.setExpr(parser->createINExpression(no_notin, expr, set, $4));
                            $$.setPosition($3);
                        }
    | expr TOK_IN expr
                        {
                            parser->normalizeExpression($1);
                            parser->normalizeExpression($3);
                            parser->normalizeExpression($3, type_set, false);
                            IHqlExpression *set = $3.getExpr();
                            IHqlExpression *expr = $1.getExpr();
                            $$.setExpr(parser->createINExpression(no_in, expr, set, $3));
                            $$.setPosition($2);
                        }
    | expr NOT TOK_IN dictionary
                        {
                            parser->normalizeExpression($1);
                            parser->normalizeExpression($4);
                            parser->normalizeExpression($4, type_dictionary, false);
                            OwnedHqlExpr dict = $4.getExpr();
                            OwnedHqlExpr expr = $1.getExpr();
                            OwnedHqlExpr indict = createINDictExpr(*parser->errorHandler, $4.pos, expr, dict);
                            $$.setExpr(getInverse(indict));
                            $$.setPosition($3);
                        }
    | dataRow NOT TOK_IN dictionary
                        {
                            parser->normalizeExpression($1);
                            parser->normalizeExpression($4);
                            parser->normalizeExpression($4, type_dictionary, false);
                            OwnedHqlExpr dict = $4.getExpr();
                            OwnedHqlExpr row = $1.getExpr();
                            OwnedHqlExpr indict = createINDictRow(*parser->errorHandler, $4.pos, row, dict);
                            $$.setExpr(getInverse(indict));
                            $$.setPosition($3);
                        }
    | expr TOK_IN dictionary
                        {
                            parser->normalizeExpression($1);
                            parser->normalizeExpression($3);
                            parser->normalizeExpression($3, type_dictionary, false);
                            OwnedHqlExpr dict = $3.getExpr();
                            OwnedHqlExpr expr = $1.getExpr();
                            $$.setExpr(createINDictExpr(*parser->errorHandler, $3.pos, expr, dict));
                            $$.setPosition($2);
                        }
    | dataRow TOK_IN dictionary
                        {
                            parser->normalizeExpression($1);
                            parser->normalizeExpression($3);
                            parser->normalizeExpression($3, type_dictionary, false);
                            OwnedHqlExpr dict = $3.getExpr();
                            OwnedHqlExpr row = $1.getExpr();
                            $$.setExpr(createINDictRow(*parser->errorHandler, $3.pos, row, dict));
                            $$.setPosition($2);
                        }
    | dataSet EQ dataSet    
                        {
                            parser->checkSameType($1, $3); $$.setExpr(createBoolExpr(no_eq, $1.getExpr(), $3.getExpr()));
                        }
    | dataSet NE dataSet    
                        {
                            parser->checkSameType($1, $3); $$.setExpr(createBoolExpr(no_ne, $1.getExpr(), $3.getExpr()));
                        }
    | dataRow EQ dataRow  
                        {
                            parser->checkSameType($1, $3); $$.setExpr(createBoolExpr(no_eq, $1.getExpr(), $3.getExpr()));
                        }
    | dataRow NE dataRow  
                        {
                            parser->checkSameType($1, $3); $$.setExpr(createBoolExpr(no_ne, $1.getExpr(), $3.getExpr()));
                        }
    | expr
    ;


compareOp
    : EQ                { $$.setInt(no_eq); }
    | NE                { $$.setInt(no_ne); }
    | LE                { $$.setInt(no_le); }
    | GE                { $$.setInt(no_ge); }
    | LT                { $$.setInt(no_lt); }
    | GT                { $$.setInt(no_gt); }
    ;

expr
    : primexpr
    | expr '+' expr     {
                            parser->normalizeExpression($1);
                            parser->normalizeExpression($3);
                            if ($1.queryExpr()->isList() || $3.queryExpr()->isList())
                            {
                                ITypeInfo * type = parser->promoteToSameType($1, $3);
                                $$.setExpr(createValue(no_addsets, type, $1.getExpr(), $3.getExpr()), $1);
                            }
                            else if (isUnicodeType($1.queryExprType()) || isUnicodeType($3.queryExprType()))
                            {
                                parser->normalizeExpression($1, type_unicode, false);
                                parser->normalizeExpression($3, type_unicode, false);
                                IAtom * locale = parser->ensureCommonLocale($1, $3);
                                // cannot be certain of length of concatenated unicode string due to normalization
                                ITypeInfo * type;
                                if(($1.queryExprType()->getTypeCode() == type_utf8) && ($3.queryExprType()->getTypeCode() == type_utf8))
                                    type = makeUtf8Type(UNKNOWN_LENGTH, locale);
                                else
                                    type = makeUnicodeType(UNKNOWN_LENGTH, locale);
                                OwnedHqlExpr left = $1.getExpr();
                                OwnedHqlExpr right = $3.getExpr();
                                $$.setExpr(createValue(no_concat, type, ensureExprType(left, type), ensureExprType(right, type)), $1);
                            }
                            else if (isStringType($1.queryExprType()) || isStringType($3.queryExprType()))
                            {
                                parser->ensureString($1);
                                parser->ensureString($3);
                                ITypeInfo * t1 = $1.queryExprType();
                                ITypeInfo * t2 = $3.queryExprType();

                                unsigned l = t1->getStringLen();
                                unsigned r = t2->getStringLen();
                                unsigned size = UNKNOWN_LENGTH;
                                if ((l != UNKNOWN_LENGTH) && (r != UNKNOWN_LENGTH))
                                    size = l + r;
                                //MORE: case sensitive?
                                ICharsetInfo * charset = t1->queryCharset();
                                ICharsetInfo * otherCharset = t2->queryCharset();
                                if (queryDefaultTranslation(charset, otherCharset))
                                    parser->reportError(ERR_CHARSET_CONFLICT, $3, "Different character sets in concatenation");
                                ICollationInfo * collation = t1->queryCollation();  // MORE!!
                                ITypeInfo * type;
                                if ((t1->getTypeCode() == type_varstring) || (t2->getTypeCode() == type_varstring))
                                    type = makeVarStringType(size);
                                else if ((t1->getTypeCode() == type_string) || (t2->getTypeCode() == type_string))
                                    type = makeStringType(size, LINK(charset), LINK(collation));
                                else
                                    type = makeDataType(size);
                                $$.setExpr(createValue(no_concat, type, $1.getExpr(), $3.getExpr()), $1);
                            }
                            else
                            {
                                $$.setExpr(parser->createArithmeticOp(no_add, $1, $3), $1);
                            }
                        }
    | expr '-' expr       
                        {
                            $$.setExpr(parser->createArithmeticOp(no_sub, $1, $3), $1);
                        }
    | expr ORDER expr   
                        {
                            parser->normalizeExpression($1);
                            parser->normalizeExpression($3);
                            ::Release(parser->checkPromoteType($1, $3));
                            $$.setExpr(createValue(no_order, makeIntType(4, true), $1.getExpr(), $3.getExpr()));
                        }
    | expr '*' expr       
                        {
                            $$.setExpr(parser->createArithmeticOp(no_mul, $1, $3), $1);
                        }
    | expr '/' expr     
                        {
                            parser->normalizeExpression($1);
                            parser->normalizeExpression($3);
                            if (!isDecimalType($1.queryExprType()) && !isDecimalType($3.queryExprType()))
                                parser->ensureType($1, parser->defaultRealType);
                            $$.setExpr(parser->createArithmeticOp(no_div, $1, $3), $1);
                        }
    | expr '%' expr     {
                            parser->normalizeExpression($1, type_int, false);
                            parser->normalizeExpression($3, type_int, false);
                            parser->applyDefaultPromotions($1, true);
                            parser->applyDefaultPromotions($3, false);
                            ITypeInfo * type = parser->promoteToSameType($1, $3); // MORE _ should calculate at wider width then cast down to narrower?
                            $$.setExpr(createValue(no_modulus, type, $1.getExpr(), $3.getExpr()));
                        }
    | expr DIV expr     {
                            parser->normalizeExpression($1);
                            parser->normalizeExpression($3);
                            parser->applyDefaultPromotions($1, true);
                            parser->applyDefaultPromotions($3, false);
                            parser->normalizeExpression($1, type_int, false);
                            parser->normalizeExpression($3, type_int, false);
                            ITypeInfo * type = parser->promoteToSameType($1, $3);
                            $$.setExpr(createValue(no_div, type, $1.getExpr(), $3.getExpr()));
                        }
    | expr SHIFTL expr  {
                            parser->normalizeExpression($1);
                            parser->normalizeExpression($3);
                            parser->applyDefaultPromotions($1, true);
                            parser->normalizeExpression($1, type_int, false);
                            parser->normalizeExpression($3, type_int, false);
                            IHqlExpression * left = $1.getExpr();
                            $$.setExpr(createValue(no_lshift, left->getType(), left, $3.getExpr()));
                        }
    | expr SHIFTR expr  {
                            parser->normalizeExpression($1);
                            parser->normalizeExpression($3);
                            parser->applyDefaultPromotions($1, false);
                            parser->normalizeExpression($1, type_int, false);
                            parser->normalizeExpression($3, type_int, false);
                            IHqlExpression * left = $1.getExpr();
                            $$.setExpr(createValue(no_rshift, left->getType(), left, $3.getExpr()));
                        }
    | expr '&' expr     {
                            parser->normalizeExpression($1);
                            parser->normalizeExpression($3);
                            //MORE: We could could implement for decimal types.
                            if (!$1.queryExpr()->isBoolean() || !$3.queryExpr()->isBoolean())
                            {
                                parser->normalizeExpression($1, type_int, false);
                                parser->normalizeExpression($3, type_int, false);
                            }
                            ITypeInfo * lType = $1.queryExprType()->queryPromotedType();
                            ITypeInfo * rType = $3.queryExprType()->queryPromotedType();
                            ITypeInfo * type = getBandType(lType, rType);
                            parser->ensureType($1, type);
                            parser->ensureType($3, type);
                            $$.setExpr(createValue(no_band, type, $1.getExpr(), $3.getExpr()));
                        }
    | expr '|' expr     {
                            parser->normalizeExpression($1);
                            parser->normalizeExpression($3);
                            if (!$1.queryExpr()->isBoolean() || !$3.queryExpr()->isBoolean())
                            {
                                parser->normalizeExpression($1, type_int, false);
                                parser->normalizeExpression($3, type_int, false);
                            }
                            ITypeInfo * lType = $1.queryExprType()->queryPromotedType();
                            ITypeInfo * rType = $3.queryExprType()->queryPromotedType();
                            ITypeInfo * type = getBorType(lType, rType);
                            parser->ensureType($1, type);
                            parser->ensureType($3, type);
                            $$.setExpr(createValue(no_bor, type, $1.getExpr(), $3.getExpr()));
                        }
    | expr '^' expr     {
                            parser->normalizeExpression($1);
                            parser->normalizeExpression($3);
                            parser->normalizeExpression($1, type_int, false);
                            parser->normalizeExpression($3, type_int, false);
                            ITypeInfo * type = parser->promoteToSameType($1, $3);
                            $$.setExpr(createValue(no_bxor, type, $1.getExpr(), $3.getExpr()));
                        }
/*  | expr '[' rangeExpr ']'
                        {
                            parser->ensureTypeCanBeIndexed($1);
                            // MORE - result type is shorter if expressions are constant, same as input if not
                            ITypeInfo * subtype = parser->checkStringIndex($1, $3);
                            $$.setExpr(createValue(no_substring, subtype, $1.getExpr(), $3.getExpr()));
                        }
*/
    ;

rangeOrIndices
    : rangeExpr
    ;

rangeExpr
    : expression        {
                            IHqlExpression * expr = $1.queryExpr();
                            if (expr->isConstant() && !expr->queryType()->isInteger())
                                parser->reportWarning(CategoryMistake, WRN_INT_OR_RANGE_EXPECTED, $1.pos, "Floating point index used. Was an index range intended instead?");
                            parser->normalizeExpression($1, type_int, false);
                            parser->checkPositive($1);
                            $$.setExpr($1.getExpr());
                        }
    | expression DOTDOT expression
                        {
                            parser->normalizeExpression($1, type_int, false);
                            parser->normalizeExpression($3, type_int, false);
                            parser->checkPositive($1);
                            parser->checkPositive($3);
                            $$.setExpr(createValue(no_range, makeNullType(), $1.getExpr(), $3.getExpr()));
                        }
    | DOTDOT expression {
                            parser->normalizeExpression($2, type_int, false);
                            parser->checkPositive($2);
                            $$.setExpr(createValue(no_rangeto, makeNullType(), $2.getExpr()));
                        }
    | expression DOTDOT {
                            parser->normalizeExpression($1, type_int, false);
                            parser->checkPositive($1);
                            $$.setExpr(createValue(no_rangefrom, makeNullType(), $1.getExpr()));
                        }
    | expression DOTDOT '*' 
                        {
                            parser->normalizeExpression($1, type_int, false);
                            parser->checkPositive($1);
                            $$.setExpr(createValue(no_rangecommon, makeNullType(), $1.getExpr()));
                        }
    |                   {
                            parser->reportError(ERR_SUBSTR_EMPTYRANGE,yylval,"Empty range");

                            // recovering: assume [1..].
                            $$.setExpr(createValue(no_rangefrom, makeNullType(), createConstant(1)));
                        }
    ;

primexpr
    : primexpr1
    | '-' primexpr      {
                            parser->normalizeExpression($2);
                            if (parser->sortDepth == 0)
                            {
                                parser->applyDefaultPromotions($2, true);
                                parser->normalizeExpression($2, type_numeric, false);
                            }
                            IHqlExpression *e2 = $2.getExpr();
                            $$.setExpr(createValue(no_negate, e2->getType(), e2));
                        }
    | '+' primexpr      {
                            parser->normalizeExpression($2);
                            if (parser->sortDepth == 0)
                                parser->normalizeExpression($2, type_numeric, false);
                            $$.setExpr($2.getExpr());
                        }
    | BNOT primexpr     {
                            parser->normalizeExpression($2, type_int, false);
                            IHqlExpression *e2 = $2.getExpr();
                            $$.setExpr(createValue(no_bnot, e2->getType(), e2));
                        }
    | '(' scalarType ')' primexpr
                        {
                            parser->normalizeExpression($4, type_scalar, false);
                            Owned<ITypeInfo> type = $2.getType();
                            OwnedHqlExpr expr = $4.getExpr();
                            $$.setExpr(getCastExpr(expr, type));
                        }
    | '(' setType ')' primexpr
                        {
                            parser->normalizeExpression($4, type_set, false);
                            Owned<ITypeInfo> type = $2.getType();
                            OwnedHqlExpr expr = $4.getExpr();
                            $$.setExpr(createValue(no_cast, type.getClear(), expr.getClear()), $1);
                        }
    | transfer primexpr {
                            parser->normalizeExpression($2);
                            IHqlExpression *expr = $2.getExpr();
                            ITypeInfo *exprType = expr->queryType();
                            ITypeInfo *type = $1.getType();
                            if ((exprType->getSize() != UNKNOWN_LENGTH) && (exprType->getSize() < type->getSize()) && type->getSize() != UNKNOWN_LENGTH)
                                parser->reportError(ERR_TYPETRANS_LARGERTYPE, $1, "Type transfer: target type in is larger than source type");
                            $$.setExpr(createValue(no_typetransfer, type, expr));
                        }
    ;

primexpr1
    : atomicValue
    | primexpr1 '[' rangeOrIndices ']'
                        {
                            parser->normalizeExpression($1);
                            if ($1.queryExpr()->isList())
                            {
                                $$.setExpr(parser->createSetRange($1, $3));
                                $$.setPosition($1);
                            }
                            else
                            {
                                parser->ensureTypeCanBeIndexed($1);
                                // MORE - result type is shorter if expressions are constant, same as input if not
                                ITypeInfo * subtype = parser->checkStringIndex($1, $3);
                                $$.setExpr(createValue(no_substring, subtype, $1.getExpr(), $3.getExpr()), $1);
                            }
                        }
    | primexpr1 '[' NOBOUNDCHECK rangeOrIndices ']'
                        {
                            parser->normalizeExpression($1, type_set, false);
                            $$.setExpr(parser->createListIndex($1, $4, createAttribute(noBoundCheckAtom)));
                            $$.setPosition($1);
                        }

    | '(' expression ')'    
                        {   $$.inherit($2); }
    | COUNT '(' startTopFilter aggregateFlags ')' endTopFilter
                        {
                            $$.setExpr(createValue(no_count, LINK(parser->defaultIntegralType), $3.getExpr(), $4.getExpr()));
                        }
    | COUNT '(' GROUP optExtraFilter ')'
                        {
                            $$.setExpr(createValue(no_countgroup, LINK(parser->defaultIntegralType), $4.getExpr()));
                        }
    | COUNT '(' SORTLIST_ID ')'
                        {
                            OwnedHqlExpr list = $3.getExpr();
                            //list could either be a no_sortlist - in which case we want the number of elements,
                            //or a no_param, in which case it doesn't matter what we return
                            $$.setExpr(getSizetConstant(list->numChildren()), $1);
                        }
    | COUNT '(' dictionary ')'
                        {
                            $$.setExpr(createValue(no_countdict, LINK(parser->defaultIntegralType), $3.getExpr()));
                        }
    | CHOOSE '(' expression ',' chooseList ')'
                        {
                            parser->normalizeExpression($3, type_int, false);
                            HqlExprArray args;
                            $5.unwindCommaList(args);
                            ITypeInfo * retType = parser->promoteToSameType(args, $5, NULL, false);             // should be true
                            args.add(*$3.getExpr(),0);
                            $$.setExpr(createValue(no_choose, retType, args));
                        }
    | EXISTS '(' GROUP optExtraFilter ')'
                        {
                            $$.setExpr(createValue(no_existsgroup, makeBoolType(), $4.getExpr()));
                        }
    | EXISTS '(' dataSet aggregateFlags ')'
                        {
                            $$.setExpr(createBoolExpr(no_exists, $3.getExpr(), $4.getExpr()));
                            $$.setPosition($1);
                        }
    | EXISTS '(' dictionary ')'
                        {
                            $$.setExpr(createValue(no_existsdict, makeBoolType(), $3.getExpr()));
                        }
    | MAP '(' mapSpec ',' expression ')'
                        {
                            parser->normalizeExpression($5);
                            HqlExprArray args;
                            $3.unwindCommaList(args);
                            ITypeInfo * retType = parser->promoteMapToSameType(args, $5);
                            args.append(*$5.getExpr());
                            $$.setExpr(createValue(no_map, retType, args));
                        }
    | CASE '(' expression ',' beginList caseSpec ',' expression ')'
                        {
                            parser->normalizeExpression($3);
                            parser->normalizeExpression($8);
                            HqlExprArray args;
                            parser->endList(args);
                            parser->checkCaseForDuplicates(args, $6);
                            ITypeInfo * retType = parser->promoteCaseToSameType($3, args, $8);
                            args.add(*$3.getExpr(),0);
                            args.append(*$8.getExpr());
                            $$.setExpr(createValue(no_case, retType, args));
                        }
    | CASE '(' expression ',' beginList expression ')'
                        {
                            parser->normalizeExpression($3);
                            parser->normalizeExpression($6);
                            // change error to warning.
                            parser->reportWarning(CategoryUnusual, WRN_CASENOCONDITION, $1.pos, "CASE does not have any conditions");
                            HqlExprArray args;
                            parser->endList(args);
                            ::Release($3.getExpr());
                            $$.setExpr($6.getExpr(), $1);
                        }
    | IF '(' booleanExpr ',' expression ',' expression ')'
                        {
                            parser->normalizeExpression($5);
                            parser->normalizeExpression($7);
                            $$.setExpr(parser->processIfProduction($3, $5, &$7), $1);
                        }
    | IFF '(' booleanExpr ',' expression ',' expression ')'
                        {
                            parser->normalizeExpression($5);
                            parser->normalizeExpression($7);
                            $$.setExpr(parser->createIff($3, $5, $7), $1);
                        }
    | EXP '(' expression ')'
                        {
                            parser->normalizeExpression($3, type_real, false);
                            $$.setExpr(createValue(no_exp, makeRealType(8), $3.getExpr()));
                        }
    | HASH '(' beginList sortList ')'
                        {
                            HqlExprArray sortItems;
                            parser->endList(sortItems);
                            IHqlExpression * hash = parser->processSortList($4, no_hash, NULL, sortItems, NULL, NULL);
                            $$.setExpr(createValue(no_hash, LINK(parser->uint4Type), hash));
                        }
    | HASH32 '(' beginList sortList ')'
                        {
                            HqlExprArray sortItems;
                            parser->endList(sortItems);
                            IHqlExpression * hash = parser->processSortList($4, no_hash, NULL, sortItems, NULL, NULL);
                            $$.setExpr(createValue(no_hash32, makeIntType(4, false), hash));
                        }
    | HASH64 '(' beginList sortList ')'
                        {
                            HqlExprArray sortItems;
                            parser->endList(sortItems);
                            IHqlExpression * hash = parser->processSortList($4, no_hash, NULL, sortItems, NULL, NULL);
                            $$.setExpr(createValue(no_hash64, makeIntType(8, false), hash));
                        }
    | HASHMD5 '(' beginList sortList ')'
                        {
                            HqlExprArray sortItems;
                            parser->endList(sortItems);
                            IHqlExpression * hash = parser->processSortList($4, no_hash, NULL, sortItems, NULL, NULL);
                            $$.setExpr(createValue(no_hashmd5, makeDataType(16), hash));
                        }
    | CRC '(' beginList sortList ')'
                        {
                            HqlExprArray sortItems;
                            parser->endList(sortItems);
                            IHqlExpression * hash = parser->processSortList($4, no_hash, NULL, sortItems, NULL, NULL);
                            $$.setExpr(createValue(no_crc, LINK(parser->uint4Type), hash), $1);
                        }
    | ECLCRC '(' goodObject ')'
                        {
                            $$.setExpr(createValue(no_eclcrc, LINK(parser->uint4Type), createAttribute(_original_Atom, $3.getExpr())), $1);
                        }
    | ECLCRC '(' goodObject ',' PARSE ')'
                        {
                            OwnedHqlExpr expr = $3.getExpr();
                            $$.setExpr(getSizetConstant(getExpressionCRC(expr)));
                        }
    | LN '(' expression ')'
                        {
                            parser->normalizeExpression($3, type_real, false);
                            $$.setExpr(createValue(no_ln, makeRealType(8), $3.getExpr()));
                        }
    | SIN '(' expression ')'
                        {
                            parser->normalizeExpression($3, type_real, false);
                            $$.setExpr(createValue(no_sin, makeRealType(8), $3.getExpr()));
                        }
    | COS '(' expression ')'
                        {
                            parser->normalizeExpression($3, type_real, false);
                            $$.setExpr(createValue(no_cos, makeRealType(8), $3.getExpr()));
                        }
    | TAN '(' expression ')'
                        {
                            parser->normalizeExpression($3, type_real, false);
                            $$.setExpr(createValue(no_tan, makeRealType(8), $3.getExpr()));
                        }
    | ASIN '(' expression ')'
                        {
                            parser->normalizeExpression($3, type_real, false);
                            $$.setExpr(createValue(no_asin, makeRealType(8), $3.getExpr()));
                        }
    | ACOS '(' expression ')'
                        {
                            parser->normalizeExpression($3, type_real, false);
                            $$.setExpr(createValue(no_acos, makeRealType(8), $3.getExpr()));
                        }
    | ATAN '(' expression ')'
                        {
                            parser->normalizeExpression($3, type_real, false);
                            $$.setExpr(createValue(no_atan, makeRealType(8), $3.getExpr()));
                        }
    | ATAN2 '(' expression ',' expression ')'
                        {
                            parser->normalizeExpression($3, type_real, false);
                            parser->normalizeExpression($5, type_real, false);
                            $$.setExpr(createValue(no_atan2, makeRealType(8), $3.getExpr(), $5.getExpr()));
                        }
    | SINH '(' expression ')'
                        {
                            parser->normalizeExpression($3, type_real, false);
                            $$.setExpr(createValue(no_sinh, makeRealType(8), $3.getExpr()));
                        }
    | COSH '(' expression ')'
                        {
                            parser->normalizeExpression($3, type_real, false);
                            $$.setExpr(createValue(no_cosh, makeRealType(8), $3.getExpr()));
                        }
    | TANH '(' expression ')'
                        {
                            parser->normalizeExpression($3, type_real, false);
                            $$.setExpr(createValue(no_tanh, makeRealType(8), $3.getExpr()));
                        }
    | GLOBAL '(' expression globalOpts ')'
                        {
                            parser->normalizeExpression($3);
                            IHqlExpression * value = $3.getExpr();
                            $$.setExpr(createValueF(no_globalscope, value->getType(), value, $4.getExpr(), NULL));
                            $$.setPosition($1);
                        }
    | TOK_LOG '(' expression ')'
                        {
                            parser->normalizeExpression($3, type_real, false);
                            $$.setExpr(createValue(no_log10, makeRealType(8), $3.getExpr()));
                        }
    | POWER '(' expression ',' expression ')'
                        {
                            parser->normalizeExpression($3, type_real, false);
                            parser->normalizeExpression($5, type_numeric, false);
                            $$.setExpr(createValue(no_power, makeRealType(8),$3.getExpr(), $5.getExpr()));
                        }
    | RANDOM '(' ')'
                        {
                            $$.setExpr(createValue(no_random, LINK(parser->uint4Type), parser->createUniqueId()));
                        }
    | ROUND '(' expression ')'
                        {
                            parser->normalizeExpression($3, type_numeric, false);
                            ITypeInfo * type = getRoundType($3.queryExprType());
                            $$.setExpr(createValue(no_round, type, $3.getExpr()));
                        }
    | ROUND '(' expression ',' expression ')'
                        {
                            parser->normalizeExpression($3, type_numeric, false);
                            ITypeInfo * type = getRoundToType($3.queryExprType());
                            $$.setExpr(createValue(no_round, type, $3.getExpr(), $5.getExpr()));
                        }
    | ROUNDUP '(' expression ')'
                        {
                            parser->normalizeExpression($3, type_numeric, false);
                            ITypeInfo * type = getRoundType($3.queryExprType());
                            $$.setExpr(createValue(no_roundup, type, $3.getExpr()));
                        }
    | SQRT '(' expression ')'
                        {
                            parser->normalizeExpression($3, type_real, false);
                            $$.setExpr(createValue(no_sqrt, makeRealType(8), $3.getExpr()));
                        }
    | TRUNCATE '(' expression ')'
                        {
                            parser->normalizeExpression($3, type_numeric, false);
                            ITypeInfo * type = getTruncType($3.queryExprType());
                            $$.setExpr(createValue(no_truncate, type, $3.getExpr()));
                        }
    | LENGTH '(' expression ')'
                        {
                            parser->normalizeExpression($3, type_stringorunicode, false);
                            $$.setExpr(createValue(no_charlen, LINK(parser->uint4Type), $3.getExpr()));
                        }
    | TRIM '(' expression optTrimFlags ')'
                        {
                            parser->normalizeExpression($3, type_stringorunicode, false);
                            OwnedHqlExpr expr = $3.getExpr();
                            OwnedHqlExpr flags = $4.getExpr();
                            $$.setExpr(createTrimExpr(expr, flags));
                        }
    | NOFOLD '(' expression ')'
                        {
                            parser->normalizeExpression($3);
                            IHqlExpression * expr = $3.getExpr();
                            $$.setExpr(createValue(no_nofold, expr->getType(), expr));
                        }
    | NOHOIST '(' expression ')'
                        {
                            parser->normalizeExpression($3);
                            IHqlExpression * expr = $3.getExpr();
                            $$.setExpr(createValue(no_nohoist, expr->getType(), expr));
                        }
    | NOTHOR '(' expression ')'
                        {
                            parser->normalizeExpression($3);
                            IHqlExpression * expr = $3.getExpr();
                            $$.setExpr(createValue(no_nothor, expr->getType(), expr));
                        }
    | ABS '(' expression ')'
                        {
                            parser->normalizeExpression($3, type_numeric, false);
                            IHqlExpression * expr = $3.getExpr();
                            $$.setExpr(createValue(no_abs, expr->getType(), expr), $1);
                        }
    | INTFORMAT '(' expression ',' expression ',' expression ')'
                        {
                            parser->normalizeExpression($3, type_int, false);
                            parser->normalizeExpression($5, type_int, false);
                            parser->normalizeExpression($7, type_int, false);
                            IHqlExpression *len = $5.getExpr();
                            IHqlExpression *flen = foldHqlExpression(len);
                            IValue *length = flen->queryValue();
                            unsigned resultSize = UNKNOWN_LENGTH;
                            if (length)
                            {
                                resultSize = (unsigned) length->getIntValue();
                                if ((int) resultSize < 0)
                                {
                                    resultSize = 0;
                                    parser->reportError(ERR_NEGATIVE_WIDTH, $5, "INTFORMAT does not support negative widths");
                                }
                            }

                            $$.setExpr(createValue(no_intformat, makeStringType(resultSize, NULL, NULL), $3.getExpr(), flen, $7.getExpr()));
                            len->Release();
                        }
    | REALFORMAT '(' expression ',' expression ',' expression ')'
                        {
                            parser->normalizeExpression($3, type_real, false);
                            parser->normalizeExpression($5, type_int, false);
                            parser->normalizeExpression($7, type_int, false);

                            OwnedHqlExpr flen = foldHqlExpression($5.queryExpr());
                            IValue *length = flen->queryValue();
                            if (length && (length->getIntValue() < 0))
                                parser->reportError(ERR_NEGATIVE_WIDTH, $5, "REALFORMAT does not support negative widths");
                            $$.setExpr(createValue(no_realformat, makeStringType(UNKNOWN_LENGTH, NULL, NULL), $3.getExpr(), $5.getExpr(), $7.getExpr()));
                        }
    | TOXML '(' dataRow ')'
                        {
                            //MORE Could allow ,NOTRIM,OPT,???flags
                            $$.setExpr(createValue(no_toxml, makeUtf8Type(UNKNOWN_LENGTH, NULL), $3.getExpr()));
                        }
    | TOJSON '(' dataRow ')'
                        {
                            //MORE Could allow ,NOTRIM,OPT,???flags
                            $$.setExpr(createValue(no_tojson, makeUtf8Type(UNKNOWN_LENGTH, NULL), $3.getExpr()));
                        }
    | REGEXFIND '(' expression ',' expression regexOpt ')'
                        {
                            parser->normalizeExpression($3, type_stringorunicode, false);
                            if(isUnicodeType($3.queryExprType()))
                                parser->normalizeExpression($5, type_unicode, false);
                            else
                                parser->normalizeExpression($5, type_string, false);
                            $$.setExpr(createValue(no_regex_find, makeBoolType(), $3.getExpr(), $5.getExpr(), $6.getExpr()));
                        }
    | REGEXFIND '(' expression ',' expression ',' expression regexOpt ')'
                        {
                            parser->normalizeExpression($3, type_stringorunicode, false);
                            Owned<ITypeInfo> subType;
                            if(isUnicodeType($3.queryExprType()))
                            {
                                parser->normalizeExpression($5, type_unicode, false);
                                subType.setown(makeUnicodeType(UNKNOWN_LENGTH, 0));
                            }
                            else
                            {
                                parser->normalizeExpression($5, type_string, false);
                                subType.setown(makeStringType(UNKNOWN_LENGTH));
                            }
                            parser->normalizeExpression($7, type_int, false);
                            $$.setExpr(createValue(no_regex_find, subType.getLink(), $3.getExpr(), $5.getExpr(), $7.getExpr(), $8.getExpr()));
                        }
    | REGEXREPLACE '(' expression ',' expression ',' expression regexOpt ')'
                        {
                            parser->normalizeExpression($3, type_stringorunicode, false);
                            Owned<ITypeInfo> retType;
                            if(isUnicodeType($3.queryExprType()))
                            {
                                parser->normalizeExpression($5, type_unicode, false);
                                parser->normalizeExpression($7, type_unicode, false);
                                retType.setown(makeUnicodeType(UNKNOWN_LENGTH, 0));
                            }
                            else
                            {
                                parser->normalizeExpression($5, type_string, false);
                                parser->normalizeExpression($7, type_string, false);
                                retType.setown(makeStringType(UNKNOWN_LENGTH));
                            }
                            $$.setExpr(createValue(no_regex_replace, retType.getLink(), $3.getExpr(), $5.getExpr(), $7.getExpr(), $8.getExpr()));
                        }
    | ASSTRING '(' expression ')'
                        {
                            parser->normalizeExpression($3);
                            IHqlExpression *expr = $3.getExpr();
                            $$.setExpr(createValue(no_asstring, makeStringType(expr->queryType()->getSize(),NULL,NULL), expr));
                        }
    | TRANSFER '(' expression ',' scalarType ')'
                        {
                            parser->normalizeExpression($3);
                            IHqlExpression *expr = $3.getExpr();
                            ITypeInfo *exprType = expr->queryType();
                            ITypeInfo *type = $5.getType();
                            if ((exprType->getSize() != UNKNOWN_LENGTH) && (exprType->getSize() < type->getSize()) && type->getSize() != UNKNOWN_LENGTH)
                                parser->reportError(ERR_TYPETRANS_LARGERTYPE, $5, "Type transfer: target type in is larger than source type");
                            $$.setExpr(createTypeTransfer(expr, type));
                        }
    | TRANSFER '(' dataRow ',' scalarType ')'
                        {
                            //User had better know what they are doing
                            $$.setExpr(createTypeTransfer($3.getExpr(), $5.getType()), $1);
                        }
    | TRANSFER '(' dataSet ',' scalarType ')'
                        {
                            //User had better know what they are doing
                            $$.setExpr(createTypeTransfer($3.getExpr(), $5.getType()), $1);
                        }
    | MAX '(' startTopFilter ',' expression aggregateFlags ')' endTopFilter
                        {
                            parser->normalizeExpression($5);
                            IHqlExpression *e5 = $5.getExpr();
                            $$.setExpr(createValue(no_max, e5->getType(), $3.getExpr(), e5, $6.getExpr()));
                        }
    | MAX '(' GROUP ',' expression ')'
                        {
                            parser->normalizeExpression($5);
                            IHqlExpression *e5 = $5.getExpr();
                            $$.setExpr(createValue(no_maxgroup, e5->getType(), e5));
                        }
    | MIN '(' startTopFilter ',' expression aggregateFlags ')' endTopFilter
                        {
                            parser->normalizeExpression($5);
                            IHqlExpression *e5 = $5.getExpr();
                            $$.setExpr(createValue(no_min, e5->getType(), $3.getExpr(), e5, $6.getExpr()));
                        }
    | MIN '(' GROUP ',' expression ')'
                        {
                            parser->normalizeExpression($5);
                            IHqlExpression *e5 = $5.getExpr();
                            $$.setExpr(createValue(no_mingroup, e5->getType(), e5));
                        }
    | EVALUATE '(' evaluateTopFilter ',' expression ')' endTopFilter
                        {
                            parser->normalizeExpression($5);
                            OwnedHqlExpr expr = $5.getExpr();
                            OwnedHqlExpr scope = $3.getExpr();
                            $$.setExpr(createValue(no_evaluate, expr->getType(), LINK(scope), LINK(expr)));
                        }
    | SUM '(' startTopFilter ',' expression aggregateFlags ')' endTopFilter
                        {
                            parser->normalizeExpression($5);
                            Owned<ITypeInfo> temp = parser->checkPromoteNumeric($5, true);
                            OwnedHqlExpr value = $5.getExpr();
                            Owned<ITypeInfo> type = getSumAggType(value);
                            $$.setExpr(createValue(no_sum, LINK(type), $3.getExpr(), ensureExprType(value, type), $6.getExpr()));
                        }
    | SUM '(' GROUP ',' expression optExtraFilter ')'
                        {
                            parser->normalizeExpression($5);
                            Owned<ITypeInfo> temp = parser->checkPromoteNumeric($5, true);
                            OwnedHqlExpr value = $5.getExpr();
                            Owned<ITypeInfo> type = getSumAggType(value);
                            $$.setExpr(createValue(no_sumgroup, LINK(type), ensureExprType(value, type), $6.getExpr()));
                        }
    | AVE '(' startTopFilter ',' expression aggregateFlags ')' endTopFilter
                        {
                            parser->normalizeExpression($5, type_numeric, false);
                            $$.setExpr(createValue(no_ave, makeRealType(8), $3.getExpr(), $5.getExpr(), $6.getExpr()));
                        }
    | AVE '(' GROUP ',' expression optExtraFilter')'
                        {
                            parser->normalizeExpression($5, type_numeric, false);
                            $$.setExpr(createValue(no_avegroup, makeRealType(8), $5.getExpr(), $6.getExpr()));
                        }
    | VARIANCE '(' startTopFilter ',' expression aggregateFlags ')' endTopFilter
                        {
                            parser->normalizeExpression($5, type_numeric, false);
                            $$.setExpr(createValue(no_variance, makeRealType(8), $3.getExpr(), $5.getExpr(), $6.getExpr()));
                        }
    | VARIANCE '(' GROUP ',' expression optExtraFilter')'
                        {
                            parser->normalizeExpression($5, type_numeric, false);
                            $$.setExpr(createValue(no_vargroup, makeRealType(8), $5.getExpr(), $6.getExpr()));
                        }
    | COVARIANCE '(' startTopFilter ',' expression ',' expression aggregateFlags ')' endTopFilter
                        {
                            parser->normalizeExpression($5, type_numeric, false);
                            parser->normalizeExpression($7, type_numeric, false);
                            $$.setExpr(createValue(no_covariance, makeRealType(8), $3.getExpr(), $5.getExpr(), $7.getExpr(), $8.getExpr()));
                        }
    | COVARIANCE '(' GROUP ',' expression ',' expression optExtraFilter')'
                        {
                            parser->normalizeExpression($5, type_numeric, false);
                            parser->normalizeExpression($7, type_numeric, false);
                            $$.setExpr(createValue(no_covargroup, makeRealType(8), $5.getExpr(), $7.getExpr(), $8.getExpr()));
                        }
    | CORRELATION '(' startTopFilter ',' expression ',' expression aggregateFlags ')' endTopFilter
                        {
                            parser->normalizeExpression($5, type_numeric, false);
                            parser->normalizeExpression($7, type_numeric, false);
                            $$.setExpr(createValue(no_correlation, makeRealType(8), $3.getExpr(), $5.getExpr(), $7.getExpr(), $8.getExpr()));
                        }
    | CORRELATION '(' GROUP ',' expression ',' expression optExtraFilter')'
                        {
                            parser->normalizeExpression($5, type_numeric, false);
                            parser->normalizeExpression($7, type_numeric, false);
                            $$.setExpr(createValue(no_corrgroup, makeRealType(8), $5.getExpr(), $7.getExpr(), $8.getExpr()));
                        }
    | WHICH conditions  {
                            $$.setExpr(createList(no_which, LINK(parser->uint4Type), $2.getExpr()));
                        }
    | REJECTED conditions
                        {
                            $$.setExpr(createList(no_rejected, LINK(parser->uint4Type), $2.getExpr()));
                        }
    | SIZEOF '(' sizeof_type_target optMaxMin ')'
                        {
                            ITypeInfo* type = $3.getType();
                            OwnedHqlExpr max = $4.getExpr();
                            if (!max)
                                parser->checkSizeof(type,$1);

                            //rather easier to create a dummy argument with the correct type.
                            $$.setExpr(createValue(no_sizeof, LINK(parser->uint4Type), createValue(no_none, type), max.getClear()));
                        }
    | SIZEOF '(' sizeof_expr_target optMaxMin ')'
                        {
                            OwnedHqlExpr arg = $3.getExpr();
                            OwnedHqlExpr max = $4.getExpr();
                            if (!max)
                                parser->checkSizeof(arg,$1);
                            $$.setExpr(createValue(no_sizeof, LINK(parser->uint4Type), arg.getClear(), max.getClear()));
                        }
    | SIZEOF '(' error ')'
                        {
                            parser->reportError(ERR_SIZEOF_WRONGPARAM, $1,"Illegal parameter for SIZEOF"); 
                            $$.setExpr(createConstant(1), $1);
                        }
    | RANK '(' expression ',' expression optAscDesc ')'
                        {
                            parser->normalizeExpression($3);
                            parser->normalizeExpression($5, type_set, false);
                            $$.setExpr(createValue(no_rank, LINK(parser->uint4Type), $3.getExpr(), $5.getExpr(), $6.getExpr()));
                        }
    | RANKED '(' expression ',' expression optAscDesc')'
                        {
                            parser->normalizeExpression($3);
                            parser->normalizeExpression($5, type_set, false);
                            $$.setExpr(createValue(no_ranked, LINK(parser->uint4Type), $3.getExpr(), $5.getExpr(), $6.getExpr()));
                        }
    | COUNT             {
                            $$.setExpr(parser->getActiveCounter($1));
                            parser->reportWarning(CategoryDeprecated, SeverityError, ERR_COUNTER_NOT_COUNT, $1.pos, "Use of COUNT instead of COUNTER is deprecated");
                        }
    | COUNTER               {
                            $$.setExpr(parser->getActiveCounter($1));
                        }
    | ISNULL '(' expression ')'
                        {
                            parser->normalizeExpression($3);
                            $$.setExpr(createValue(no_is_null, makeBoolType(), $3.getExpr()));
                        }
    | ISVALID '(' expression ')'
                        {
                            parser->normalizeExpression($3);
                            $$.setExpr(createValue(no_is_valid, makeBoolType(), $3.getExpr()));
                        }
    | OMITTED '(' goodObject ')'
                        {
                            parser->reportError(ERR_EXPECTED, $1, "Not yet supported - needs more work");
                            parser->normalizeExpression($3);
                            OwnedHqlExpr value = $3.getExpr();
                            if (value->getOperator() != no_param)
                                parser->reportError(ERR_EXPECTED, $3, "Expected a parameter as the argument");
                            $$.setExpr(createValue(no_isomitted, makeBoolType(), value.getClear()));
                        }
    | FAILCODE          {   $$.setExpr(createValue(no_failcode, makeIntType(4, true))); }
    | FAILCODE '(' ')'  {   $$.setExpr(createValue(no_failcode, makeIntType(4, true))); }
    | FAILMESSAGE       {   $$.setExpr(createValue(no_failmessage, makeStringType(UNKNOWN_LENGTH, NULL, NULL))); }
    | FAILMESSAGE '(' ')' {   $$.setExpr(createValue(no_failmessage, makeStringType(UNKNOWN_LENGTH, NULL, NULL))); }
    | FAILMESSAGE '(' expression ')'
                        {
                            parser->normalizeExpression($3, type_string, false);
                            $$.setExpr(createValue(no_failmessage, makeStringType(UNKNOWN_LENGTH, NULL, NULL), $3.getExpr()));
                        }
    | EVENTNAME         {   $$.setExpr(createValue(no_eventname, makeStringType(UNKNOWN_LENGTH, NULL, NULL))); }
    | EVENTNAME '(' ')' {   $$.setExpr(createValue(no_eventname, makeStringType(UNKNOWN_LENGTH, NULL, NULL))); }
    | EVENTEXTRA        {   $$.setExpr(createValue(no_eventextra, makeStringType(UNKNOWN_LENGTH, NULL, NULL))); }
    | EVENTEXTRA '(' ')'
                        {   $$.setExpr(createValue(no_eventextra, makeStringType(UNKNOWN_LENGTH, NULL, NULL))); }
    | EVENTEXTRA '(' expression ')'
                        {
                            parser->normalizeExpression($3, type_string, false);
                            $$.setExpr(createValue(no_eventextra, makeStringType(UNKNOWN_LENGTH, NULL, NULL), $3.getExpr()));
                        }
    | TOK_ERROR '(' expression ',' expression ')'
                        {
                            parser->normalizeExpression($3, type_int, false);
                            parser->normalizeExpression($5, type_string, false);
                            $$.setExpr(createValue(no_fail, makeAnyType(), $3.getExpr(), $5.getExpr()));
                        }
    | TOK_ERROR '(' expression ')'
                        {
                            parser->checkIntegerOrString($3);
                            $$.setExpr(createValue(no_fail, makeAnyType(), $3.getExpr()));
                        }
    | TOK_ERROR '(' ')'         {
                            $$.setExpr(createValue(no_fail, makeAnyType()));
                        }
    | FAIL '(' scalarType ',' expression ',' expression ')'
                        {
                            $3.release();
                            parser->normalizeExpression($5, type_int, false);
                            parser->normalizeExpression($7, type_string, false);
                            $$.setExpr(createValue(no_fail, makeAnyType(), $5.getExpr(), $7.getExpr()));
                        }
    | FAIL '(' scalarType ',' expression ')'
                        {
                            $3.release();
                            parser->normalizeExpression($5);
                            parser->checkIntegerOrString($5);
                            $$.setExpr(createValue(no_fail, makeAnyType(), $5.getExpr()));
                        }
    | FAIL '(' scalarType ')'           
                        {
                            $3.release();
                            $$.setExpr(createValue(no_fail, makeAnyType()));
                        }
    | SKIP              {
                            if (!parser->curTransform)
                                parser->reportError(ERR_PARSER_CANNOTRECOVER,$1,"SKIP is only valid inside a TRANSFORM");
                            $$.setExpr(createValue(no_skip, makeAnyType()));
                        }
    | FROMUNICODE '(' expression ',' expression ')'
                        {
                            parser->normalizeExpression($3, type_unicode, false);
                            parser->normalizeExpression($5, type_string, false);
                            $$.setExpr(createValue(no_fromunicode, makeDataType(UNKNOWN_LENGTH), $3.getExpr(), $5.getExpr()));
                        }
    | TOUNICODE '(' expression ',' expression ')'
                        {
                            parser->normalizeExpression($3);
                            parser->ensureData($3);
                            parser->normalizeExpression($5, type_string, false);
                            $$.setExpr(createValue(no_tounicode, makeUnicodeType(UNKNOWN_LENGTH, 0), $3.getExpr(), $5.getExpr()));
                        }
    | KEYUNICODE '(' expression ')'
                        {
                            parser->normalizeExpression($3, type_unicode, false);
                            $$.setExpr(createValue(no_keyunicode, makeDataType(UNKNOWN_LENGTH), $3.getExpr(), createConstant(str($3.queryExprType()->queryLocale())), createConstant(3)));
                        }
    | KEYUNICODE '(' expression ',' expression ')'
                        {
                            parser->normalizeExpression($3, type_unicode, false);
                            parser->normalizeExpression($5);
                            parser->ensureString($5);
                            Owned<IHqlExpression> lexpr = $5.getExpr();
                            Owned<ITypeInfo> ltype = lexpr->getType();
                            Owned<IHqlExpression> locale = (ltype->getTypeCode() == type_varstring) ? lexpr.getLink() : createValue(no_implicitcast, makeVarStringType(ltype->getStringLen()), lexpr.getLink());
                            $$.setExpr(createValue(no_keyunicode, makeDataType(UNKNOWN_LENGTH), $3.getExpr(), locale.getLink(), createConstant(3)));
                        }
    | KEYUNICODE '(' expression ',' ',' expression ')'
                        {
                            parser->normalizeExpression($3, type_unicode, false);
                            parser->normalizeExpression($6, type_int, false);
                            $$.setExpr(createValue(no_keyunicode, makeDataType(UNKNOWN_LENGTH), $3.getExpr(), createConstant(str($3.queryExprType()->queryLocale())), $6.getExpr()));
                        }
    | KEYUNICODE '(' expression ',' expression ',' expression ')'
                        {
                            parser->normalizeExpression($3, type_unicode, false);
                            parser->normalizeExpression($5);
                            parser->normalizeExpression($7, type_int, false);
                            parser->ensureString($5);
                            Owned<IHqlExpression> lexpr = $5.getExpr();
                            Owned<ITypeInfo> ltype = lexpr->getType();
                            Owned<IHqlExpression> locale = (ltype->getTypeCode() == type_varstring) ? lexpr.getLink() : createValue(no_implicitcast, makeVarStringType(ltype->getStringLen()), lexpr.getLink());
                            $$.setExpr(createValue(no_keyunicode, makeDataType(UNKNOWN_LENGTH), $3.getExpr(), locale.getLink(), $7.getExpr()));
                        }
    | MATCHED '(' patternReference ')'
                        {
                            $$.setExpr(createValue(no_matched, makeBoolType(), $3.getExpr())); //, parser->createUniqueId()));
                        }
    | MATCHTEXT '(' patternReference ')'
                        {
                            $$.setExpr(createValue(no_matchtext, makeStringType(UNKNOWN_LENGTH, NULL, NULL), $3.getExpr())); //, parser->createUniqueId()));
                        }
    | MATCHUNICODE '(' patternReference ')'
                        {
                            $$.setExpr(createValue(no_matchunicode, makeUnicodeType(UNKNOWN_LENGTH, NULL), $3.getExpr())); //, parser->createUniqueId()));
                        }
    | MATCHUTF8 '(' patternReference ')'
                        {
                            $$.setExpr(createValue(no_matchutf8, makeUtf8Type(UNKNOWN_LENGTH, NULL), $3.getExpr())); //, parser->createUniqueId()));
                        }
    | MATCHLENGTH '(' patternReference ')'
                        {
                            $$.setExpr(createValue(no_matchlength, LINK(parser->uint4Type), $3.getExpr())); //, parser->createUniqueId()));
                        }
    | MATCHPOSITION '(' patternReference ')'
                        {
                            $$.setExpr(createValue(no_matchposition, LINK(parser->uint4Type), $3.getExpr())); //, parser->createUniqueId()));
                        }
    | MATCHED
                        {
                            $$.setExpr(createValue(no_matched, makeBoolType())); //, parser->createUniqueId()));
                        }
    | MATCHTEXT
                        {
                            $$.setExpr(createValue(no_matchtext, makeStringType(UNKNOWN_LENGTH, NULL, NULL))); //, parser->createUniqueId()));
                        }
    | MATCHUNICODE
                        {
                            $$.setExpr(createValue(no_matchunicode, makeUnicodeType(UNKNOWN_LENGTH, NULL))); //, parser->createUniqueId()));
                        }
    | MATCHUTF8
                        {
                            $$.setExpr(createValue(no_matchutf8, makeUtf8Type(UNKNOWN_LENGTH, NULL))); //, parser->createUniqueId()));
                        }
    | MATCHLENGTH
                        {
                            $$.setExpr(createValue(no_matchlength, LINK(parser->uint4Type))); //, parser->createUniqueId()));
                        }
    | MATCHPOSITION
                        {
                            $$.setExpr(createValue(no_matchposition, LINK(parser->uint4Type))); //, parser->createUniqueId()));
                        }
    | MATCHED '(' ')'
                        {
                            $$.setExpr(createValue(no_matched, makeBoolType())); //, parser->createUniqueId()));
                        }
    | MATCHTEXT '(' ')'
                        {
                            $$.setExpr(createValue(no_matchtext, makeStringType(UNKNOWN_LENGTH, NULL, NULL))); //, parser->createUniqueId()));
                        }
    | MATCHUNICODE '(' ')'
                        {
                            $$.setExpr(createValue(no_matchunicode, makeUnicodeType(UNKNOWN_LENGTH, NULL))); //, parser->createUniqueId()));
                        }
    | MATCHUTF8 '(' ')'
                        {
                            $$.setExpr(createValue(no_matchutf8, makeUtf8Type(UNKNOWN_LENGTH, NULL))); //, parser->createUniqueId()));
                        }
    | MATCHLENGTH '(' ')'
                        {
                            $$.setExpr(createValue(no_matchlength, LINK(parser->uint4Type))); //, parser->createUniqueId()));
                        }
    | MATCHPOSITION '(' ')'
                        {
                            $$.setExpr(createValue(no_matchposition, LINK(parser->uint4Type))); //, parser->createUniqueId()));
                        }
    | MATCHTEXT '(' expression ')'
                        {
                            parser->normalizeExpression($3);
                            $$.setExpr(parser->createCheckMatchAttr($3, type_string));
                            $$.setPosition($1);
                        }
    | MATCHUNICODE '(' expression ')'
                        {
                            parser->normalizeExpression($3);
                            $$.setExpr(parser->createCheckMatchAttr($3, type_unicode));
                            $$.setPosition($1);
                        }
    | MATCHUTF8 '(' expression ')'
                        {
                            parser->normalizeExpression($3);
                            $$.setExpr(parser->createCheckMatchAttr($3, type_utf8));
                            $$.setPosition($1);
                        }
    | MATCHTEXT '(' dataRow ')'
                        {
                            $$.setExpr(parser->createCheckMatchAttr($3, type_string));
                            $$.setPosition($1);
                        }
    | MATCHUNICODE '(' dataRow ')'
                        {
                            $$.setExpr(parser->createCheckMatchAttr($3, type_unicode));
                            $$.setPosition($1);
                        }
    | MATCHUTF8 '(' dataRow ')'
                        {
                            $$.setExpr(parser->createCheckMatchAttr($3, type_utf8));
                            $$.setPosition($1);
                        }
    | ROWDIFF '(' dataRow ',' dataRow optCount ')'
                        {
                            $$.setExpr(createValue(no_rowdiff, makeStringType(UNKNOWN_LENGTH, NULL, NULL), $3.getExpr(), $5.getExpr(), $6.getExpr()));
                        }
    | WORKUNIT          {   $$.setExpr(createValue(no_wuid, makeStringType(UNKNOWN_LENGTH, NULL, NULL))); }
    | XMLDECODE '(' expression ')'
                        {
                            parser->normalizeExpression($3, type_stringorunicode, false);
                            ITypeInfo * type = isUnicodeType($3.queryExprType()) ? makeUnicodeType(UNKNOWN_LENGTH, NULL) : makeStringType(UNKNOWN_LENGTH, NULL, NULL);
                            $$.setExpr(createValue(no_xmldecode, type, $3.getExpr()));
                        }
    | XMLENCODE '(' expression xmlEncodeFlags ')'
                        {
                            parser->normalizeExpression($3, type_stringorunicode, false);
                            ITypeInfo * type = isUnicodeType($3.queryExprType()) ? makeUnicodeType(UNKNOWN_LENGTH, NULL) : makeStringType(UNKNOWN_LENGTH, NULL, NULL);
                            $$.setExpr(createValue(no_xmlencode, type, $3.getExpr(), $4.getExpr()));
                        }
    | XMLTEXT '(' expression ')'
                        {
                            parser->normalizeExpression($3, type_string, false);
                            parser->validateXPath($3);
                            $$.setExpr(createValue(no_xmltext, makeStringType(UNKNOWN_LENGTH, NULL, NULL), $3.getExpr(), parser->createUniqueId()));
                        }
    | XMLUNICODE '(' expression ')'
                        {
                            parser->normalizeExpression($3, type_string, false);
                            parser->validateXPath($3);
                            $$.setExpr(createValue(no_xmlunicode, makeUnicodeType(UNKNOWN_LENGTH, NULL), $3.getExpr(), parser->createUniqueId()));
                        }
    | KEYED '(' expression ')'
                        {
                            parser->normalizeExpression($3);
                            IHqlExpression * e = $3.getExpr();
                            $$.setExpr(createValue(no_assertkeyed, e->getType(), e));
                        }
    | KEYED '(' expression ',' OPT ')'
                        {
                            parser->normalizeExpression($3);
                            IHqlExpression * e = $3.getExpr();
                            $$.setExpr(createValue(no_assertkeyed, e->getType(), e, createAttribute(extendAtom)));
                        }
    | STEPPED '(' expression ')'
                        {
                            parser->normalizeExpression($3);
                            IHqlExpression * e = $3.getExpr();
                            $$.setExpr(createValue(no_assertstepped, e->getType(), e));
                        }
    | WILD '(' expression ')'
                        {
                            parser->normalizeExpression($3);
                            IHqlExpression * e = $3.getExpr();
                            if (e->getOperator() != no_select)
                                parser->reportError(ERR_EXPECTED, $2, "WILD requires a key field as a parameter");
                            $$.setExpr(createValue(no_assertwild, makeBoolType(), e));
                        }
    | TOK_CATCH '(' expression ',' expression ')'
                        {
                            parser->normalizeExpression($3);
                            parser->normalizeExpression($5);
                            ITypeInfo * type = parser->promoteToSameType($3, $5);
                            $$.setExpr(createValue(no_catch, type, $3.getExpr(), $5.getExpr()));
                        }
    | __COMPOUND__ '(' action ',' expression ')'
                        {
                            parser->normalizeExpression($5);
                            //Not public! only for internal testing.
                            $$.setExpr(createCompound($3.getExpr(), $5.getExpr()));
                            $$.setPosition($1);
                        }
    | WHEN '(' expression ',' action sideEffectOptions ')'
                        {
                            parser->normalizeExpression($3);
                            OwnedHqlExpr options = $6.getExpr();
                            OwnedHqlExpr expr = $3.getExpr();
                            if (options)
                                $$.setExpr(createValueF(no_executewhen, expr->getType(), LINK(expr), $5.getExpr(), options.getClear(), NULL), $1);
                            else
                                $$.setExpr(createCompound($5.getExpr(), expr), $1);
                        }
    | __COMMON__ '(' expression ')'
                        {
                            parser->normalizeExpression($3);
                            $$.setExpr(createAliasOwn($3.getExpr(), NULL));
                            $$.setPosition($1);
                        }
    | CLUSTERSIZE
                        {
                            $$.setExpr(createValue(no_clustersize, makeIntType(4, false)));
                            $$.setPosition($1);
                        }
    | CHOOSENALL
                        {
                            $$.setExpr(createConstant(CHOOSEN_ALL_LIMIT));
                            $$.setPosition($1);
                        }
    | WORKUNIT '(' expression ',' simpleType ')'
                        {
                            parser->normalizeExpression($3, type_any, true);
                            OwnedHqlExpr seq = $3.getExpr();
                            OwnedHqlExpr name;
                            if (isStringType(seq->queryType()))
                            {
                                name.set(seq);
                                seq.setown(createConstant(0));
                            }
                            if (name)
                                name.setown(createExprAttribute(namedAtom, LINK(name)));
                            $$.setExpr(createValue(no_getresult, $5.getType(), createExprAttribute(sequenceAtom, LINK(seq)), LINK(name)));
                            $$.setPosition($1);
                        }
    | LOCAL '(' expression ')'
                        {
                            parser->normalizeExpression($3);
                            IHqlExpression * expr = $3.getExpr();
                            $$.setExpr(createValue(no_forcelocal, expr->getType(), expr));
                            $$.setPosition($1);
                        }
    | NOLOCAL '(' expression ')'
                        {
                            parser->normalizeExpression($3);
                            IHqlExpression * expr = $3.getExpr();
                            $$.setExpr(createValue(no_forcenolocal, expr->getType(), expr));
                            $$.setPosition($1);
                        }
    | THISNODE '(' expression ')'
                        {
                            parser->normalizeExpression($3);
                            IHqlExpression * expr = $3.getExpr();
                            $$.setExpr(createValue(no_thisnode, expr->getType(), expr));
                            $$.setPosition($1);
                        }
    | COUNT '(' expressionList ')'
                        {
                            OwnedHqlExpr list = parser->createListFromExpressionList($3);
                            $$.setExpr(createValue(no_countlist, LINK(parser->defaultIntegralType), LINK(list)));
                            $$.setPosition($1);
                        }
    | EXISTS '(' expressionList ')'
                        {
                            if (parser->isSingleValuedExpressionList($3))
                                parser->reportWarning(CategoryMistake, WRN_SILLY_EXISTS,$1.pos,"EXISTS() on a scalar expression is always true, was this intended?");

                            OwnedHqlExpr list = parser->createListFromExpressionList($3);
                            $$.setExpr(createValue(no_existslist, makeBoolType(), LINK(list)));
                            $$.setPosition($1);
                        }
    | SUM '(' expressionList ')'
                        {
                            OwnedHqlExpr list = parser->createListFromExpressionList($3);
                            ITypeInfo * elemType = parser->queryElementType($3, list);
                            Owned<ITypeInfo> sumType = getSumAggType(elemType);
                            $$.setExpr(createValue(no_sumlist, LINK(sumType), LINK(list)));
                            $$.setPosition($1);
                        }
    | MAX '(' expressionList ')'
                        {
                            OwnedHqlExpr list = parser->createListFromExpressionList($3);
                            ITypeInfo * elemType = parser->queryElementType($3, list);
                            $$.setExpr(createValue(no_maxlist, LINK(elemType), LINK(list)));
                            $$.setPosition($1);
                        }
    | MIN '(' expressionList ')'
                        {
                            OwnedHqlExpr list = parser->createListFromExpressionList($3);
                            ITypeInfo * elemType = parser->queryElementType($3, list);
                            $$.setExpr(createValue(no_minlist, LINK(elemType), LINK(list)));
                            $$.setPosition($1);
                        }
    | AVE '(' expressionList ')'
                        {
                            OwnedHqlExpr list = parser->createListFromExpressionList($3);
                            $$.setExpr(parser->createAveList($3, list));
                            $$.setPosition($1);
                        }
    | NAMEOF '(' dataSet ')'
                        {
                            $$.setExpr(createValue(no_nameof, makeStringType(UNKNOWN_LENGTH, NULL, NULL), $3.getExpr()));
                        }
    | UNICODEORDER '(' expression ',' expression ')' 
                        {
                            parser->normalizeExpression($3, type_unicode, false);
                            parser->normalizeExpression($5, type_unicode, false);
                            ::Release(parser->checkPromoteType($3, $5));
                            IAtom * locale = parser->ensureCommonLocale($3, $5);
                            $$.setExpr(createValue(no_unicodeorder, makeIntType(4, true), $3.getExpr(), $5.getExpr(), createConstant(str(locale)), createConstant(3)));
                        }
    | UNICODEORDER '(' expression ',' expression ',' expression ')'
                        {
                            parser->normalizeExpression($3, type_unicode, false);
                            parser->normalizeExpression($5, type_unicode, false);
                            parser->normalizeExpression($7);
                            parser->ensureString($7);
                            ::Release(parser->checkPromoteType($3, $5));
                            Owned<IHqlExpression> lexpr = $7.getExpr();
                            Owned<ITypeInfo> ltype = lexpr->getType();
                            Owned<IHqlExpression> locale = (ltype->getTypeCode() == type_varstring) ? lexpr.getLink() : createValue(no_implicitcast, makeVarStringType(ltype->getStringLen()), lexpr.getLink());
                            $$.setExpr(createValue(no_unicodeorder, makeIntType(4, true), $3.getExpr(), $5.getExpr(), locale.getLink(), createConstant(3)));
                        }
    | UNICODEORDER '(' expression ',' expression ',' ',' expression ')'
                        {
                            parser->normalizeExpression($3, type_unicode, false);
                            parser->normalizeExpression($5, type_unicode, false);
                            IAtom * locale = parser->ensureCommonLocale($3, $5);
                            parser->normalizeExpression($8, type_int, false);
                            ::Release(parser->checkPromoteType($3, $5));
                            $$.setExpr(createValue(no_unicodeorder, makeIntType(4, true), $3.getExpr(), $5.getExpr(), createConstant(str(locale)), $8.getExpr()));
                        }
    | UNICODEORDER '(' expression ',' expression ',' expression ',' expression ')'
                        {
                            parser->normalizeExpression($3, type_unicode, false);
                            parser->normalizeExpression($5, type_unicode, false);
                            parser->normalizeExpression($7);
                            parser->normalizeExpression($9, type_int, false);
                            parser->ensureString($7);
                            ::Release(parser->checkPromoteType($3, $5));
                            Owned<IHqlExpression> lexpr = $7.getExpr();
                            Owned<ITypeInfo> ltype = lexpr->getType();
                            Owned<IHqlExpression> locale = (ltype->getTypeCode() == type_varstring) ? lexpr.getLink() : createValue(no_implicitcast, makeVarStringType(ltype->getStringLen()), lexpr.getLink());
                            $$.setExpr(createValue(no_unicodeorder, makeIntType(4, true), $3.getExpr(), $5.getExpr(), locale.getLink(), $9.getExpr()));
                        }
    | '[' beginList nonDatasetList ']'
                        {
                            HqlExprArray sortItems;
                            parser->endList(sortItems);
                            OwnedHqlExpr fields = parser->processSortList($3, no_list, NULL, sortItems, NULL, NULL);
                            $$.setExpr(fields.getClear(), $1);
                        }
    | '[' ']'           {
                            $$.setExpr(createList(no_list, makeSetType(NULL), NULL));
                            $$.setPosition($1);
                        }
    | ALL               {
                            $$.setExpr(createValue(no_all, makeSetType(NULL), NULL));
                            $$.setPosition($1);
                        }
    | SET '(' startTopFilter ',' expression ')' endTopFilter
                        {
                            parser->normalizeExpression($5, type_scalar, false);
                            IHqlExpression * ds = $3.getExpr();
                            IHqlExpression * field = $5.getExpr();
                            $$.setExpr(createValue(no_createset, makeSetType(field->getType()), ds, field));
                            $$.setPosition($1);
                        }
    | GETENV '(' expression ')'
                        {
                            parser->normalizeExpression($3, type_stringorunicode, false);
                            $$.setExpr(createValue(no_getenv, makeVarStringType(UNKNOWN_LENGTH), $3.getExpr()), $1);
                        }
    | GETENV '(' expression ',' expression ')'
                        {
                            parser->normalizeExpression($3, type_stringorunicode, false);
                            parser->normalizeExpression($5, type_stringorunicode, false);
                            $$.setExpr(createValue(no_getenv, makeVarStringType(UNKNOWN_LENGTH), $3.getExpr(), $5.getExpr()), $1);
                        }
    | __STAND_ALONE__   
                        {
                            $$.setExpr(createValue(no_debug_option_value, makeBoolType(), createConstant("standAloneExe")));
                        }
    | __DEBUG__ '(' stringConstExpr ')'
                        {
                            $$.setExpr(createValue(no_debug_option_value, makeStringType(UNKNOWN_LENGTH, NULL), $3.getExpr()), $1);
                        }
    | __DEBUG__ '(' stringConstExpr ',' simpleType ')'
                        {
                            $$.setExpr(createValue(no_debug_option_value, $5.getType(), $3.getExpr()), $1);
                        }
    | __PLATFORM__
                        {
                            OwnedHqlExpr option = createConstant("targetClusterType");
                            $$.setExpr(createValue(no_debug_option_value, makeStringType(UNKNOWN_LENGTH, NULL), option.getClear()), $1);
                        }
    ;

optCount
    :                   {   $$.setNullExpr(); }
    | ',' COUNT         {   $$.setExpr(createAttribute(countAtom)); }
    ;

evaluateTopFilter
    : dataRow           {
                            parser->pushTopScope($1.queryExpr());
                            $$.setExpr($1.getExpr());
                            parser->insideEvaluate = true;
                        }
    ;


alienTypeInstance
    : qualifiedTypeId
                        {
                            parser->beginFunctionCall($1);  //
                        }
    optParams  // alien type
                        {
                            OwnedHqlExpr alienExpr;
                            OwnedHqlExpr args = $3.getExpr();
                            if ($1.queryExpr()->isFunction())
                            {
                                alienExpr.setown(parser->bindParameters($1, args.getClear()));
                            }
                            else
                            {
                                if (args)
                                    parser->reportError(ERR_TYPE_NOPARAMNEEDED, $1, "Type does not require parameters: %s", str($1.queryExpr()->queryName()));
                                alienExpr.setown($1.getExpr());
                            }
                            $$.setType(makeModifier(alienExpr->getType(), typemod_indirect, LINK(alienExpr)));
                            $$.setPosition($1);
                        }
    ;

sizeof_type_target
    : simpleType
    | setType
    | alienTypeInstance
    ;

sizeof_expr_target
    : expression
                        {
                            parser->normalizeExpression($1);
                            $$.inherit($1);
                        }
    | dataSet
    | dataRow
    | enumTypeId
    | recordDef
    | fieldSelectedFromRecord
                        {
                            OwnedHqlExpr rhs = $1.getExpr();
                            IHqlExpression * field = queryFieldFromSelect(rhs);
                            //This way to ensure backward compatibility of the eclcrc
                            $$.setExpr(createSelectExpr(getActiveTableSelector(), LINK(field)));
                            $$.setPosition($1);
                        }
    ;

fieldSelectedFromRecord
    : recordScope VALUE_ID leaveScope
                        {
                            $$.setExpr(parser->createSelect($1.getExpr(), $2.getExpr(), $1), $1);
                        }
    | recordScope DATASET_ID leaveScope
                        {
                            $$.setExpr(parser->createSelect($1.getExpr(), $2.getExpr(), $1), $1);
                        }
    | recordScope startPointerToMember leaveScope VALUE_ID_REF endPointerToMember
                        {
                            $$.setExpr(parser->createIndirectSelect($1.getExpr(), $4.getExpr(), $1), $1);
                        }
    ;

optMaxMin
    :                   { $$.setNullExpr(); }
    | ',' MAX           {
                            $$.setExpr(createAttribute(maxAtom));
                            $$.setPosition($1);
                        }
    | ',' MIN           {
                            $$.setExpr(createAttribute(minAtom));
                            $$.setPosition($1);
                        }
    ;

beginCounterScope
    :                       
                        { 
                            parser->counterStack.append(* new OwnedHqlExprItem); 
                            $$.clear();
                        }
    ;

endCounterScope
    :                   {
                            $$.setNullExpr();
                            if (parser->counterStack.ordinality())
                            {
                                $$.setExpr(parser->counterStack.tos().value.getClear());
                                parser->counterStack.pop();
                            }
                        }
    ;

optAscDesc
    :                   { $$.setNullExpr(); }
    | ',' DESC          { $$.setExpr(createAttribute(descAtom)); }
    ;

optExtraFilter
    :                   { $$.setNullExpr(); }
    | ',' booleanExpr   { $$.setExpr($2.getExpr()); }
    ;

regexOpt
    :                   { $$.setNullExpr(); }
    | ',' NOCASE        { $$.setExpr(createAttribute(noCaseAtom)); }
    ;

xmlEncodeFlags
    :                   { $$.setNullExpr(); }
    | ',' ALL           { $$.setExpr(createAttribute(allAtom)); }
    ;

aggregateFlags
    :                   { $$.setNullExpr(); }
    | ',' KEYED         { $$.setExpr(createAttribute(keyedAtom)); $$.setPosition($2); }
    | ',' prefetchAttribute
                        {
                            $$.setExpr($2.getExpr(), $2);
                        }
    ;

transfer
    : TYPE_LPAREN typeDef TYPE_RPAREN 
                        { $$ = $2; }
    ;

atomicValue
    : qualifiedFieldName
    | const             { $$.setExpr($1.getExpr()); }
    ;

moduleScopeDot
    : abstractModule '.'
                        {
                            OwnedHqlExpr expr = $1.getExpr();
                            parser->modScope.set(expr->queryScope());
                            $$.setExpr(parser->checkConcreteModule($1, expr));
                            $$.setPosition($1);
                        }
    | pseudoResolutionScope '.'
                        {
                            $$.setExpr(createNullScope());
                        }
    ;

pseudoResolutionScope
    : '^'               {
                            parser->outerScopeAccessDepth = 1;
                            $$.clear();
                        }
    | pseudoResolutionScope '^'
                        {
                            ++parser->outerScopeAccessDepth;
                            $$.clear();
                        }
    ;

abstractModule
    :   SCOPE_ID        {
                            IHqlExpression *expr = $1.getExpr();
                            $$.setExpr(expr);
                        }
    | moduleScopeDot SCOPE_ID leaveScope
                        {
                            $1.release();
                            IHqlExpression *expr = $2.getExpr();
                            $$.setExpr(expr, $2);
                        }
    | '$'
                        {
                            IHqlExpression * scopeExpr = queryExpression(parser->globalScope);
                            $$.setExpr(LINK(scopeExpr), $1);
                        }
    | VALUE_MACRO abstractModule ENDMACRO
                        {
                            $$.setExpr($2.getExpr());
                        }
    | moduleScopeDot VALUE_MACRO leaveScope abstractModule ENDMACRO
                        {
                            $1.release();
                            $$.setExpr($4.getExpr(), $4);
                        }
    |   scopeFunctionWithParameters
                        {
                            OwnedHqlExpr value = $1.getExpr();
                            IHqlExpression * func;
                            IHqlExpression * params = NULL;
                            if (value->getOperator() == no_comma)
                            {
                                func = value->queryChild(0);
                                params = value->queryChild(1);
                            }
                            else
                                func = value;
                            IHqlExpression * expr = parser->bindParameters($1, func, params);
                            $$.setExpr(expr, $1);
                        }
    | STORED '(' abstractModule ')'
                        {
                            OwnedHqlExpr scope = $3.getExpr();
                            OwnedHqlExpr storedScope = parser->createStoredModule($3, scope);
                            $$.setExpr(storedScope.getClear());
                            $$.setPosition($1);
                        }
    | compoundModule
    | LIBRARY '(' libraryName ',' scopeFunction ','
                        {
                            parser->beginFunctionCall($5);
                        }
    actualParameters ')'
                        {
                            //Need to create a library definition from referenced attribute, adding the name/internal attribute
                            //and then bind it to create the library instance.
                            OwnedHqlExpr name = $3.getExpr();
                            OwnedHqlExpr func = $5.getExpr();
                            HqlExprArray actuals;
                            $8.unwindCommaList(actuals);
                            $$.setExpr(parser->createLibraryInstance($1, name, func, actuals));
                            $$.setPosition($1);
                        }
    | LIBRARY '(' libraryName ',' scopeFunctionWithParameters ')'
                        {
                            OwnedHqlExpr value = $5.getExpr();
                            IHqlExpression * func;
                            HqlExprArray actuals;
                            if (value->getOperator() == no_comma)
                            {
                                func = value->queryChild(0);
                                value->queryChild(1)->unwindList(actuals, no_comma);
                            }
                            else
                                func = value;

                            //Need to create a library definition from referenced attribute, adding the name/internal attribute
                            //and then bind it to create the library instance.
                            OwnedHqlExpr name = $3.getExpr();
                            $$.setExpr(parser->createLibraryInstance($1, name, func, actuals));
                            $$.setPosition($1);
                        }
    | startCompoundExpression beginInlineFunctionToken optDefinitions RETURN abstractModule ';' endInlineFunctionToken
                        {
                            Owned<ITypeInfo> retType = $1.getType();
                            $$.setExpr(parser->leaveLamdaExpression($5), $7);
                        }
    | IF '(' booleanExpr ',' abstractModule ',' abstractModule ')'
                        {
                            OwnedHqlExpr trueExpr = $5.getExpr();
                            OwnedITypeInfo scopeType = trueExpr->getType();  // actually needs to be the common base class.
                            OwnedHqlExpr module = createValue(no_if, scopeType.getClear(), $3.getExpr(), LINK(trueExpr), $7.getExpr());
                            $$.setExpr(createDelayedScope(module.getClear()), $1);
                        }
    ;

scopeFunctionWithParameters
    :   scopeFunction '('
                        {
                            parser->beginFunctionCall($1);
                        }
    actualParameters ')'
                        {
                            //Slightly ugly that we need this production to common up the code
                            //but otherwise we get s/r errors
                            OwnedHqlExpr parms = $4.getExpr();
                            //NB: Do not call createComma() incase the first argument is a dataset
                            if (parms)
                                $$.setExpr(createValue(no_comma, $1.getExpr(), parms.getClear()), $1);
                            else
                                $$.setExpr($1.getExpr(), $1);
                        }
    ;

libraryName
    : expression
                        {
                            parser->normalizeExpression($1, type_string, false);
                            //default name of library implementation name
                            $$.setExpr(createExprAttribute(nameAtom, $1.getExpr()));
                        }
    | INTERNAL '(' scopeFunction ')'
                        {
                            //want to create a name based on the name of the scope reference, but one that will be commmoned up between all
                            //internal instances of the same library.
                            OwnedHqlExpr internal = $3.getExpr();
                            IAtom * name = internal->queryName();
                            StringBuffer nameText;
                            nameText.append("lib").append(name).append("_").append(getExpressionCRC(internal));
                            OwnedHqlExpr nameExpr = createExprAttribute(nameAtom, createConstant(nameText.str()));
                            $$.setExpr(createComma(nameExpr.getClear(), createExprAttribute(internalAtom, internal.getClear()), createAttribute(_original_Atom, createAttribute(name))));
                        }
    ;

leaveScope
    :                   {
                            parser->dotScope.clear();
                            parser->modScope.clear();
                            parser->outerScopeAccessDepth = 0;
                            $$.clear();
                        }
    ;

scopeProjectOpts
    :                   {   $$.setNullExpr(); }
    | scopeProjectOpts ',' scopeProjectOpt
                        {
                            $$.setExpr(createComma($1.getExpr(), $3.getExpr()));
                            $$.setPosition($2);
                        }
    ;

scopeProjectOpt
    : OPT               {   $$.setExpr(createAttribute(optAtom)); $$.setPosition($1); }
    | UNKNOWN_ID            // Will include known ids as well since they won't be returned as known ids.
                        {
                            $$.setExpr(createId($1.getId()));
                            $$.setPosition($1);
                        }
    ;


qualifiedFieldName
    : dotScope VALUE_ID leaveScope
                        {
                            IHqlExpression *e1 = $1.getExpr();
                            IHqlExpression *e3 = $2.getExpr();
                            assertex(e1 && e1->getOperator() != no_record);

                            if (e3->getOperator() == no_field)
                                $$.setExpr(parser->createSelect(e1, e3, $2), $1);
                            else
                            {
                                e1->Release();      // some error occurred elsewhere
                                $$.setExpr(e3, $1);
                            }
                        }
    | dotScope startPointerToMember leaveScope VALUE_ID_REF endPointerToMember
                        {
                            IHqlExpression *e1 = $1.getExpr();
                            IHqlExpression *e3 = $4.getExpr();
                            $$.setExpr(parser->createIndirectSelect(e1, e3, $4), $1);
                        }
    | globalValueAttribute
    ;

globalValueAttribute
    : VALUE_ID
    | startPointerToMember VALUE_ID_REF endPointerToMember
                        {
                            //This means look id up in the current top scope.  It doesn't make sense if there is no active dataset
                            OwnedHqlExpr rhs = $2.getExpr();
                            IHqlExpression *top = parser->queryTopScope();
                            if (top && top->queryRecord())
                            {
                                $$.setExpr(parser->createIndirectSelect(LINK(top), rhs.getClear(), $1));
                            }
                            else
                            {
                                IIdAtom * name = parser->createFieldNameFromExpr(rhs);
                                const char * text = name ? str(name) : "?";
                                parser->reportError(ERR_OBJ_NOACTIVEDATASET, $1, "No active dataset to resolve field '%s'", text);
                                $$.setExpr(createNullExpr(rhs));
                            }
                        }
    | moduleScopeDot VALUE_ID leaveScope
                        {
                            $1.release();
                            $$.setExpr($2.getExpr(), $1);
                        }
    | VALUE_MACRO expression ENDMACRO 
                        { 
                            $$.setExpr($2.getExpr(), $1);
                        }
    | moduleScopeDot VALUE_MACRO leaveScope expression ENDMACRO
                        {
                            $1.release();
                            $$.setExpr($4.getExpr(), $1);
                        }
    | valueFunction '('
                        {
                            parser->beginFunctionCall($1);
                        }
    actualParameters ')'
                        {
                            $$.setExpr(parser->bindParameters($1, $4.getExpr()));
                        }
    ;

dataRow
    : dataSet '[' expression ']'
                        {
                            IHqlExpression * expr = $3.queryExpr();
                            if (expr->isConstant() && !expr->queryType()->isInteger())
                                parser->reportWarning(CategoryMistake, WRN_INT_OR_RANGE_EXPECTED, $3.pos, "Floating point index used. Was an index range intended instead?");
                            parser->normalizeExpression($3, type_int, false);
                            $$.setExpr(createRow(no_selectnth, $1.getExpr(), $3.getExpr()));
                        }
    | dictionary '[' expressionList ']'
                        {
                            HqlExprArray args;
                            $3.unwindCommaList(args);
                            OwnedHqlExpr dict = $1.getExpr();
                            OwnedHqlExpr row = createValue(no_rowvalue, makeNullType(), args);
                            $$.setExpr(createSelectMapRow(*parser->errorHandler, $3.pos, dict, row));
                        }
    | dataSet '[' NOBOUNDCHECK expression ']'
                        {   
                            parser->normalizeExpression($4, type_int, false);
                            $$.setExpr(createRow(no_selectnth, $1.getExpr(), createComma($4.getExpr(), createAttribute(noBoundCheckAtom))));
                        }
    | dotScope DATAROW_ID leaveScope
                        {
                            IHqlExpression *e1 = $1.getExpr();
                            IHqlExpression *e2 = $2.getExpr();
                            $$.setExpr(parser->createSelect(e1, e2, $2));
                        }
    | dotScope RECORD_ID leaveScope
                        {
                            IHqlExpression *e1 = $1.getExpr();
                            IHqlExpression *e2 = $2.getExpr();
                            $$.setExpr(parser->createSelect(e1, e2, $2));
                        }
    | moduleScopeDot DATAROW_ID leaveScope
                        {
                            $1.release();
                            $$.setExpr($2.getExpr());
                        }
    | datarowFunction '('
                        {
                            parser->beginFunctionCall($1);
                        }
    actualParameters ')'
                        {
                            $$.setExpr(parser->bindParameters($1, $4.getExpr()));
                        }
    | simpleDataRow
    | VALUE_MACRO dataRow ENDMACRO
                        {
                            $$.setExpr($2.getExpr());
                            $$.setPosition($1);
                        }
    | moduleScopeDot VALUE_MACRO leaveScope dataRow ENDMACRO
                        {
                            $1.release();
                            $$.setExpr($4.getExpr());
                            $$.setPosition($2);
                        }
    | startCompoundExpression beginInlineFunctionToken optDefinitions RETURN dataRow ';' endInlineFunctionToken
                        {
                            Owned<ITypeInfo> retType = $1.getType();
                            $$.setExpr(parser->leaveLamdaExpression($5), $7);
                        }
    ;

simpleDataRow
    : DATAROW_ID
    | LEFT              {
                            $$.setExpr(parser->getSelector($1, no_left), $1);
                        }
    | RIGHT             {
                            $$.setExpr(parser->getSelector($1, no_right), $1);
                        }
    | RIGHT_NN
                        {
                            //Slightly bizarre syntax - really only here for the merge transform of a user defined aggregate
                            IHqlExpression *right = parser->queryRightScope();
                            if (right)
                            {
                                OwnedHqlExpr selSeq = parser->getSelectorSequence();
                                OwnedHqlExpr selector = createSelector(no_right, right, selSeq);
                                OwnedHqlExpr rows = parser->resolveRows($1, selector);
                                OwnedHqlExpr index = createConstant(createIntValue($1.getInt(), LINK(parser->defaultIntegralType)));
                                $$.setExpr(createRow(no_selectnth, rows.getClear(), index.getClear()), $1);
                            }
                            else
                            {
                                parser->reportError(ERR_RIGHT_ILL_HERE, $1, "RIGHT not legal here");
                                $$.setExpr(createRow(no_null, LINK(queryNullRecord())), $1);
                            }
                        }
    | IF '(' booleanExpr ',' dataRow ',' dataRow ')'
                        {
                            $$.setExpr(parser->processIfProduction($3, $5, &$7), $1);
                        }
    | IF '(' booleanExpr ',' dataRow ')'
                        {
                            $$.setExpr(parser->processIfProduction($3, $5, NULL), $1);
                        }
    | IFF '(' booleanExpr ',' dataRow ',' dataRow ')'
                        {
                            parser->ensureDataset($5);
                            parser->ensureDataset($7);
                            OwnedHqlExpr ds = parser->processIfProduction($3, $5, &$7);
                            $$.setExpr(createRow(no_selectnth, ds.getClear(), getSizetConstant(1)), $1);
                        }
    | IFF '(' booleanExpr ',' dataRow ')'
                        {
                            parser->ensureDataset($5);
                            OwnedHqlExpr ds = parser->processIfProduction($3, $5, NULL);
                            $$.setExpr(createRow(no_selectnth, ds.getClear(), getSizetConstant(1)), $1);
                        }
    | HTTPCALL '(' expression ',' expression ',' expression ',' recordDef ')'
                        {
                            parser->normalizeExpression($3);
                            parser->normalizeExpression($5);
                            parser->normalizeExpression($7);
                            IHqlExpression * ds = createDataset(no_httpcall, $3.getExpr(), createComma($5.getExpr(), $7.getExpr(), $9.getExpr()));
                            $$.setExpr(createRow(no_selectnth, ds, createConstantOne()));
                        }
    | HTTPCALL '(' expression ',' expression ',' expression ',' recordDef ',' soapFlags ')'
                        {
                            parser->normalizeExpression($3);
                            parser->normalizeExpression($5);
                            parser->normalizeExpression($7);
                            IHqlExpression * ds = createDataset(no_httpcall, $3.getExpr(), createComma($5.getExpr(), $7.getExpr(), $9.getExpr(), $11.getExpr()));
                            $$.setExpr(createRow(no_selectnth, ds, createConstantOne()));
                        }
    | SOAPCALL '(' expression ',' expression ',' recordDef ',' recordDef ')'
                        {
                            parser->normalizeExpression($3);
                            parser->checkSoapRecord($7);
                            IHqlExpression * ds = createDataset(no_soapcall, $3.getExpr(), createComma($5.getExpr(), $7.getExpr(), $9.getExpr()));
                            $$.setExpr(createRow(no_selectnth, ds, createConstantOne()));
                        }
    | SOAPCALL '(' expression ',' expression ',' recordDef ',' recordDef ',' soapFlags ')'
                        {
                            parser->normalizeExpression($3);
                            parser->normalizeExpression($5);
                            parser->checkSoapRecord($7);
                            IHqlExpression * ds = createDataset(no_soapcall, $3.getExpr(), createComma($5.getExpr(), $7.getExpr(), $9.getExpr(), $11.getExpr()));
                            $$.setExpr(createRow(no_selectnth, ds, createConstantOne()));
                            parser->checkOnFailRecord($$.queryExpr(), $1);
                        }
    | SOAPCALL '(' expression ',' expression ',' recordDef ',' transform ',' recordDef ')'
                        {
                            parser->normalizeExpression($3);
                            parser->normalizeExpression($5);
                            IHqlExpression * ds = createDataset(no_newsoapcall, $3.getExpr(), createComma($5.getExpr(), $7.getExpr(), $9.getExpr(), $11.getExpr()));
                            $$.setExpr(createRow(no_selectnth, ds, createConstantOne()));
                        }
    | SOAPCALL '(' expression ',' expression ',' recordDef ',' transform ',' recordDef ',' soapFlags ')'
                        {
                            parser->normalizeExpression($3);
                            parser->normalizeExpression($5);
                            IHqlExpression * ds = createDataset(no_newsoapcall, $3.getExpr(), createComma($5.getExpr(), $7.getExpr(), createComma($9.getExpr(), $11.getExpr(), $13.getExpr())));
                            $$.setExpr(createRow(no_selectnth, ds, createConstantOne()));
                            parser->checkOnFailRecord($$.queryExpr(), $1);
                        }
    | ROW '(' inlineDatasetValue ',' recordDef ')'
                        {
                            OwnedHqlExpr row = createRow(no_temprow, $3.getExpr(), $5.getExpr());
                            $$.setExpr(convertTempRowToCreateRow(*parser->errorHandler, $3.pos, row));
                            $$.setPosition($1);
                        }
    | ROW '(' startLeftSeqRow ',' recordDef ')' endSelectorSequence
                        {
                            OwnedHqlExpr row = $3.getExpr();
                            OwnedHqlExpr record = $5.getExpr();
                            $7.release();
                            OwnedHqlExpr transform = parser->createDefaultAssignTransform(record, row, $5);
                            $$.setExpr(createRow(no_createrow, transform.getClear()), $1);
                        }
    | ROW '(' startLeftSeqRow ',' transform ')' endSelectorSequence
                        {
                            $$.setExpr(parser->createProjectRow($3, $5, $7), $1);
                        }
    | ROW '(' transform ')'
                        {
                            OwnedHqlExpr transform = $3.getExpr();
                            $$.setExpr(createRow(no_createrow, transform.getClear()));
                            $$.setPosition($1);
                        }
    | ROW '(' simpleRecord ')'
                        {
                            parser->checkSoapRecord($3);
                            OwnedHqlExpr record = $3.getExpr();
                            $$.setExpr(createRow(no_createrow, convertRecordToTransform(record, false)));
                            $$.setPosition($1);
                        }
    | ROW '(' dataSet ')'
                        {
                            OwnedHqlExpr ds = $3.getExpr();
                            $$.setExpr(ensureActiveRow(ds));
                            $$.setPosition($1);
                        }
    | ROW '(' '[' ']' ',' simpleRecord ')'
                        {
                            OwnedHqlExpr record = $6.getExpr();
                            OwnedHqlExpr transform = parser->createClearTransform(record, $7);
                            $$.setExpr(createRow(no_createrow, LINK(transform)));
                            $$.setPosition($1);
                        }
    | PROJECT '(' startLeftSeqRow ',' transform ')' endSelectorSequence
                        {
                            $$.setExpr(parser->createProjectRow($3, $5, $7), $1);
                        }
    | GLOBAL '(' dataRow globalOpts ')'
                        {
                            $$.setExpr(createRow(no_globalscope, $3.getExpr(), $4.getExpr()));
                            $$.setPosition($1);
                        }
    | GLOBAL '(' dataRow ',' expression globalOpts ')'
                        {
                            parser->normalizeExpression($5, type_string, false);
                            $$.setExpr(createRow(no_globalscope, $3.getExpr(), createComma($5.getExpr(), $6.getExpr())));
                            $$.setPosition($1);
                        }
    | NOFOLD '(' dataRow ')'
                        {
                            $$.setExpr(createRow(no_nofold, $3.getExpr(), NULL));
                            $$.setPosition($1);
                        }
    | NOHOIST '(' dataRow ')'
                        {
                            $$.setExpr(createRow(no_nohoist, $3.getExpr(), NULL));
                            $$.setPosition($1);
                        }
    | LOCAL '(' dataRow ')'
                        {
                            $$.setExpr(createRow(no_forcelocal, $3.getExpr()));
                            $$.setPosition($1);
                        }
    | NOLOCAL '(' dataRow ')'
                        {
                            $$.setExpr(createRow(no_forcenolocal, $3.getExpr()));
                            $$.setPosition($1);
                        }
    | ALLNODES '(' beginList dataRow ignoreDummyList ')'
                        {
                            $$.setExpr(createRow(no_allnodes, $4.getExpr()));
                            $$.setPosition($1);
                        }
    | THISNODE '(' dataRow ')'
                        {
                            $$.setExpr(createRow(no_thisnode, $3.getExpr()));
                            $$.setPosition($1);
                        }
    | TRANSFER '(' expression ',' recordDef ')'
                        {
                            parser->normalizeExpression($3);
                            IHqlExpression *expr = $3.getExpr();
                            IHqlExpression *record = $5.getExpr();
//                          if ((exprType->getSize() != UNKNOWN_LENGTH) && (exprType->getSize() < type->getSize()) && type->getSize() != UNKNOWN_LENGTH)
//                              parser->reportError(ERR_TYPETRANS_LARGERTYPE, $5, "Type transfer: target type in is larger than source type");
                            $$.setExpr(createRow(no_typetransfer, record, expr));
                            $$.setPosition($1);
                        }
    | __COMMON__ '(' dataRow ')'
                        {
                            $$.setExpr(createAliasOwn($3.getExpr(), NULL));
                            $$.setPosition($1);
                        }
    | SKIP '(' ROW recordDef ')'
                        {
                            if (!parser->curTransform)
                                parser->reportError(ERR_PARSER_CANNOTRECOVER,$1,"SKIP is only valid inside a TRANSFORM");
                            $$.setExpr(createRow(no_skip, $4.getExpr()));
                        }
    | MATCHROW '(' patternReference ')'
                        {
                            IHqlExpression * record = $3.queryExpr()->queryRecord();
                            if (!record)
                            {
                                parser->reportError(ERR_NOT_ROW_RULE,$1,"Referenced rule does not have a associated row");
                                record = queryNullRecord();
                            }

                            $$.setExpr(createRow(no_matchrow, LINK(record), $3.getExpr())); //, parser->createUniqueId())));
                        }
    | FROMXML '(' recordDef ',' expression optCommaTrim ')'
                        {
                            parser->normalizeExpression($5, type_stringorunicode, false);
                            $$.setExpr(createRow(no_fromxml, $3.getExpr(), createComma($5.getExpr(), $6.getExpr())), $1);
                        }
    | FROMJSON '(' recordDef ',' expression optCommaTrim ')'
                        {
                            parser->normalizeExpression($5, type_stringorunicode, false);
                            $$.setExpr(createRow(no_fromjson, $3.getExpr(), createComma($5.getExpr(), $6.getExpr())), $1);
                        }
    | WHEN '(' dataRow ',' action sideEffectOptions ')'
                        {
                            OwnedHqlExpr options = $6.getExpr();
                            if (options)
                                $$.setExpr(createRow(no_executewhen, $3.getExpr(), createComma($5.getExpr(), options.getClear())), $1);
                            else
                                $$.setExpr(createCompound($5.getExpr(), $3.getExpr()), $1);
                        }
    ;

dictionary
    : simpleDictionary
    | dictionary '+' dictionary
                        {   parser->createAppendDictionaries($$, $1, $3, NULL);    }
    ;

simpleDictionary
    : scopedDictionaryId
    | NOFOLD '(' dictionary ')'
                        {
                            $$.setExpr(createDictionary(no_nofold, $3.getExpr(), NULL));
                            $$.setPosition($1);
                        }
    | NOHOIST '(' dictionary ')'
                        {
                            $$.setExpr(createDictionary(no_nohoist, $3.getExpr(), NULL));
                            $$.setPosition($1);
                        }
    | THISNODE '(' dictionary ')'
                        {
                            $$.setExpr(createDictionary(no_thisnode, $3.getExpr()), $1);
                        }
    | DICTIONARY '(' startTopFilter ',' recordDef ')' endTopFilter
                        {
                            OwnedHqlExpr dataset = $3.getExpr();
                            parser->checkOutputRecord($5, false);
                            OwnedHqlExpr record = $5.getExpr();
                            HqlExprArray args;
                            args.append(*LINK(dataset));
                            args.append(*LINK(record));
                            OwnedHqlExpr ds = createDataset(no_usertable, args);
                            parser->checkProjectedFields(ds, $5);
                            $$.setExpr(createDictionary(no_createdictionary, ds.getClear()), $1);
                        }
    | DICTIONARY '(' startTopFilter ')' endTopFilter
                        {
                            OwnedHqlExpr ds = $3.getExpr();
                            $$.setExpr(createDictionary(no_createdictionary, ds.getClear()), $1);
                        }
    | DICTIONARY '(' '[' ']' ',' recordDef ')'
                        {
                            HqlExprArray values;  // Empty list
                            OwnedHqlExpr table = createDataset(no_temptable, createValue(no_recordlist, NULL, values), $6.getExpr());
                            OwnedHqlExpr ds = convertTempTableToInlineTable(*parser->errorHandler, $4.pos, table);
                            $$.setExpr(createDictionary(no_createdictionary, ds.getClear()), $1);
                        }
    | DICTIONARY '(' '[' beginList inlineDatasetValueList ']' ',' recordDef ')'
                        {
                            HqlExprArray values;
                            parser->endList(values);
                            OwnedHqlExpr table = createDataset(no_temptable, createValue(no_recordlist, NULL, values), $8.getExpr());
                            OwnedHqlExpr ds = convertTempTableToInlineTable(*parser->errorHandler, $5.pos, table);
                            $$.setExpr(createDictionary(no_createdictionary, ds.getClear()), $1);
                        }
    | '(' dictionary  ')'  {
                            $$.setExpr($2.getExpr());
                            $$.setPosition($1);
                        }
    | IF '(' booleanExpr ',' dictionary ',' dictionary ')'
                        {
                            OwnedHqlExpr ds = parser->processIfProduction($3, $5, &$7);
                            $$.setExpr(ds.getClear(), $1);
                        }
    | IF '(' booleanExpr ',' dictionary ')'
                        {
                            OwnedHqlExpr ds = parser->processIfProduction($3, $5, NULL);
                            $$.setExpr(ds.getClear(), $1);
                        }
    | IFF '(' booleanExpr ',' dictionary ',' dictionary ')'
                        {
                            OwnedHqlExpr ds = parser->processIfProduction($3, $5, &$7);
                            $$.setExpr(ds.getClear(), $1);
                        }
    | IFF '(' booleanExpr ',' dictionary ')'
                        {
                            OwnedHqlExpr ds = parser->processIfProduction($3, $5, NULL);
                            $$.setExpr(ds.getClear(), $1);
                        }
    | MAP '(' mapDictionarySpec ',' dictionary ')'
                        {
                            HqlExprArray args;
                            IHqlExpression * elseDict = $5.getExpr();
                            $3.unwindCommaList(args);
                            ForEachItemIn(idx, args)
                            {
                                IHqlExpression * cur = args.item(idx).queryChild(1);
                                parser->checkRecordTypesMatch(cur, elseDict, $5);
                            }
                            args.append(*elseDict);
                            $$.setExpr(::createDictionary(no_map, args));
                            $$.setPosition($1);
                        }
    | MAP '(' mapDictionarySpec ')'
                        {
                            HqlExprArray args;
                            $3.unwindCommaList(args);
                            IHqlExpression * elseDict;
                            if (args.ordinality())
                                elseDict = createNullExpr(&args.item(0));
                            else
                                elseDict = createDictionary(no_null, LINK(queryNullRecord()));
                            ForEachItemIn(idx, args)
                            {
                                IHqlExpression * cur = args.item(idx).queryChild(1);
                                parser->checkRecordTypesMatch(cur, elseDict, $1);
                            }
                            args.append(*elseDict);
                            $$.setExpr(::createDictionary(no_map, args));
                            $$.setPosition($1);
                        }
    | CASE '(' expression ',' beginList caseDictionarySpec ',' dictionary ')'
                        {
                            parser->normalizeExpression($3, type_scalar, false);
                            HqlExprArray args;
                            IHqlExpression * elseDict = $8.getExpr();
                            parser->endList(args);
                            parser->checkCaseForDuplicates(args, $6);
                            ForEachItemIn(idx, args)
                            {
                                IHqlExpression * cur = args.item(idx).queryChild(1);
                                parser->checkRecordTypesMatch(cur, elseDict, $8);
                            }
                            args.add(*$3.getExpr(),0);
                            args.append(*elseDict);
                            $$.setExpr(::createDataset(no_case, args));
                            $$.setPosition($1);
                        }
    | CASE '(' expression ',' beginList caseDictionarySpec ')'
                        {
                            parser->normalizeExpression($3, type_scalar, false);
                            HqlExprArray args;
                            parser->endList(args);
                            IHqlExpression * elseDict;
                            if (args.ordinality())
                                elseDict = createNullExpr(&args.item(0));
                            else
                                elseDict = createDictionary(no_null, LINK(queryNullRecord()));
                            parser->checkCaseForDuplicates(args, $6);
                            ForEachItemIn(idx, args)
                            {
                                IHqlExpression * cur = args.item(idx).queryChild(1);
                                parser->checkRecordTypesMatch(cur, elseDict, $6);
                            }
                            args.add(*$3.getExpr(),0);
                            args.append(*elseDict);
                            $$.setExpr(::createDictionary(no_case, args));
                            $$.setPosition($1);
                        }
    | CASE '(' expression ',' beginList dictionary ')'
                        {
                            parser->normalizeExpression($3, type_scalar, false);
                            // change error to warning.
                            parser->reportWarning(CategoryUnusual, WRN_CASENOCONDITION, $1.pos, "CASE does not have any conditions");
                            HqlExprArray list;
                            parser->endList(list);
                            $3.release();
                            $$.setExpr($6.getExpr(), $1);
                        }
    | FAIL '(' dictionary failDatasetParam ')'
                        {
                            OwnedHqlExpr dict = $3.getExpr();
                            //Actually allow a sequence of arbitrary actions....
                            $$.setExpr(createDictionary(no_fail, LINK(dict->queryRecord()), $4.getExpr()));
                            $$.setPosition($1);
                        }
    | TOK_ERROR '(' dictionary failDatasetParam ')'
                        {
                            OwnedHqlExpr dict = $3.getExpr();
                            //Actually allow a sequence of arbitrary actions....
                            $$.setExpr(createDictionary(no_fail, LINK(dict->queryRecord()), $4.getExpr()));
                            $$.setPosition($1);
                        }
    | CHOOSE '(' expression ',' dictionaryList ')'
                        {
                            parser->normalizeExpression($3, type_int, false);
                            OwnedHqlExpr values = $5.getExpr();
                            HqlExprArray args;
                            values->unwindList(args, no_comma);

                            IHqlExpression * compareDict = NULL;
                            ForEachItemIn(idx, args)
                            {
                                IHqlExpression * cur = &args.item(idx);
                                if (cur->queryRecord())
                                {
                                    if (compareDict)
                                    {
                                        parser->checkRecordTypesMatch(cur, compareDict, $5);
                                    }
                                    else
                                        compareDict = cur;
                                }
                            }

                            args.add(*$3.getExpr(), 0);
                            $$.setExpr(createDictionary(no_chooseds, args), $1); // no_choosedict ?
                        }
    ;

dictionaryList
    : dictionary
    | dictionary ',' dictionaryList
                        {
                            $$.setExpr(createComma($1.getExpr(), $3.getExpr()));
                            $$.setPosition($1);
                        }
    ;


scopedDictionaryId
    : globalScopedDictionaryId
    | dotScope DICTIONARY_ID leaveScope
                        {
                            IHqlExpression *e1 = $1.getExpr();
                            IHqlExpression *e2 = $2.getExpr();
                            if (e1 && (e1->getOperator() != no_record) && (e2->getOperator() == no_field))
                                $$.setExpr(parser->createSelect(e1, e2, $2));
                            else
                            {
                                ::Release(e1);
                                $$.setExpr(e2);
                            }
                        }
    | dictionaryFunction '('
                        {
                            parser->beginFunctionCall($1);
                        }
    actualParameters ')'
                        {
                            $$.setExpr(parser->bindParameters($1, $4.getExpr()));
                        }
    ;

globalScopedDictionaryId
    : DICTIONARY_ID
    | moduleScopeDot DICTIONARY_ID leaveScope
                        {
                            OwnedHqlExpr scope = $1.getExpr();
                            $$.setExpr($2.getExpr());
                        }
    ;

dataSet
    : simpleDataSet
    | startCompoundExpression beginInlineFunctionToken optDefinitions RETURN dataSet ';' endInlineFunctionToken
                        {
                            Owned<ITypeInfo> retType = $1.getType();
                            $$.setExpr(parser->leaveLamdaExpression($5), $7);
                        }
    | startSimpleFilter conditions endSimpleFilter /* simple dataset with conditions */
                        {
                            IHqlExpression *filter = $2.getExpr();
                            IHqlExpression *dataset = $1.getExpr();
                            $$.setExpr(filter ? createDataset(no_filter, dataset, filter) : dataset, $1);
                        }
    | dataRow conditions 
                        {
                            /** Error production **/
                            parser->reportError(ERR_EXPECTED_DATASET, $1, "Expected a dataset instead of a row");
                            $2.release();
                            $$.setExpr(createDatasetFromRow($1.getExpr()), $1);
                        }
    | VALUE_MACRO dataSet ENDMACRO {  $$.setExpr($2.getExpr()); }
    | moduleScopeDot VALUE_MACRO leaveScope dataSet ENDMACRO
                        {
                            $1.release();
                            $$.setExpr($4.getExpr());
                        }
    | dataSet '-' dataSet
                        {
                            OwnedHqlExpr left = $1.getExpr();
                            OwnedHqlExpr right = $3.getExpr();
                            parser->checkRecordTypesSimilar(left, right, $3.pos);

                            OwnedHqlExpr seq = parser->createActiveSelectorSequence(left, right);
                            OwnedHqlExpr leftSelect = createSelector(no_left, left, seq);
                            OwnedHqlExpr rightSelect = createSelector(no_right, right, seq);

                            OwnedHqlExpr transform = parser->createDefaultAssignTransform(left->queryRecord(), leftSelect, $1);
                            OwnedHqlExpr cond = createBoolExpr(no_eq, LINK(leftSelect), LINK(rightSelect));
                            $$.setExpr(createDatasetF(no_join, LINK(left), LINK(right), cond.getClear(), transform.getClear(), LINK(seq), createAttribute(leftonlyAtom), NULL), $1);
                        }
/*
  The following would implement (X - Y) + (Y - X), but it would need a new operator since ^ is overloaded for the scope resolution
    | dataSet '^' dataSet
                        {
                            OwnedHqlExpr left = $1.getExpr();
                            OwnedHqlExpr right = $3.getExpr();
                            parser->checkRecordTypesSimilar(left, right, $3);

                            OwnedHqlExpr seq = parser->createActiveSelectorSequence(left, right);
                            OwnedHqlExpr leftSelect = createSelector(no_left, left, seq);
                            OwnedHqlExpr rightSelect = createSelector(no_right, right, seq);

                            OwnedHqlExpr transform = parser->createDefaultAssignTransform(left->queryRecord(), leftSelect, $1);
                            OwnedHqlExpr cond = createBoolExpr(no_eq, LINK(leftSelect), LINK(rightSelect));
                            $$.setExpr(createDatasetF(no_join, LINK(left), LINK(right), cond.getClear(), transform.getClear(), LINK(seq), createAttribute(fullonlyAtom), NULL), $1);
                        }
*/
    | dataSet '+' dataSet
                        {   parser->createAppendFiles($$, $1, $3, NULL);    }
    | dataSet '&' dataSet
                        {   parser->createAppendFiles($$, $1, $3, _ordered_Atom);   }
    | dataSet ANDAND dataSet
                        {   parser->createAppendFiles($$, $1, $3, _orderedPull_Atom);   }
    | dataSet '+' dataRow
                        {   parser->createAppendFiles($$, $1, $3, NULL);    }
    | dataSet '&' dataRow
                        {   parser->createAppendFiles($$, $1, $3, _ordered_Atom);   }
    | dataSet ANDAND dataRow
                        {   parser->createAppendFiles($$, $1, $3, _orderedPull_Atom);   }
    | dataRow '+' dataSet
                        {   parser->createAppendFiles($$, $1, $3, NULL);    }
    | dataRow '&' dataSet
                        {   parser->createAppendFiles($$, $1, $3, _ordered_Atom);   }
    | dataRow ANDAND dataSet
                        {   parser->createAppendFiles($$, $1, $3, _orderedPull_Atom);   }
    | dataRow '+' dataRow
                        {   parser->createAppendFiles($$, $1, $3, NULL);    }
    | dataRow '&' dataRow
                        {   parser->createAppendFiles($$, $1, $3, _ordered_Atom);   }
    | dataRow ANDAND dataRow
                        {   parser->createAppendFiles($$, $1, $3, _orderedPull_Atom);   }
    | dataSet '[' expression DOTDOT expression ']'
                        {
                            parser->normalizeExpression($3, type_int, false);
                            parser->normalizeExpression($5, type_int, false);
                            parser->applyDefaultPromotions($3, true);
                            parser->applyDefaultPromotions($5, true);
                            Owned<ITypeInfo> type = parser->promoteToSameType($3, $5);
                            IHqlExpression * ds = $1.getExpr();
                            IHqlExpression * from = $3.getExpr();
                            IHqlExpression * to = $5.getExpr();
                            IHqlExpression * length = createValue(no_add, LINK(type), createValue(no_sub, LINK(type), to, LINK(from)), createConstant(type->castFrom(true, (__int64)1)));
                            $$.setExpr(createDataset(no_choosen, ds, createComma(length, from)));
                        }
    | dataSet '[' DOTDOT expression ']'
                        {
                            parser->normalizeExpression($4, type_int, false);
                            IHqlExpression * ds = $1.getExpr();
                            IHqlExpression * length = $4.getExpr();
                            $$.setExpr(createDataset(no_choosen, ds, length));
                        }
    | dataSet '[' expression DOTDOT ']'
                        {
                            parser->normalizeExpression($3, type_int, false);
                            IHqlExpression * ds = $1.getExpr();
                            IHqlExpression * from = $3.getExpr();
                            $$.setExpr(createDataset(no_choosen, ds, createComma(createConstant(CHOOSEN_ALL_LIMIT), from)));
                        }
    ;

simpleDataSet
    : scopedDatasetId
    | setOfDatasets '[' expression ']'
                        {
                            parser->normalizeExpression($3, type_int, false);
                            $$.setExpr(createDataset(no_rowsetindex, $1.getExpr(), $3.getExpr()));
                            $$.setPosition($1);
                        }
    | ALIAS '(' dataSet ')'
                        {
                            $$.setExpr(createDataset(no_dataset_alias, $3.getExpr(), ::createUniqueId()), $1);
                        }
    | EBCDIC '(' startTopFilter ')' endTopFilter
                        {
                            IHqlExpression *ds = $3.getExpr();
                            OwnedHqlExpr ebcdicRecord = parser->transformRecord(ds, ebcdicAtom, $3);
                            if (ebcdicRecord)
                            {
                                OwnedHqlExpr seq = parser->createActiveSelectorSequence(ds, NULL);
                                OwnedHqlExpr left = createSelector(no_left, ds, seq);
                                OwnedHqlExpr transform = parser->createDefaultAssignTransform(ebcdicRecord, left, $1);
                                $$.setExpr(createDatasetF(no_transformebcdic, ds, transform.getClear(), LINK(seq), NULL));
                            }
                            else
                                $$.setExpr(ds);
                            $$.setPosition($1);
                        }
    | ASCII '(' startTopFilter ')' endTopFilter
                        {
                            IHqlExpression *ds = $3.getExpr();
                            OwnedHqlExpr asciiRecord = parser->transformRecord(ds, asciiAtom, $3);
                            if (asciiRecord)
                            {
                                OwnedHqlExpr seq = parser->createActiveSelectorSequence(ds, NULL);
                                OwnedHqlExpr left = createSelector(no_left, ds, seq);
                                OwnedHqlExpr transform = parser->createDefaultAssignTransform(asciiRecord, left, $1);
                                $$.setExpr(createDatasetF(no_transformascii, ds, transform.getClear(), LINK(seq), NULL));
                            }
                            else
                                $$.setExpr(ds);
                            $$.setPosition($1);
                        }
    | CHOOSEN '(' dataSet ',' expression choosenExtra ')'
                        {
                            if ($5.queryExpr()->getOperator() == no_all)
                                $5.release().setExpr(createConstant(CHOOSEN_ALL_LIMIT));
                            parser->normalizeExpression($5, type_int, false);
                            
                            IHqlExpression * limit = $5.getExpr();
                            if (limit->queryValue() && limit->queryValue()->getIntValue() == 0)
                                parser->reportWarning(CategoryUnusual, WRN_CHOOSEN_ALL,$1.pos,"Use CHOOSEN(dataset, ALL) to remove implicit choosen.  CHOOSEN(dataset, 0) now returns no records.");
                            $$.setExpr(createDataset(no_choosen, $3.getExpr(), createComma(limit, $6.getExpr())), $1);
                            parser->attachPendingWarnings($$);
                        }
    | CHOOSESETS '(' startTopFilter ',' setCountList ')' endTopFilter
                        {
                            $$.setExpr(createDataset(no_choosesets, $3.getExpr(), $5.getExpr()));
                            $$.setPosition($1);
                        }
    | DEDUP '(' startTopLeftRightSeqFilter optDedupFlags ')' endTopLeftRightFilter endSelectorSequence
                        {
                            IHqlExpression *ds = $3.getExpr();
                            parser->expandWholeAndExcept(ds, $4);

                            IHqlExpression *flags = $4.getExpr();
                            //checkDedup(ds, flags, $3);
                            OwnedHqlExpr dedup = createDataset(no_dedup, ds, createComma(flags, $7.getExpr()));

                            parser->checkDistribution($3, dedup, false);
                            bool hasHash = dedup->hasAttribute(hashAtom);
                            if (hasHash || dedup->hasAttribute(allAtom))
                            {
                                IHqlExpression * keep = dedup->queryAttribute(keepAtom);
                                if (keep && !matchesConstantValue(keep->queryChild(0), 1))
                                    parser->reportError(ERR_DEDUP_ALL_KEEP, $4, "KEEP is not supported for DEDUP(ALL)");
                            }
                            if (hasHash)
                            {
                                if (dedup->hasAttribute(rightAtom))
                                    parser->reportError(ERR_DEDUP_ALL_KEEP, $4, "RIGHT is not supported for DEDUP(HASH)");
                            }

                            $$.setExpr(dedup.getClear(), $1);
                        }
    | DISTRIBUTE '(' startTopFilter startDistributeAttrs ',' expression optDistributeAttrs ')' endTopFilter
                        {
                            parser->normalizeExpression($6, type_numeric, false);
                            $$.setExpr(createDataset(no_distribute, $3.getExpr(), createComma($6.getExpr(), $7.getExpr())));
                            $$.setPosition($1);
                        }
    | DISTRIBUTE '(' startTopFilter startDistributeAttrs optDistributeAttrs ')' endTopFilter
                        {
                            OwnedHqlExpr ds = $3.getExpr();
                            OwnedHqlExpr active = ensureActiveRow(ds);
                            //Expand row components (see parser->processSortList() for similar code for hash)
                            HqlExprArray components;
                            RecordSelectIterator iter(active->queryRecord(), active);
                            ForEach(iter)
                                components.append(*iter.get());
                            OwnedHqlExpr sortlist = createSortList(components);
                            OwnedHqlExpr hash = createValue(no_hash32, makeIntType(4, false), LINK(sortlist), createAttribute(internalAtom));
                            $$.setExpr(createDataset(no_distribute, ds.getClear(), createComma(hash.getClear(), $5.getExpr())), $1);
                        }
    | DISTRIBUTE '(' startTopFilter startDistributeAttrs ',' skewAttribute optDistributeAttrs ')' endTopFilter
                        {
                            $$.setExpr(createDataset(no_distribute, $3.getExpr(), createComma($6.getExpr(), $7.getExpr())));
                            $$.setPosition($1);
                        }
    | DISTRIBUTE '(' startTopFilter startDistributeAttrs ',' PARTITION_ATTR '(' beginList sortList ')' optDistributeAttrs ')' endTopFilter
                        {
                            HqlExprArray sortItems;
                            parser->endList(sortItems);
                            OwnedHqlExpr fields = parser->processSortList($9, no_distribute, NULL, sortItems, NULL, NULL);
                            HqlExprArray args;
                            unwindChildren(args, fields);
                            OwnedHqlExpr value = createValue(no_sortpartition, LINK(parser->defaultIntegralType), args);
                            $$.setExpr(createDatasetF(no_distribute, $3.getExpr(), value.getClear(), $11.getExpr(), NULL));
                            $$.setPosition($1);
                        }
    | DISTRIBUTE '(' startTopFilter startDistributeAttrs ',' startRightDistributeSeqFilter endTopFilter ',' expression optKeyedDistributeAttrs ')' endSelectorSequence
                        {
                            parser->normalizeExpression($9, type_boolean, false);
                            IHqlExpression * left = $3.getExpr();
                            IHqlExpression * right = $6.getExpr();
                            IHqlExpression * cond = $9.getExpr();
                            OwnedHqlExpr attr = $10.getExpr();

                            if (!isKey(right))
                                parser->reportError(ERR_EXPECTED_INDEX,$5,"Expected an index as the second parameter");

                            IHqlExpression * ds = createDataset(no_keyeddistribute, left, createComma(right, cond, LINK(attr), $12.getExpr()));

                            JoinSortInfo joinInfo(cond, NULL, NULL, NULL, NULL);
                            joinInfo.findJoinSortOrders(false);
                            unsigned numUnsortedFields = numPayloadFields(right);
                            if (joinInfo.extraMatch || (!ds->hasAttribute(firstAtom) && (joinInfo.queryLeftReq().ordinality() != getFieldCount(right->queryRecord())-numUnsortedFields)))
                                parser->reportError(ERR_MATCH_KEY_EXACTLY,$9,"Condition on DISTRIBUTE must match the key exactly");
                            if (joinInfo.hasOptionalEqualities())
                                parser->reportError(ERR_MATCH_KEY_EXACTLY,$9,"field[1..*] is not supported for a keyed distribute");

                            //Should check that all index fields are accounted for...
                            $$.setExpr(ds);
                            $$.setPosition($1);
                        }

    | DISTRIBUTE '(' startTopFilter startDistributeAttrs ',' startRightDistributeSeqFilter endTopFilter optKeyedDistributeAttrs ')' endSelectorSequence
                        {
                            IHqlExpression * left = $3.getExpr();
                            IHqlExpression * right = $6.getExpr();
                            if (!isKey(right))
                                parser->reportError(ERR_EXPECTED_INDEX,$6,"Expected an index as the second parameter");

                            IHqlExpression * cond = parser->createDistributeCond(left, right, $6, $10);

                            IHqlExpression * ds = createDataset(no_keyeddistribute, left, createComma(right, cond, $8.getExpr(), $10.getExpr()));
                            //Should check that all index fields are accounted for...

                            $$.setExpr(ds);
                            $$.setPosition($1);
                        }
    | DISTRIBUTED '(' startTopFilter ',' expression distributedFlags ')' endTopFilter
                        {
                            parser->normalizeExpression($5, type_numeric, false);
                            $$.setExpr(createDataset(no_distributed, $3.getExpr(), createComma($5.getExpr(), $6.getExpr())));
                            $$.setPosition($1);
                        }
    | DISTRIBUTED '(' startTopFilter ')' endTopFilter
                        {
                            $$.setExpr(createDataset(no_distributed, $3.getExpr(), createAttribute(unknownAtom)));
                            $$.setPosition($1);
                        }
    | PARTITION '(' startTopFilter ',' startSortOrder beginList sortList ')' endSortOrder endTopFilter
                        {
                            HqlExprArray sortItems;
                            parser->endList(sortItems);
                            OwnedHqlExpr fields = parser->processSortList($7, no_distribute, NULL, sortItems, NULL, NULL);
                            HqlExprArray args;
                            unwindChildren(args, fields);
                            OwnedHqlExpr value = createValue(no_sortpartition, LINK(parser->defaultIntegralType), args);
                            $$.setExpr(createDataset(no_distribute, $3.getExpr(), value.getClear()));
                            $$.setPosition($1);
                        }
    | JOIN '(' startLeftDelaySeqFilter ',' startRightFilter ',' expression beginCounterScope opt_join_transform_flags endCounterScope ')' endSelectorSequence
                        {
                            parser->normalizeExpression($7, type_boolean, false);

                            IHqlExpression * left = $3.getExpr();
                            IHqlExpression * right = $5.getExpr();
                            IHqlExpression * cond = $7.getExpr();
                            OwnedHqlExpr flags = $9.getExpr();
                            OwnedHqlExpr counter = $10.getExpr();
                            OwnedHqlExpr transform;
                            if (flags)
                            {
                                if (flags->getOperator() == no_comma)
                                {
                                    IHqlExpression * child0 = flags->queryChild(0);
                                    if (child0->isTransform())
                                    {
                                        transform.set(child0);
                                        flags.set(flags->queryChild(1));
                                    }
                                }
                                else if (flags->isTransform())
                                    transform.setown(flags.getClear());
                            }

                            if (!transform)
                            {
                                IHqlExpression * seq = $12.queryExpr();
                                transform.setown(parser->createDefJoinTransform(left,right,$1,seq,flags));
                            }

                            if (counter)
                                flags.setown(createComma(flags.getClear(), createAttribute(_countProject_Atom, counter.getClear())));

                            IHqlExpression *join = createDataset(no_join, left, createComma(right, cond, createComma(transform.getClear(), flags.getClear(), $12.getExpr())));

                            bool isLocal = join->hasAttribute(localAtom);
                            parser->checkDistribution($3, left, isLocal, true);
                            parser->checkDistribution($5, right, isLocal, true);
                            parser->checkJoinFlags($1, join);
                            parser->checkOnFailRecord(join, $1);

                            if (!join->hasAttribute(allAtom))
                                parser->warnIfFoldsToConstant(cond, $7);

                            $$.setExpr(join, $1);
                        }
    | MERGEJOIN '(' startTopLeftRightSeqSetDatasets ',' startLeftRows expression ',' endRowsGroup beginList sortList ')' endTopLeftRightFilter endSelectorSequence
                        {
                            HqlExprArray sortItems;
                            parser->endList(sortItems);
                            parser->normalizeExpression($6, type_boolean, false);
                            IHqlExpression * ds = $3.getExpr();
                            IHqlExpression * cond = $6.getExpr();
                            OwnedHqlExpr flags;
                            parser->expandSortedAsList(sortItems);
                            IHqlExpression * order = parser->processSortList($10, no_mergejoin, ds, sortItems, NULL, &flags);
                            IHqlExpression * join = createDataset(no_mergejoin, ds, createComma(cond, order, flags.getClear(), createComma($8.getExpr(), $13.getExpr())));
                            $$.setExpr(join);
                            $$.setPosition($1);
                        }
    | JOIN '(' startTopLeftRightSeqSetDatasets ',' startLeftRows expression ',' transform ',' mergeJoinFlags ')' endRowsGroup endTopLeftRightFilter endSelectorSequence
                        {
                            parser->normalizeExpression($6, type_boolean, false);
                            IHqlExpression * ds = $3.getExpr();
                            IHqlExpression * cond = $6.getExpr();
                            IHqlExpression * tform = $8.getExpr();
                            if (!queryAttributeInList(sortedAtom, $10.queryExpr()))
                                parser->reportError(ERR_EXPECTED, $10, "SORTED() is required for STEPPED join");

                            OwnedHqlExpr flags;
                            HqlExprArray sortItems;
                            $10.unwindCommaList(sortItems);
                            parser->expandSortedAsList(sortItems);
                            IHqlExpression * order = parser->processSortList($10, no_nwayjoin, ds, sortItems, NULL, &flags);
                            IHqlExpression * join = createDataset(no_nwayjoin, ds, createComma(cond, tform, order, createComma(flags.getClear(), $12.getExpr(), $14.getExpr())));
                            $$.setExpr(join);
                            $$.setPosition($1);
                        }
    | MERGE '(' startTopLeftRightSeqSetDatasets ',' beginList sortList ')' endTopLeftRightFilter endSelectorSequence
                        {
                            HqlExprArray sortItems;
                            parser->endList(sortItems);
                            if (!queryAttribute(sortedAtom, sortItems))
                                parser->reportWarning(CategoryDeprecated, WRN_MERGE_RECOMMEND_SORTED, $1.pos, "MERGE without an explicit SORTED() attribute is deprecated");

                            IHqlExpression * ds = $3.getExpr();
                            parser->expandSortedAsList(sortItems);
                            OwnedHqlExpr flags;
                            IHqlExpression * order = parser->processSortList($6, no_nwaymerge, ds, sortItems, NULL, &flags);
                            IHqlExpression * join = createDataset(no_nwaymerge, ds, createComma(order, flags.getClear(), $9.getExpr()));
                            $$.setExpr(join, $1);
                            parser->attachPendingWarnings($$);
                        }
    | PROCESS '(' startLeftDelaySeqFilter ',' startRightRow ',' beginCounterScope transform ',' transform optCommonAttrs ')' endCounterScope endSelectorSequence
                        {
                            IHqlExpression * left = $3.getExpr();
                            IHqlExpression * right = $5.getExpr();
                            IHqlExpression * counter = $13.getExpr();
                            IHqlExpression * attr = $11.getExpr();
                            if (counter)
                                attr = createComma(attr, createAttribute(_countProject_Atom, counter));
                            parser->ensureTransformTypeMatch($8, left);
                            parser->ensureTransformTypeMatch($10, right);
                            $$.setExpr(createDataset(no_process, left, createComma(right, $8.getExpr(), $10.getExpr(), createComma(attr, $14.getExpr()))));
                            $$.setPosition($1);
                        }
    | ROLLUP '(' startTopLeftRightSeqFilter ',' expression ',' transform optCommonAttrs ')' endTopLeftRightFilter endSelectorSequence
                        {
                            parser->normalizeExpression($5);
                            parser->ensureTransformTypeMatch($7, $3.queryExpr());

                            OwnedHqlExpr tr = $7.getExpr();
                            IHqlExpression *attr = $8.getExpr();
                            $$.setExpr(createDataset(no_rollup, $3.getExpr(), createComma($5.getExpr(), tr.getClear(), attr, $11.getExpr())));
                            parser->checkDistribution($3, $$.queryExpr(), false);
                            $$.setPosition($1);
                        }
    | ROLLUP '(' startTopLeftRightSeqFilter ',' transform rollupExtra ')' endTopLeftRightFilter endSelectorSequence
                        {
                            IHqlExpression *ds = $3.getExpr();
                            IHqlExpression *tr = $5.getExpr();
                            OwnedHqlExpr seq = $9.getExpr();

                            parser->expandWholeAndExcept(ds, $6);
                            OwnedHqlExpr extra = $6.getExpr();
                            HqlExprArray args;
                            if (extra)
                                extra->unwindList(args, no_comma);
                            IHqlExpression * attr = NULL;
                            IHqlExpression * cond = NULL;
                            OwnedHqlExpr left = createSelector(no_left, ds, seq);
                            OwnedHqlExpr right = createSelector(no_right, ds, seq);

                            HqlExprArray values;
                            ForEachItemIn(idx, args)
                            {
                                IHqlExpression & cur = args.item(idx);
                                if (cur.isAttribute())
                                    attr = createComma(attr, LINK(&cur));
                                else
                                    values.append(OLINK(cur));
                            }
                            if (values.ordinality())
                                cond = createSortList(values);
                            else
                                cond = createConstant(true);

                            if (!recordTypesMatch(ds, tr))
                                parser->reportError(ERR_TRANSFORM_TYPE_MISMATCH,$5,"Type returned from transform must match the source dataset type");
                            $$.setExpr(createDataset(no_rollup, ds, createComma(cond, tr, attr, LINK(seq))));
                            parser->checkDistribution($3, $$.queryExpr(), false);
                            $$.setPosition($1);
                        }
    | ROLLUP '(' startTopLeftRightSeqFilter ',' startLeftRowsGroup ',' transform ')' endRowsGroup endTopLeftRightFilter endSelectorSequence
                        {
                            parser->checkGrouped($3);
                            IHqlExpression *attr = NULL;
                            $$.setExpr(createDataset(no_rollupgroup, $3.getExpr(), createComma($7.getExpr(), attr, $9.getExpr(), $11.getExpr())));
                            $$.setPosition($1);
                        }
    | COMBINE '(' startLeftDelaySeqFilter ',' startRightFilter ')' endSelectorSequence
                        {
                            IHqlExpression * left = $3.getExpr();
                            IHqlExpression * right = $5.getExpr();
                            IHqlExpression * transform = parser->createDefJoinTransform(left,right,$1, $7.queryExpr(),NULL);
                            IHqlExpression * combine = createDataset(no_combine, left, createComma(right, transform, $7.getExpr()));
                            $$.setExpr(combine);
                            $$.setPosition($1);
                        }
    | COMBINE '(' startLeftDelaySeqFilter ',' startRightFilter ',' transform optCommonAttrs ')' endSelectorSequence
                        {
                            IHqlExpression * left = $3.getExpr();
                            IHqlExpression * right = $5.getExpr();
                            IHqlExpression * combine = createDataset(no_combine, left, createComma(right, $7.getExpr(), $8.getExpr(), $10.getExpr()));
                            $$.setExpr(combine);
                            $$.setPosition($1);
                        }
    | COMBINE '(' startLeftDelaySeqFilter ',' startRightFilter ',' startRightRowsGroup ',' transform optCommonAttrs ')' endRowsGroup endSelectorSequence
                        {
                            IHqlExpression * left = $3.getExpr();
                            IHqlExpression * right = $5.getExpr();
                            IHqlExpression * combine = createDataset(no_combinegroup, left, createComma(right, $9.getExpr(), $10.getExpr(), createComma($12.getExpr(), $13.getExpr())));
                            $$.setExpr(combine);
                            $$.setPosition($1);
                        }
    | LOOP '(' startLeftRowsSeqFilter beginCounterScope ',' expression ',' dataSet endCounterScope loopOptions ')' endRowsGroup endSelectorSequence
                        {
                            parser->normalizeExpression($6);
                            parser->ensureDatasetTypeMatch($8, $3.queryExpr());
                            parser->checkBooleanOrNumeric($6);
                            IHqlExpression * left = $3.getExpr();
                            IHqlExpression * body = createValue(no_loopbody, makeNullType(), $8.getExpr());
                            IHqlExpression * counter = $9.getExpr();
                            if (counter)
                                body = createComma(body, createAttribute(_countProject_Atom, counter));
                            IHqlExpression * loopCondition = parser->createLoopCondition(left, $6.getExpr(), NULL, $13.queryExpr(), $12.queryExpr());
                            IHqlExpression * loopExpr = createDataset(no_loop, left, createComma(loopCondition, body, $10.getExpr(), createComma($12.getExpr(), $13.getExpr())));
                            parser->checkLoopFlags($1, loopExpr);
                            $$.setExpr(loopExpr);
                            $$.setPosition($1);
                        }
    | LOOP '(' startLeftRowsSeqFilter beginCounterScope ',' expression ',' expression ',' dataSet endCounterScope loopOptions ')' endRowsGroup endSelectorSequence
                        {
                            parser->ensureDatasetTypeMatch($10, $3.queryExpr());
                            parser->normalizeExpression($6);
                            parser->checkBooleanOrNumeric($6);
                            parser->normalizeExpression($8, type_boolean, false);
                            IHqlExpression * left = $3.getExpr();
                            IHqlExpression * body = createValue(no_loopbody, makeNullType(), $10.getExpr());
                            IHqlExpression * counter = $11.getExpr();
                            if (counter)
                                body = createComma(body, createAttribute(_countProject_Atom, counter));
                            IHqlExpression * loopCondition = parser->createLoopCondition(left, $6.getExpr(), $8.getExpr(), $15.queryExpr(), $14.queryExpr());
                            IHqlExpression * loopExpr = createDataset(no_loop, left, createComma(loopCondition, body, $12.getExpr(), createComma($14.getExpr(), $15.getExpr())));
                            parser->checkLoopFlags($1, loopExpr);
                            $$.setExpr(loopExpr);
                            $$.setPosition($1);
                        }
    | LOOP '(' startLeftRowsSeqFilter beginCounterScope ',' expression ',' expression ',' expression ',' dataSet endCounterScope loopOptions ')' endRowsGroup endSelectorSequence
                        {
                            //LOOP(ds, <count>,<filter-cond>,<loop-cond>, f(rows(left)))
                            parser->ensureDatasetTypeMatch($12, $3.queryExpr());
                            parser->normalizeExpression($6, type_numeric, false);
                            parser->normalizeExpression($8, type_boolean, false);
                            parser->normalizeExpression($10, type_boolean, false);
                            IHqlExpression * left = $3.getExpr();
                            IHqlExpression * body = createValue(no_loopbody, makeNullType(), $12.getExpr());
                            IHqlExpression * counter = $13.getExpr();
                            if (counter)
                                body = createComma(body, createAttribute(_countProject_Atom, counter));
                            IHqlExpression * loopCondition = createComma($6.getExpr(), $8.getExpr(), $10.getExpr());
                            IHqlExpression * loopExpr = createDataset(no_loop, left, createComma(loopCondition, body, $14.getExpr(), createComma($16.getExpr(), $17.getExpr())));
                            parser->checkLoopFlags($1, loopExpr);
                            $$.setExpr(loopExpr);
                            $$.setPosition($1);
                        }
    | GRAPH '(' startLeftRowsSeqFilter beginCounterScope ',' expression ',' dataSet endCounterScope graphOptions ')' endRowsGroup endSelectorSequence
                        {
                            parser->ensureDatasetTypeMatch($8, $3.queryExpr());
                            parser->normalizeExpression($6);
                            parser->checkBooleanOrNumeric($6);
                            IHqlExpression * left = $3.getExpr();
                            IHqlExpression * body = createValue(no_loopbody, makeNullType(), $8.getExpr());
                            IHqlExpression * counter = $9.getExpr();
                            if (counter)
                                body = createComma(body, createAttribute(_countProject_Atom, counter));
                            IHqlExpression * loopExpr = createDataset(no_graphloop, left, createComma($6.getExpr(), body, $10.getExpr(), createComma($12.getExpr(), $13.getExpr())));
                            parser->checkLoopFlags($1, loopExpr);
                            $$.setExpr(loopExpr);
                            $$.setPosition($1);
                        }
    | GRAPH '(' startLeftRowsSeqFilter ')' endRowsGroup endSelectorSequence
                        {
                            $5.release();
                            $6.release();
                            $$.setExpr(createDataset(no_forcegraph, $3.getExpr()), $1);
                        }
    | ITERATE '(' startLeftRightSeqFilter ',' beginCounterScope transform optCommonAttrs ')' endCounterScope endSelectorSequence
                        {
                            parser->ensureTransformTypeMatch($6, $3.queryExpr());

                            IHqlExpression *ds = $3.getExpr();
                            IHqlExpression *tr = $6.getExpr();
                            IHqlExpression *attr = $7.getExpr();
                            IHqlExpression * counter = $9.getExpr();
                            if (counter)
                                attr = createComma(attr, createAttribute(_countProject_Atom, counter));
                            $$.setExpr(createDataset(no_iterate, ds, createComma(tr, attr, $10.getExpr())));
                            $$.setPosition($1);
                            parser->checkDistribution($3, $$.queryExpr(), false);
                        }
    | LIMIT '(' dataSet ',' expression limitOptions ')'
                        {
                            parser->normalizeExpression($5, type_int, false);
                            OwnedHqlExpr ds = $3.getExpr();
                            HqlExprArray args;
                            args.append(*LINK(ds));
                            args.append(*$5.getExpr());
                            $6.unwindCommaList(args);
                            node_operator op = queryAttribute(keyedAtom, args) || queryAttribute(countAtom, args) ? no_keyedlimit : no_limit;
                            IHqlExpression * onFail = queryAttribute(onFailAtom, args);
                            if (onFail && !parser->checkTransformTypeMatch($6, ds, onFail->queryChild(0)))
                                args.zap(*onFail);
                            $$.setExpr(createDataset(op, args));
                            $$.setPosition($1);
                        }
    | TOK_CATCH '(' dataSet ',' catchOption optCommonAttrs ')'
                        {
                            OwnedHqlExpr ds = $3.getExpr();
                            HqlExprArray args;
                            args.append(*LINK(ds));
                            args.append(*$5.getExpr());
                            $6.unwindCommaList(args);
                            IHqlExpression * onFail = queryAttribute(onFailAtom, args);
                            if (onFail && !parser->checkTransformTypeMatch($4, ds, onFail->queryChild(0)))
                                args.zap(*onFail);
                            $$.setExpr(createDataset(no_catchds, args));
                            $$.setPosition($1);
                        }
/*
    | TOK_CATCH '(' dataSet ',' expression ',' catchOption optCommonAttrs ')'
                        {
                            parser->normalizeExpression($5, type_boolean, false);
                            OwnedHqlExpr ds = $3.getExpr();
                            HqlExprArray args;
                            args.append(*LINK(ds));
                            args.append(*$5.getExpr());
                            args.append(*$7.getExpr());
                            $8.unwindCommaList(args);
                            IHqlExpression * onFail = queryAttribute(onFailAtom, args);
                            if (onFail && !parser->checkTransformTypeMatch($4, ds, onFail->queryChild(0)))
                                args.zap(*onFail);
                            $$.setExpr(createDataset(no_catchds, args));
                            $$.setPosition($1);
                        }
*/
    | MERGE '(' startTopFilter ',' mergeDataSetList ')' endTopFilter
                        {
                            bool isLocal = queryAttributeInList(localAtom, $5.queryExpr()) != NULL;
                            parser->checkMergeInputSorted($3, isLocal);
                            OwnedHqlExpr ds = $3.getExpr();

                            HqlExprArray args, orderedArgs;
                            args.append(*LINK(ds));
                            OwnedHqlExpr rest=$5.getExpr();
                            if (rest)
                                rest->unwindList(args, no_comma);

                            IHqlExpression * sorted = queryAttribute(sortedAtom, args);
                            OwnedHqlExpr newSorted;
                            if (!sorted)
                            {
                                parser->reportWarning(CategoryDeprecated, WRN_MERGE_RECOMMEND_SORTED, $1.pos, "MERGE without an explicit SORTED() attribute is deprecated");
                                OwnedHqlExpr order = getExistingSortOrder(ds, isLocal, true);
                                HqlExprArray sorts;
                                if (order)
                                    unwindChildren(sorts, order);
                                newSorted.setown(createExprAttribute(sortedAtom, sorts));
                                args.append(*LINK(newSorted));
                                // because parameters aren't substituted, need to deduce the sort order again later once they are
                                args.append(*createAttribute(_implicitSorted_Atom));
                            }
                            else
                            {
                                newSorted.setown(replaceSelector(sorted, ds->queryNormalizedSelector(), queryActiveTableSelector()));
                                args.zap(*sorted);
                                args.append(*LINK(newSorted));
                            }

                            ForEachItemIn(i, args)
                            {
                                IHqlExpression & cur = args.item(i);
                                if (cur.isDataset())
                                    parser->checkMergeSortOrder($1, ds, &cur, newSorted);
                            }

                            reorderAttributesToEnd(orderedArgs, args);
                            IHqlExpression *merge = createDataset(no_merge, orderedArgs);
                            $$.setExpr(merge);
                            $$.setPosition($1);
                        }
    | COGROUP '(' startTopFilter ',' cogroupDataSetList ')' endTopFilter
                        {
                            OwnedHqlExpr ds = $3.getExpr();
                            OwnedHqlExpr rest=$5.getExpr();

                            HqlExprArray args;
                            args.append(*LINK(ds));
                            if (rest)
                                rest->unwindList(args, no_comma);
                            IHqlExpression * group = queryAttribute(groupAtom, args);
                            if (!group)
                            {
                                parser->reportError(ERR_COGROUP_NO_GROUP, $1, "COGROUP requires a GROUP() parameter");
                                args.append(*createExprAttribute(groupAtom, getUnknownSortlist()));
                            }
                            else
                            {
                                OwnedHqlExpr newGroup = replaceSelector(group, ds->queryNormalizedSelector(), queryActiveTableSelector());
                                args.zap(*group);
                                args.append(*newGroup.getClear());
                            }

                            parser->checkRecordsMatch($3, args);

                            HqlExprArray orderedArgs;
                            reorderAttributesToEnd(orderedArgs, args);
                            $$.setExpr(createDataset(no_cogroup, orderedArgs), $1);
                        }
    | NONEMPTY '(' mergeDataSetList ')'     // mergeDataSetList to allow ,LOCAL
                        {
                            HqlExprArray args, orderedArgs;
                            $3.unwindCommaList(args);
                            parser->checkRecordsMatch($3, args);
                            reorderAttributesToEnd(orderedArgs, args);
                            $$.setExpr(createDataset(no_nonempty, args));
                            $$.setPosition($1);
                        }
    | PROJECT '(' startLeftSeqFilter ',' beginCounterScope transform endCounterScope projectOptions ')' endSelectorSequence
                        {
                            IHqlExpression *te = createComma($6.getExpr(), $8.getExpr(), $10.getExpr());
                            IHqlExpression *ds = $3.getExpr();
                            OwnedHqlExpr counter = $7.getExpr();
                            if (counter)
                                te = createComma(te, createAttribute(_countProject_Atom, LINK(counter)));
                            $$.setExpr(createDataset(no_hqlproject, ds, te));
                            $$.setPosition($1);
                        }
    | PROJECT '(' startLeftSeqFilter ',' beginCounterScope recordDef endCounterScope projectOptions ')' endSelectorSequence
                        {
                            OwnedHqlExpr transform = parser->createRowAssignTransform($3, $6, $10);
                            $6.release();
                            $7.release();
                            $$.setExpr(createDataset(no_hqlproject, $3.getExpr(), createComma(transform.getClear(), $8.getExpr(), $10.getExpr())));
                            $$.setPosition($1);
                        }
    | PULL '(' startTopFilter ')' endTopFilter
                        {
                            $$.setExpr(createDataset(no_metaactivity, $3.getExpr(), createAttribute(pullAtom)));
                            $$.setPosition($1);
                        }
    | DENORMALIZE '(' startLeftDelaySeqFilter ',' startRightFilter ',' expression ',' beginCounterScope transform endCounterScope optJoinFlags ')' endSelectorSequence
                        {
                            parser->normalizeExpression($7, type_boolean, false);
                            IHqlExpression * ds = $3.getExpr();
                            parser->ensureTransformTypeMatch($10, ds);

                            IHqlExpression * transform = $10.getExpr();
                            IHqlExpression * counter = $11.getExpr();
                            if (counter)
                                counter = createAttribute(_countProject_Atom, counter);

                            //MORE: This should require local otherwise it needs to do a full join type thing.
                            IHqlExpression * extra = createComma($5.getExpr(), $7.getExpr(), transform, createComma($12.getExpr(), counter, $14.getExpr()));
                            $$.setExpr(createDataset(no_denormalize, ds, extra));
                            $$.setPosition($1);
                            parser->checkJoinFlags($1, $$.queryExpr());
                        }
    | DENORMALIZE '(' startLeftDelaySeqFilter ',' startRightFilter ',' expression ',' beginCounterScope startRightRowsGroup endCounterScope ',' transform optJoinFlags ')' endRowsGroup endSelectorSequence
                        {
                            parser->normalizeExpression($7, type_boolean, false);
                            IHqlExpression * ds = $3.getExpr();
                            IHqlExpression * transform = $13.getExpr();
                            IHqlExpression * extra = createComma($5.getExpr(), $7.getExpr(), transform, createComma($14.getExpr(), $16.getExpr(), $17.getExpr()));
                            $$.setExpr(createDataset(no_denormalizegroup, ds, extra));
                            $$.setPosition($1);
                            $11.release();
                            parser->checkJoinFlags($1, $$.queryExpr());
                        }
    | NOFOLD '(' dataSet ')'
                        {
                            $$.setExpr(createDataset(no_nofold, $3.getExpr(), NULL));
                            $$.setPosition($1);
                        }
    | NOHOIST '(' dataSet ')'
                        {
                            $$.setExpr(createDataset(no_nohoist, $3.getExpr(), NULL));
                            $$.setPosition($1);
                        }
    | NOTHOR '(' dataSet ')'
                        {
                            $$.setExpr(createDataset(no_nothor, $3.getExpr(), NULL));
                            $$.setPosition($1);
                        }
    | NORMALIZE '(' startLeftSeqFilter ',' expression ',' beginCounterScope transform endCounterScope optCommonAttrs ')' endSelectorSequence
                        {
                            parser->normalizeExpression($5, type_numeric, false);
                            IHqlExpression * counter = $9.getExpr();
                            if (counter)
                                counter = createAttribute(_countProject_Atom, counter);
                            IHqlExpression * extra = createComma($5.getExpr(), $8.getExpr(), counter, createComma($10.getExpr(), $12.getExpr()));
                            $$.setExpr(createDataset(no_normalize, $3.getExpr(), extra));
                            $$.setPosition($1);
                        }
    | NORMALIZE '(' startLeftSeqFilter ',' startRightFilter ',' beginCounterScope transform endCounterScope optCommonAttrs ')' endSelectorSequence
                        {
                            //NB: SelSeq is based only on the left dataset, not the right as well.
                            IHqlExpression * counter = $9.getExpr();
                            if (counter)
                                counter = createAttribute(_countProject_Atom, counter);
                            IHqlExpression * extra = createComma($5.getExpr(), $8.getExpr(), counter, createComma($10.getExpr(), $12.getExpr()));
                            $$.setExpr(createDataset(no_normalize, $3.getExpr(), extra));
                            $$.setPosition($1);
                        }
/*
 * Needs more thought on the representation - so we can track distributions etc.
 * and need to implement an activity to generate it.
    | NORMALIZE '(' startLeftSeqFilter ',' startLeftRowsGroup ',' beginCounterScope dataSet endCounterScope optCommonAttrs ')' endRowsGroup endSelectorSequence
                        {
                            parser->checkGrouped($3);
                            IHqlExpression * counter = $9.getExpr();
                            if (counter)
                                counter = createAttribute(_countProject_Atom, counter);
                            IHqlExpression *attr = createComma($10.getExpr(), $12.getExpr(), $13.getExpr());
                            $$.setExpr(createDataset(no_normalizegroup, $3.getExpr(), createComma($8.getExpr(), counter, attr)));
                            $$.setPosition($1);
                        }
 */
    | GROUP '(' startTopFilter startGROUP beginList sortList endGROUP
                        {
                            HqlExprArray sortItems;
                            parser->endList(sortItems);
                            OwnedHqlExpr input = $3.getExpr();
                            OwnedHqlExpr attrs;
                            IHqlExpression *groupOrder = parser->processSortList($6, no_group, input, sortItems, NULL, &attrs);
                            OwnedHqlExpr args = createComma(groupOrder, LINK(attrs));
                            $$.setExpr(createDataset(no_group, input.getClear(), args.getClear()), $1);
                        }
    | GROUPED '(' startTopFilter startGROUP beginList sortList endGROUP
                        {
                            HqlExprArray sortItems;
                            parser->endList(sortItems);
                            IHqlExpression *input = $3.getExpr();
                            OwnedHqlExpr attrs;
                            IHqlExpression *groupOrder = parser->processSortList($6, no_group, input, sortItems, NULL, &attrs);
                            IHqlExpression *args = createComma(groupOrder, attrs.getClear());
                            $$.setExpr(createDataset(no_grouped, input, args), $1);
                        }
    | GROUP '(' startTopFilter ')' endTopFilter
                        {
                            IHqlExpression *input = $3.getExpr();
                            $$.setExpr(createDataset(no_group, input, NULL));
                            $$.setPosition($1);
                        }
    | GROUP '(' startTopFilter startGROUP beginList ROW endGROUP ignoreDummyList
                        {
                            IHqlExpression *input = $3.getExpr();
                            $$.setExpr(createDataset(no_group, input, createComma(getActiveTableSelector(), createLocalAttribute())));
                            $$.setPosition($1);
                        }
    | REGROUP '(' dataSetList ')'
                        {
                            OwnedHqlExpr ds = $3.getExpr();
                            HqlExprArray args;
                            ds->unwindList(args, no_comma);
                            parser->checkRegrouping($3.pos, args);
                            $$.setExpr(createDataset(no_regroup, args));
                            $$.setPosition($1);
                        }
    | HAVING '(' startLeftRowsSeqFilter ',' condList ')' endRowsGroup endSelectorSequence
                        {
                            parser->checkGrouped($3);
                            IHqlExpression *attr = NULL;        // possibly local may make sense if thor supported it as a global operation, but it would be too painful.
                            $$.setExpr(createDataset(no_filtergroup, $3.getExpr(), createComma($5.getExpr(), attr, $7.getExpr(), $8.getExpr())));
                            $$.setPosition($1);
                        }
    | KEYED '(' dataSet indexListOpt ')' endTopFilter
                        {
                            IHqlExpression * dataset = $3.getExpr();
                            IHqlExpression * indices = $4.getExpr();
                            $$.setExpr(createDataset(no_keyed, dataset, indices));
                            $$.setPosition($1);
                        }
    | UNGROUP '(' startTopFilter ')' endTopFilter
                        {
                            IHqlExpression *input = $3.getExpr();
                            $$.setExpr(createDataset(no_group, input, NULL));
                            $$.setPosition($1);
                        }
    | TABLE '(' startTopFilter ',' recordDef beginList optSortList ')' endTopFilter
                        {
                            HqlExprArray sortItems;
                            parser->endList(sortItems);
                            OwnedHqlExpr dataset = $3.getExpr();

                            parser->checkOutputRecord($5, false);

                            OwnedHqlExpr record = $5.getExpr();

                            OwnedHqlExpr attrs;
                            OwnedHqlExpr grouping = parser->processSortList($7, no_usertable, dataset, sortItems, NULL, &attrs);

                            if (grouping && !queryAttributeInList(groupedAtom, attrs))
                            {
                                parser->checkGrouping($7, dataset,record,grouping);
                                if (dataset->getOperator() == no_group && isGrouped(dataset))
                                    parser->reportWarning(CategoryIgnored, WRN_GROUPINGIGNORED, $3.pos, "Grouping of table input will have no effect, was this intended?");
                            }

                            HqlExprArray args;
                            args.append(*LINK(dataset));
                            args.append(*LINK(record));
                            if (grouping)
                                args.append(*LINK(grouping));
                            if (attrs)
                                attrs->unwindList(args, no_comma);
                            //appendUniqueId();
                            $$.setExpr(createDataset(no_usertable, args));
                            parser->checkProjectedFields($$.queryExpr(), $5);
                            $$.setPosition($1);
                        }
    | TABLE '(' startTopFilter ',' transform ')' endTopFilter
                        {
                            OwnedHqlExpr tform = $5.getExpr();
                            $$.setExpr(createDataset(no_newusertable, $3.getExpr(), createComma(LINK(tform->queryRecord()), ensureTransformType(tform, no_newtransform))));
                            $$.setPosition($1);
                        }
    | TABLE '(' startTopFilter ')' endTopFilter
                        {
                            $$.setExpr(createDataset(no_dataset_alias, $3.getExpr(), ::createUniqueId()), $1);
                        }
    | FETCH '(' startLeftDelaySeqFilter ',' startRightFilter ',' expression ',' transform optCommonAttrs ')' endSelectorSequence
                        {
                            parser->normalizeExpression($7, type_int, false);
                            IHqlExpression * left = $3.getExpr();
                            IHqlExpression * right = $5.getExpr();

                            node_operator modeOp = no_none;
                            if (left->getOperator() == no_table)
                                modeOp = left->queryChild(2)->getOperator();
                            if ((modeOp != no_thor) && (modeOp != no_flat) && (modeOp != no_csv) && (modeOp != no_xml))
                                parser->reportError(ERR_FETCH_NON_DATASET, $3, "First parameter of FETCH should be a disk file");

                            IHqlExpression *join = createDataset(no_fetch, left, createComma(right, $7.getExpr(), $9.getExpr(), createComma($10.getExpr(), $12.getExpr())));

                            $$.setExpr(join);
                            $$.setPosition($1);
                        }
    | FETCH '(' startLeftDelaySeqFilter ',' startRightFilter ',' expression optCommonAttrs ')' endSelectorSequence
                        {
                            parser->normalizeExpression($7, type_int, false);
                            IHqlExpression * left = $3.getExpr();
                            IHqlExpression * right = $5.getExpr();
                            IHqlExpression * transform = parser->createDefJoinTransform(left, right, $7, $10.queryExpr(),NULL);

                            node_operator modeOp = no_none;
                            if (left->getOperator() == no_table)
                                modeOp = left->queryChild(2)->getOperator();
                            if ((modeOp != no_thor) && (modeOp != no_flat) && (modeOp != no_csv) && (modeOp != no_xml))
                                parser->reportError(ERR_FETCH_NON_DATASET, $3, "First parameter of FETCH should be a disk file");

                            IHqlExpression *join = createDataset(no_fetch, left, createComma(right, $7.getExpr(), transform, createComma($8.getExpr(), $10.getExpr())));

                            $$.setExpr(join);
                            $$.setPosition($1);
                        }
    | INDEX '(' startTopFilter ',' indexTopRecordAndName optIndexFlags ')' endTopFilter endTopFilter
                        {
                            IHqlExpression *dataset = $3.getExpr();
                            OwnedHqlExpr record = $5.getExpr();
                            OwnedHqlExpr extra = $6.getExpr();
                            parser->extractIndexRecordAndExtra(record, extra);
                            OwnedHqlExpr transform = parser->extractTransformFromExtra(extra);

                            parser->inheritRecordMaxLength(dataset, record);

                            bool hasFileposition = getBoolAttributeInList(extra, filepositionAtom, true);
                            record.setown(parser->checkIndexRecord(record, $5, extra));
                            if (transform)
                            {
                                if (!recordTypesMatch(dataset, transform))
                                {
                                    parser->reportError(ERR_TRANSFORM_TYPE_MISMATCH, $5,"Type returned from transform must match the source dataset type");
                                    transform.setown(parser->createClearTransform(dataset->queryRecord(), $5));
                                }
                                //Convert the no_transform to a no_newtransform, because it is currently a bit of a fake.
                                HqlExprArray args;
                                unwindChildren(args, transform);
                                transform.setown(createValue(no_newtransform, transform->getType(), args));
                                $$.setExpr(createDataset(no_newkeyindex, dataset, createComma(record.getClear(), transform.getClear(), extra.getClear())));
                            }
                            else
                                $$.setExpr(createDataset(no_keyindex, dataset, createComma(record.getClear(), extra.getClear())));
                            parser->checkIndexRecordTypes($$.queryExpr(), $1);
                            $$.setPosition($1);
                        }
    | INDEX '(' indexTopRecordAndName optIndexFlags ')' endTopFilter
                        {
                            OwnedHqlExpr record = $3.getExpr();
                            OwnedHqlExpr extra = $4.getExpr();
                            parser->extractIndexRecordAndExtra(record, extra);
                            $$.setExpr(parser->createIndexFromRecord(record, extra, $3));
                            parser->checkIndexRecordTypes($$.queryExpr(), $1);
                            $$.setPosition($1);
                        }
    | INDEX '(' startTopFilter ',' expression optIndexFlags ')' endTopFilter
                        {
                            parser->normalizeExpression($5, type_string, false);
                            OwnedHqlExpr newName = $5.getExpr();
                            OwnedHqlExpr dataset = $3.getExpr();
                            if (isKey(dataset))
                            {
                                node_operator keyOp = dataset->getOperator();
                                HqlExprArray args;
                                unwindChildren(args, dataset);
                                if (keyOp == no_keyindex)
                                    args.replace(*newName.getClear(), 2);
                                else
                                    args.replace(*newName.getClear(), 3);
                                $6.unwindCommaList(args);
                                dataset.setown(createDataset(keyOp, args));
                            }
                            else
                                parser->reportError(ERR_EXPECTED_INDEX,$3,"Index aliasing syntax - expected an index as the first parameter");

                            $$.setExpr(dataset.getClear());
                            $$.setPosition($1);
                        }
    | DATASET '(' thorFilenameOrList ',' beginCounterScope dsRecordDef endCounterScope ',' mode optDsOptions dsEnd
                        {
                            OwnedHqlExpr counter = $7.queryExpr();
                            if (counter)
                                parser->reportError(ERR_ILL_HERE,$6,"Not expecting COUNTER for DATASET");
                            parser->warnIfRecordPacked($6);
                            parser->transferOptions($3, $10);
                            parser->normalizeExpression($3, type_string, false);

                            OwnedHqlExpr mode = $9.getExpr();
                            OwnedHqlExpr options = $10.getExpr();
                            OwnedHqlExpr filename = $3.getExpr();
                            if (mode->getOperator() == no_comma)
                            {
                                //Handle a messy common-grammar production which isn't in the format we want
                                assertex(mode->queryChild(0)->getOperator() == no_pipe);
                                HqlExprArray args;
                                unwindChildren(args, mode->queryChild(0));
                                mode->queryChild(1)->unwindList(args, no_comma);
                                mode.setown(createValue(no_pipe, makeNullType(), args));
                            }

                            //LOCAL(expression) is now an overloaded the syntax, so disambiguate here...
                            if (filename->getOperator() == no_forcelocal)
                            {
                                filename.setown(createValue(no_assertconstant, filename->getType(), LINK(filename->queryChild(0))));
                                options.setown(createComma(options.getClear(), createAttribute(localUploadAtom)));
                            }
                            IHqlExpression * dataset = createNewDataset(filename.getClear(), $6.getExpr(), mode.getClear(), NULL, NULL, options.getClear());
                            parser->checkValidRecordMode(dataset, $4, $9);
                            $$.setExpr(dataset);
                            $$.setPosition($1);
                        }
    | DATASET '(' thorFilenameOrList ',' beginCounterScope simpleType endCounterScope optDsOptions dsEnd
                        {
                            OwnedHqlExpr counter = $7.queryExpr();
                            if (counter)
                                parser->reportError(ERR_ILL_HERE,$6,"Not expecting COUNTER for DATASET");
                            parser->transferOptions($3, $8);
                            parser->normalizeExpression($3, type_string, false);

                            OwnedHqlExpr options = $8.getExpr();
                            OwnedITypeInfo type = $6.getType();
                            OwnedHqlExpr filename = $3.getExpr();
                            OwnedHqlExpr option;
                            switch (type->getTypeCode())
                            {
                            case type_string:
                                option.setown(createAttribute(asciiAtom));
                                break;
                            case type_utf8:
                            case type_unicode:
                                option.setown(createAttribute(unicodeAtom));
                                break;
                            default:
                                parser->reportError(ERR_EXPECTED, $1, "Expected STRING/UTF8/UNICODE");
                            }
                            
                            OwnedHqlExpr empty = createList(no_list, makeSetType(NULL), NULL);
                            OwnedHqlExpr quoted = createExprAttribute(quoteAtom, LINK(empty));
                            OwnedHqlExpr separator = createExprAttribute(separatorAtom, LINK(empty));
                            OwnedHqlExpr mode = createValue(no_csv, makeNullType(), quoted.getClear(), separator.getClear(), option.getClear());
                            OwnedHqlExpr field = createField(lineId, LINK(type), NULL);
                            OwnedHqlExpr record = createRecord(field);

                            //LOCAL(expression) is now an overloaded the syntax, so disambiguate here...
                            if (filename->getOperator() == no_forcelocal)
                            {
                                filename.setown(createValue(no_assertconstant, filename->getType(), LINK(filename->queryChild(0))));
                                options.setown(createComma(options.getClear(), createAttribute(localUploadAtom)));
                            }
                            IHqlExpression * dataset = createNewDataset(filename.getClear(), record.getClear(), mode.getClear(), NULL, NULL, options.getClear());
                            parser->checkValidRecordMode(dataset, $4, $9);
                            $$.setExpr(dataset, $1);
                        }
    | DATASET '(' dataSet ',' thorFilenameOrList ',' mode optDsOptions dsEnd
                        {
                            parser->warnIfRecordPacked($3);
                            parser->transferOptions($5, $8);
                            parser->normalizeExpression($5, type_string, false);

                            IHqlExpression * origin = $3.getExpr();
                            IHqlExpression * filename = $5.getExpr();
                            IHqlExpression * mode = $7.getExpr();
                            IHqlExpression * attrs = createComma(createAttribute(_origin_Atom, origin), $8.getExpr());
                            IHqlExpression * dataset = createNewDataset(filename, LINK(origin->queryRecord()), mode, NULL, NULL, attrs);

                            parser->checkValidRecordMode(dataset, $4, $7);
                            $$.setExpr(dataset);
                            $$.setPosition($1);
                        }
    | DATASET '(' '[' beginList inlineDatasetValueList ']' ',' recordDef optDatasetFlags ')'
                        {
                            HqlExprArray values;
                            parser->endList(values);
                            OwnedHqlExpr table = createDataset(no_temptable, createValue(no_recordlist, NULL, values), createComma($8.getExpr(), $9.getExpr()));
                            $$.setExpr(convertTempTableToInlineTable(*parser->errorHandler, $5.pos, table));
                            $$.setPosition($1);
                        }
    | DATASET '(' '[' beginList transformList ']' optDatasetFlags ')'
                        {
                            HqlExprArray values;
                            parser->endList(values);
                            IHqlExpression * record = values.item(0).queryRecord();
                            parser->checkCompatibleTransforms(values, record, $5);
                            $$.setExpr(createDataset(no_inlinetable, createValue(no_transformlist, makeNullType(), values), createComma(LINK(record), $7.getExpr())));
                            $$.setPosition($1);
                        }
    | DATASET '(' thorFilenameOrList ',' beginCounterScope recordDef endCounterScope ')'
                        {
                            //NB: $3 is required to be a list, but uses thorfilename production to work around a s/r error
                            OwnedHqlExpr counter = $7.queryExpr();
                            if (counter)
                                parser->reportError(ERR_ILL_HERE,$6,"Not expecting COUNTER for DATASET");
                            parser->normalizeExpression($3, type_set, false);

                            $$.setExpr(parser->createDatasetFromList($3, $6), $1);
                        }
    | DATASET '(' WORKUNIT '(' expression ',' expression ')' ',' recordDef ')'
                        {
                            parser->normalizeExpression($5, type_scalar, false);
                            parser->normalizeExpression($7, type_scalar, false);
                            IHqlExpression * wuid = createExprAttribute(wuidAtom, $5.getExpr());
                            IHqlExpression * arg = $7.getExpr();
                            if (isStringType(arg->queryType()))
                            {
                                arg = createAttribute(nameAtom, arg);
                                arg = createComma(createExprAttribute(sequenceAtom, createConstant(0)), arg);
                            }
                            else
                                arg = createExprAttribute(sequenceAtom, arg);
                            $$.setExpr(createDataset(no_workunit_dataset, $10.getExpr(), createComma(wuid, arg)));
                            $$.setPosition($1);
                        }
    | DATASET '(' WORKUNIT '(' expression ')' ',' recordDef ')'
                        {
                            parser->normalizeExpression($5, type_scalar, false);
                            IHqlExpression * arg = $5.getExpr();
                            if (isStringType(arg->queryType()))
                            {
                                arg = createExprAttribute(nameAtom, arg);
                                arg = createComma(createExprAttribute(sequenceAtom, createConstant(0)), arg);
                            }
                            else
                                arg = createExprAttribute(sequenceAtom, arg);
                            $$.setExpr(createDataset(no_workunit_dataset, $8.getExpr(), arg));
                            $$.setPosition($1);
                        }
    | DATASET '(' thorFilenameOrList ',' beginCounterScope transform endCounterScope optDatasetFlags ')'
                        {
                            parser->normalizeExpression($3, type_int, false);
                            IHqlExpression * counter = $7.getExpr();
                            if (counter)
                                counter = createAttribute(_countProject_Atom, counter);
                            OwnedHqlExpr options = $8.getExpr();
                            if (options)
                            {
                                if (options->numChildren() > 0)
                                    parser->reportError(ERR_DSPARAM_INVALIDOPTCOMB, $8, "The DATASET options DISTRIBUTED, LOCAL, and NOLOCAL are not permutable.");
                            }
                            $$.setExpr(createDataset(no_dataset_from_transform, $3.getExpr(), createComma($6.getExpr(), counter, options.getClear())));
                            $$.setPosition($1);
                        }
    | ENTH '(' dataSet ',' expression optCommonAttrs ')'
                        {
                            parser->normalizeExpression($5, type_numeric, false);
                            $$.setExpr(createDataset(no_enth, $3.getExpr(), createComma($5.getExpr(), $6.getExpr())));
                            $$.setPosition($1);
                        }
    | ENTH '(' dataSet ',' expression ',' expression optCommonAttrs ')'
                        {
                            parser->normalizeExpression($5, type_numeric, false);
                            parser->normalizeExpression($7, type_numeric, false);
                            $$.setExpr(createDataset(no_enth, $3.getExpr(), createComma($5.getExpr(), $7.getExpr(), $8.getExpr())));
                            $$.setPosition($1);
                        }
    | ENTH '(' dataSet ',' expression ',' expression ',' expression optCommonAttrs ')'
                        {
                            parser->normalizeExpression($5, type_numeric, false);
                            parser->normalizeExpression($7, type_numeric, false);
                            parser->normalizeExpression($9, type_numeric, false);
                            $$.setExpr(createDataset(no_enth, $3.getExpr(), createComma($5.getExpr(), $7.getExpr(), $9.getExpr(), $10.getExpr())));
                            $$.setPosition($1);
                        }
    | PIPE '(' expression ',' recordDef optPipeOptions ')'
                        {
                            parser->normalizeExpression($3, type_string, false);
                            parser->checkValidPipeRecord($5, $5.queryExpr(), $6.queryExpr(), NULL);
                            $$.setExpr(createNewDataset(createConstant(""), $5.getExpr(), createValue(no_pipe, makeNullType(), $3.getExpr()), NULL, NULL, $6.getExpr()));
                            $$.setPosition($1);
                        }
    | PIPE '(' startTopFilter ',' expression optPipeOptions endTopFilter ')'
                        {
                            parser->normalizeExpression($5, type_string, false);

                            OwnedHqlExpr attrs = $6.getExpr();
                            parser->checkValidPipeRecord($3, $3.queryExpr()->queryRecord(), attrs, NULL);
                            parser->checkValidPipeRecord($3, $3.queryExpr()->queryRecord(), NULL, queryAttributeInList(outputAtom, attrs));

                            $$.setExpr(createDataset(no_pipe, $3.getExpr(), createComma($5.getExpr(), LINK(attrs))));
                            $$.setPosition($1);
                        }
    | PIPE '(' startTopFilter ',' expression ',' recordDef optPipeOptions endTopFilter ')'
                        {
                            parser->normalizeExpression($5, type_string, false);

                            OwnedHqlExpr attrs = $8.getExpr();
                            parser->checkValidPipeRecord($3, $3.queryExpr()->queryRecord(), NULL, queryAttributeInList(outputAtom, attrs));
                            parser->checkValidPipeRecord($7, $7.queryExpr()->queryRecord(), attrs, NULL);

                            $$.setExpr(createDataset(no_pipe, $3.getExpr(), createComma($5.getExpr(), $7.getExpr(), LINK(attrs))));
                            $$.setPosition($1);
                        }
    | PRELOAD '(' dataSet optConstExpression ')'
                        {
                            $$.setExpr(createDataset(no_preload, $3.getExpr(), $4.getExpr()));
                            $$.setPosition($1);
                        }
    | SAMPLE '(' dataSet ',' expression ')'
                        {
                            parser->normalizeExpression($5, type_int, false);
                            $$.setExpr(createDataset(no_sample, $3.getExpr(), $5.getExpr()));
                            $$.setPosition($1);
                        }
    | SAMPLE '(' dataSet ',' expression ',' expression ')'
                        {
                            parser->normalizeExpression($5, type_int, false);
                            parser->normalizeExpression($7, type_int, false);
                            $$.setExpr(createDataset(no_sample, $3.getExpr(), createComma($5.getExpr(), $7.getExpr())));
                            $$.setPosition($1);
                        }
    | TOPN '(' startTopFilter ',' expression ',' startSortOrder beginList sortListOptCurleys ')' endSortOrder endTopFilter
                        {
                            HqlExprArray sortItems;
                            parser->endList(sortItems);
                            parser->normalizeExpression($5, type_int, false);
                            IHqlExpression* dataset = $3.queryExpr();
                            IHqlExpression *input = $3.getExpr();
                            OwnedHqlExpr attrs;
                            IHqlExpression *sortOrder = parser->processSortList($9, no_topn, dataset, sortItems, NULL, &attrs);
                            if (!sortOrder)
                            {
                                parser->reportError(ERR_SORT_EMPTYLIST, $1, "The list to be sorted on is empty");
                                $$.setExpr(input);
                            }
                            else
                            {
                                IHqlExpression * best = queryAttributeInList(bestAtom, attrs);
                                if (best)
                                    parser->checkMaxCompatible(sortOrder, best, $9);
                                bool isLocal = (queryAttributeInList(localAtom, attrs)!=NULL);
                                parser->checkDistribution($3, input, isLocal, false);
                                attrs.setown(createComma(sortOrder, $5.getExpr(), LINK(attrs)));
                                $$.setExpr(createDataset(no_topn, input, attrs.getClear()));
                            }
                            $$.setPosition($1);
                        }
    | SORT '(' startSortOrder startTopFilter ',' beginList sortListOptCurleys ')' endSortOrder endTopFilter
                        {
                            HqlExprArray sortItems;
                            parser->endList(sortItems);
                            $$.setExpr(parser->createSortExpr(no_sort, $4, $7, sortItems));
                            $$.setPosition($1);
                        }
    | SUBSORT '(' startSortOrder startTopFilter ',' sortListExpr ',' sortListExpr optCommonAttrs ')'  endSortOrder endTopFilter
                        {
                            OwnedHqlExpr options = $9.getExpr();
                            if (isGrouped($4.queryExpr()))
                                parser->reportError(HQLERR_CannotBeGrouped, $1, "SUBSORT not yet supported on grouped datasets");
                            //NB: $6 and $8 are reversed in their internal representation to make consistent with no_sort
                            $$.setExpr(createDataset(no_subsort, $4.getExpr(), createComma($8.getExpr(), $6.getExpr(), options.getClear())), $1);
                        }
    | SORTED '(' startSortOrder startTopFilter ',' beginList sortListOptCurleys ')' endSortOrder endTopFilter
                        {
                            HqlExprArray sortItems;
                            parser->endList(sortItems);
                            $$.setExpr(parser->createSortExpr(no_sorted, $4, $7, sortItems));
                            $$.setPosition($1);
                        }
    | SORTED '(' startSortOrder dataSet ')' endSortOrder
                        {
                            OwnedHqlExpr dataset = $4.getExpr();
                            HqlExprArray args, sorted;
                            IHqlExpression * record = dataset->queryRecord();
                            unwindRecordAsSelects(sorted, record, dataset->queryNormalizedSelector());
                            args.append(*dataset.getClear());
                            args.append(*createSortList(sorted));
                            $$.setExpr(createDataset(no_sorted, args));
                            $$.setPosition($1);
                        }
    | STEPPED '(' startTopFilter ',' expressionList optStepFlags ')' endTopFilter
                        {
                            OwnedHqlExpr dataset = $3.getExpr();

                            HqlExprArray args;
                            $5.unwindCommaList(args);
                            OwnedHqlExpr stepOrder = createSortList(args);
                            $$.setExpr(createDatasetF(no_stepped, dataset.getClear(), stepOrder.getClear(), $6.getExpr(), NULL));
                            $$.setPosition($1);
                        }
    | '(' dataSet  ')'  {
                            $$.setExpr($2.getExpr());
                            $$.setPosition($1);
                        }
    | IF '(' booleanExpr ',' dataSet ',' dataSet ')'
                        {
                            OwnedHqlExpr ds = parser->processIfProduction($3, $5, &$7);
                            $$.setExpr(ds.getClear(), $1);
                        }
    | IF '(' booleanExpr ',' dataSet ')'
                        {
                            OwnedHqlExpr ds = parser->processIfProduction($3, $5, NULL);
                            $$.setExpr(ds.getClear(), $1);
                        }
    | IFF '(' booleanExpr ',' dataSet ',' dataSet ')'
                        {
                            OwnedHqlExpr ds = parser->processIfProduction($3, $5, &$7);
                            $$.setExpr(ds.getClear(), $1);
                        }
    | IFF '(' booleanExpr ',' dataSet ')'
                        {
                            OwnedHqlExpr ds = parser->processIfProduction($3, $5, NULL);
                            $$.setExpr(ds.getClear(), $1);
                        }
    | MAP '(' mapDatasetSpec ',' dataSet ')'
                        {
                            HqlExprArray args;
                            OwnedHqlExpr elseExpr = $5.getExpr();
                            $3.unwindCommaList(args);
                            parser->ensureMapToRecordsMatch(elseExpr, args, $5, false);
                            args.append(*elseExpr.getClear());
                            $$.setExpr(::createDataset(no_map, args));
                            $$.setPosition($1);
                        }
    | MAP '(' mapDatasetSpec ')'
                        {
                            HqlExprArray args;
                            $3.unwindCommaList(args);
                            OwnedHqlExpr elseExpr;
                            if (args.ordinality())
                                elseExpr.setown(createNullExpr(&args.item(0)));
                            else
                                elseExpr.setown(createDataset(no_null, LINK(queryNullRecord())));

                            parser->ensureMapToRecordsMatch(elseExpr, args, $3, false);
                            args.append(*elseExpr.getClear());
                            $$.setExpr(::createDataset(no_map, args));
                            $$.setPosition($1);
                        }
    | CASE '(' expression ',' beginList caseDatasetSpec ',' dataSet ')'
                        {
                            parser->normalizeExpression($3, type_scalar, false);
                            HqlExprArray args;
                            OwnedHqlExpr elseExpr = $8.getExpr();
                            parser->endList(args);
                            parser->checkCaseForDuplicates(args, $6);

                            parser->ensureMapToRecordsMatch(elseExpr, args, $8, false);

                            args.add(*$3.getExpr(),0);
                            args.append(*elseExpr.getClear());
                            $$.setExpr(::createDataset(no_case, args));
                            $$.setPosition($1);
                        }
    | CASE '(' expression ',' beginList caseDatasetSpec ')'
                        {
                            parser->normalizeExpression($3, type_scalar, false);
                            HqlExprArray args;
                            parser->endList(args);
                            OwnedHqlExpr elseDs;
                            if (args.ordinality())
                                elseDs.setown(createNullExpr(&args.item(0)));
                            else
                                elseDs.setown(createDataset(no_null, LINK(queryNullRecord())));
                            parser->checkCaseForDuplicates(args, $6);

                            parser->ensureMapToRecordsMatch(elseDs, args, $6, false);

                            args.add(*$3.getExpr(),0);
                            args.append(*elseDs.getClear());
                            $$.setExpr(::createDataset(no_case, args));
                            $$.setPosition($1);
                        }
    | CASE '(' expression ',' beginList dataSet ')'
                        {
                            parser->normalizeExpression($3, type_scalar, false);
                            // change error to warning.
                            parser->reportWarning(CategoryUnusual, WRN_CASENOCONDITION, $1.pos, "CASE does not have any conditions");
                            HqlExprArray list;
                            parser->endList(list);
                            $3.release();
                            $$.setExpr($6.getExpr(), $1);
                        }
    | CHOOSE '(' expression ',' mergeDataSetList ')'
                        {
                            parser->normalizeExpression($3, type_int, false);
                            OwnedHqlExpr values = $5.getExpr();
                            HqlExprArray unorderedArgs;
                            values->unwindList(unorderedArgs, no_comma);

                            HqlExprArray args;
                            reorderAttributesToEnd(args, unorderedArgs);

                            IHqlExpression * compareDs = NULL;
                            ForEachItemIn(idx, args)
                            {
                                IHqlExpression * cur = &args.item(idx);
                                if (cur->queryRecord())
                                {
                                    if (compareDs)
                                    {
                                        if (isGrouped(cur) != isGrouped(compareDs))
                                            parser->reportError(ERR_GROUPING_MISMATCH, $1, "Branches of the condition have different grouping");
                                        OwnedHqlExpr mapped = parser->checkEnsureRecordsMatch(compareDs, cur, $5.pos, false);
                                        if (mapped != cur)
                                            args.replace(*mapped.getClear(), idx);
                                    }
                                    else
                                        compareDs = cur;
                                }
                            }

                            args.add(*$3.getExpr(), 0);
                            $$.setExpr(createDataset(no_chooseds, args), $1);
                        }
    | PARSE '(' startTopLeftSeqFilter ',' expression ',' startRootPattern ',' recordDef endRootPattern endTopLeftFilter doParseFlags ')' endSelectorSequence
                        {
                            parser->normalizeExpression($5, type_stringorunicode, false);
                            parser->checkOutputRecord($9, false);
                            IHqlExpression * ds = createDataset(no_parse, $3.getExpr(), createComma($5.getExpr(), $7.getExpr(), $9.getExpr(), createComma($12.getExpr(), $14.getExpr())));
                            if (ds->hasAttribute(tomitaAtom) && (ds->queryChild(2)->queryType()->getTypeCode() == type_pattern))
                                parser->reportError(ERR_EXPECTED_RULE, $7, "Expected a rule as parameter to PARSE");
                            $$.setExpr(ds);
                            $$.setPosition($1);
                        }
    | PARSE '(' startTopLeftSeqFilter ',' expression ',' startRootPattern ',' transform endRootPattern endTopLeftFilter doParseFlags ')' endSelectorSequence
                        {
                            parser->normalizeExpression($5, type_stringorunicode, false);
                            IHqlExpression * record = $9.queryExpr()->queryRecord();
                            IHqlExpression * ds = createDataset(no_newparse, $3.getExpr(), createComma($5.getExpr(), $7.getExpr(), LINK(record), createComma($9.getExpr(), $12.getExpr(), $14.getExpr())));
                            if (ds->hasAttribute(tomitaAtom) && (ds->queryChild(2)->queryType()->getTypeCode() == type_pattern))
                                parser->reportError(ERR_EXPECTED_RULE, $7, "Expected a rule as parameter to PARSE");
                            $$.setExpr(ds);
                            $$.setPosition($1);
                        }
    | PARSE '(' startTopLeftSeqFilter ',' expression ',' transform endTopLeftFilter xmlParseFlags ')' endSelectorSequence
                        {
                            parser->normalizeExpression($5, type_stringorunicode, false);
                            IHqlExpression * record = $7.queryExpr()->queryRecord();
                            $$.setExpr(createDataset(no_newxmlparse, $3.getExpr(), createComma($5.getExpr(), LINK(record), $7.getExpr(), createComma($9.getExpr(), $11.getExpr()))));
                            $$.setPosition($1);
                        }
    | PARSE '(' startTopLeftSeqFilter ',' expression ',' recordDef endTopLeftFilter xmlParseFlags ')' endSelectorSequence
                        {
                            parser->normalizeExpression($5, type_stringorunicode, false);
                            parser->checkOutputRecord($7, false);
                            $$.setExpr(createDataset(no_xmlparse, $3.getExpr(), createComma($5.getExpr(), $7.getExpr(), $9.getExpr(), $11.getExpr())));
                            $$.setPosition($1);
                        }
    | FAIL '(' recordDef failDatasetParam ')'
                        {
                            //Actually allow a sequence of arbitrary actions....
                            $$.setExpr(createDataset(no_fail, $3.getExpr(), $4.getExpr()));
                            $$.setPosition($1);
                        }
    | FAIL '(' dataSet failDatasetParam ')'
                        {
                            OwnedHqlExpr ds = $3.getExpr();
                            //Actually allow a sequence of arbitrary actions....
                            $$.setExpr(createDataset(no_fail, LINK(ds->queryRecord()), $4.getExpr()));
                            $$.setPosition($1);
                        }
    | TOK_ERROR '(' recordDef failDatasetParam ')'
                        {
                            //Actually allow a sequence of arbitrary actions....
                            $$.setExpr(createDataset(no_fail, $3.getExpr(), $4.getExpr()));
                            $$.setPosition($1);
                        }
    | TOK_ERROR '(' dataSet failDatasetParam ')'
                        {
                            OwnedHqlExpr ds = $3.getExpr();
                            //Actually allow a sequence of arbitrary actions....
                            $$.setExpr(createDataset(no_fail, LINK(ds->queryRecord()), $4.getExpr()));
                            $$.setPosition($1);
                        }
    | SKIP '(' recordDef ')'
                        {
                            if (!parser->curTransform)
                                parser->reportError(ERR_PARSER_CANNOTRECOVER,$1,"SKIP is only valid inside a TRANSFORM");
                            $$.setExpr(createDataset(no_skip, $3.getExpr()));
                        }
    | SOAPCALL '(' expression ',' expression ',' recordDef ',' DATASET '(' recordDef ')' ')'
                        {
                            parser->normalizeExpression($3);
                            parser->normalizeExpression($5);
                            parser->checkSoapRecord($7);
                            $$.setExpr(createDataset(no_soapcall, $3.getExpr(), createComma($5.getExpr(), $7.getExpr(), $11.getExpr())));
                            $$.setPosition($1);
                        }
    | SOAPCALL '(' expression ',' expression ',' recordDef ',' DATASET '(' recordDef ')' ',' soapFlags ')'
                        {
                            parser->normalizeExpression($3);
                            parser->normalizeExpression($5);
                            parser->checkSoapRecord($7);
                            $$.setExpr(createDataset(no_soapcall, $3.getExpr(), createComma($5.getExpr(), $7.getExpr(), $11.getExpr(), $14.getExpr())));
                            parser->checkOnFailRecord($$.queryExpr(), $1);
                            $$.setPosition($1);
                        }
    | SOAPCALL '(' expression ',' expression ',' recordDef ',' transform ',' DATASET '(' recordDef ')' ')'
                        {
                            parser->normalizeExpression($3);
                            parser->normalizeExpression($5);
                            $$.setExpr(createDataset(no_newsoapcall, $3.getExpr(), createComma($5.getExpr(), $7.getExpr(), $9.getExpr(), $13.getExpr())));
                            $$.setPosition($1);
                        }
    | SOAPCALL '(' expression ',' expression ',' recordDef ',' transform ',' DATASET '(' recordDef ')' ',' soapFlags ')'
                        {
                            parser->normalizeExpression($3);
                            parser->normalizeExpression($5);
                            $$.setExpr(createDataset(no_newsoapcall, $3.getExpr(), createComma($5.getExpr(), $7.getExpr(), $9.getExpr(), createComma($13.getExpr(), $16.getExpr()))));
                            parser->checkOnFailRecord($$.queryExpr(), $1);
                            $$.setPosition($1);
                        }
    | SOAPCALL '(' startTopLeftSeqFilter ',' expression ',' expression ',' recordDef ',' DATASET '(' recordDef ')' ')' endTopLeftFilter endSelectorSequence
                        {
                            parser->normalizeExpression($5);
                            parser->normalizeExpression($7);
                            parser->checkSoapRecord($9);
                            $$.setExpr(createDataset(no_soapcall_ds, $3.getExpr(), createComma($5.getExpr(), $7.getExpr(), $9.getExpr(), createComma($13.getExpr(), $17.getExpr()))));
                            $$.setPosition($1);
                        }
    | SOAPCALL '(' startTopLeftSeqFilter ',' expression ',' expression ',' recordDef ',' DATASET '(' recordDef ')' ',' soapFlags ')' endTopLeftFilter endSelectorSequence
                        {
                            parser->normalizeExpression($5);
                            parser->normalizeExpression($7);
                            parser->checkSoapRecord($9);
                            $$.setExpr(createDataset(no_soapcall_ds, $3.getExpr(), createComma($5.getExpr(), $7.getExpr(), $9.getExpr(), createComma($13.getExpr(), $16.getExpr(), $19.getExpr()))));
                            parser->checkOnFailRecord($$.queryExpr(), $1);
                            $$.setPosition($1);
                        }
    | SOAPCALL '(' startTopLeftSeqFilter ',' expression ',' expression ',' recordDef ',' transform ',' DATASET '(' recordDef ')' ')' endTopLeftFilter endSelectorSequence
                        {
                            parser->normalizeExpression($5);
                            parser->normalizeExpression($7);
                            $$.setExpr(createDataset(no_newsoapcall_ds, $3.getExpr(), createComma($5.getExpr(), $7.getExpr(), $9.getExpr(), createComma($11.getExpr(), $15.getExpr(), $19.getExpr()))));
                            $$.setPosition($1);
                        }
    | SOAPCALL '(' startTopLeftSeqFilter ',' expression ',' expression ',' recordDef ',' transform ',' DATASET '(' recordDef ')' ',' soapFlags ')' endTopLeftFilter endSelectorSequence
                        {
                            parser->normalizeExpression($5);
                            parser->normalizeExpression($7);
                            $$.setExpr(createDataset(no_newsoapcall_ds, $3.getExpr(), createComma($5.getExpr(), $7.getExpr(), $9.getExpr(), createComma($11.getExpr(), $15.getExpr(), $18.getExpr(), $21.getExpr()))));
                            parser->checkOnFailRecord($$.queryExpr(), $1);
                            $$.setPosition($1);
                        }
    | GLOBAL '(' dataSet globalOpts ')'
                        {
                            $$.setExpr(createDataset(no_globalscope, $3.getExpr(), $4.getExpr()));
                            $$.setPosition($1);
                        }
    | GLOBAL '(' dataSet ',' expression globalOpts ')'
                        {
                            parser->normalizeExpression($5, type_string, false);
                            $$.setExpr(createDataset(no_globalscope, $3.getExpr(), createComma($5.getExpr(), $6.getExpr())));
                            $$.setPosition($1);
                        }
    | LOCAL '(' dataSet ')'
                        {
                            $$.setExpr(createDataset(no_forcelocal, $3.getExpr()));
                            $$.setPosition($1);
                        }
    | NOLOCAL '(' dataSet ')'
                        {
                            $$.setExpr(createDataset(no_forcenolocal, $3.getExpr()));
                            $$.setPosition($1);
                        }
    | ALLNODES '(' beginList dataSet ignoreDummyList remoteOptions ')'
                        {
                            OwnedHqlExpr ds = $4.getExpr();
                            if (isGrouped(ds))
                                parser->reportError(ERR_REMOTE_GROUPED, $1, "ALLNODES() is not currently supported on grouped datasets");
                            HqlExprArray args;
                            args.append(*LINK(ds));
                            $6.unwindCommaList(args);
                            IHqlExpression * onFail = queryAttribute(onFailAtom, args);
                            if (onFail && !parser->checkTransformTypeMatch($4, ds, onFail->queryChild(0)))
                                args.zap(*onFail);
                            $$.setExpr(createDataset(no_allnodes, args));
                            $$.setPosition($1);
                        }
    | THISNODE '(' dataSet ')'
                        {
                            $$.setExpr(createDataset(no_thisnode, $3.getExpr()));
                            $$.setPosition($1);
                        }
    | DATASET '(' dataRow ')'
                        {
                            IHqlExpression * row = $3.getExpr();
                            $$.setExpr(createDatasetFromRow(row));
                            $$.setPosition($1);
                        }
    | DATASET '(' dictionary ')'
                        {
                            IHqlExpression * dictionary = $3.getExpr();
                            $$.setExpr(createDataset(no_datasetfromdictionary, dictionary), $1);
                        }
    | _EMPTY_ '(' recordDef ')'
                        {
                            IHqlExpression * record = $3.getExpr();
                            $$.setExpr(createDataset(no_null, record));
                            $$.setPosition($1);
                        }
    | __COMPOUND__ '(' action ',' dataSet ')'
                        {
                            if (parser->okToAddSideEffects($5.queryExpr()))
                            {
                                $$.setExpr(createCompound($3.getExpr(), $5.getExpr()));
                            }
                            else
                            {
                                $3.release();
                                $$.setExpr($5.getExpr());
                            }
                            $$.setPosition($1);
                        }
    | __COMMON__ '(' dataSet ')'
                        {
                            $$.setExpr(createAliasOwn($3.getExpr(), NULL));
                            $$.setPosition($1);
                        }
    | TOK_ASSERT '(' startTopFilter ',' expression assertFlags ')' endTopFilter
                        {
                            parser->normalizeExpression($5);
                            IHqlExpression * action = parser->createAssert($5, NULL, $6);
                            $$.setExpr(createDataset(no_assert_ds, $3.getExpr(), action));
                            $$.setPosition($1);
                        }
    | TOK_ASSERT '(' startTopFilter ',' expression ',' expression assertFlags ')' endTopFilter
                        {
                            parser->normalizeExpression($5);
                            parser->normalizeExpression($7);
                            IHqlExpression * action = parser->createAssert($5, &$7, $8);
                            $$.setExpr(createDataset(no_assert_ds, $3.getExpr(), action));
                            //MORE: This should possibly be a more general no_throughapply
                            $$.setPosition($1);
                        }
    | TOK_ASSERT '(' startTopFilter ',' assertActions ')' endTopFilter
                        {
                            $$.setExpr(createDataset(no_assert_ds, $3.getExpr(), $5.getExpr()));
                            //MORE: This should possibly be a more general no_throughapply
                            $$.setPosition($1);
                        }
    | ROWS '(' dataRow ')'
                        {
                            OwnedHqlExpr ds = $3.getExpr();
                            $$.setExpr(parser->resolveRows($1, ds), $1);
                        }
    | XMLPROJECT '(' expression ',' transform ')'
                        {
                            parser->normalizeExpression($3, type_string, false);
                            parser->validateXPath($3);
                            $$.setExpr(createDatasetF(no_xmlproject, $3.getExpr(), $5.getExpr(), parser->createUniqueId(), NULL));
                            $$.setPosition($1);
                        }
    | MAP '(' mapDatarowSpec ',' dataRow ')'
                        {
                            HqlExprArray args;
                            OwnedHqlExpr elseExpr = $5.getExpr();
                            $3.unwindCommaList(args);

                            parser->ensureMapToRecordsMatch(elseExpr, args, $5, true);

                            args.append(*elseExpr.getClear());
                            $$.setExpr(::createRow(no_map, args));
                            $$.setPosition($1);
                        }
    | CASE '(' expression ',' beginList caseDatarowSpec ',' dataRow ')'
                        {
                            parser->normalizeExpression($3, type_scalar, false);
                            HqlExprArray args;
                            OwnedHqlExpr elseExpr = $8.getExpr();
                            parser->endList(args);
                            parser->checkCaseForDuplicates(args, $6);

                            parser->ensureMapToRecordsMatch(elseExpr, args, $8, true);

                            args.add(*$3.getExpr(),0);
                            args.append(*elseExpr.getClear());
                            $$.setExpr(::createRow(no_case, args), $1);
                        }
    | WHEN '(' dataSet ',' action sideEffectOptions ')'
                        {
                            $$.setExpr(createDatasetF(no_executewhen, $3.getExpr(), $5.getExpr(), $6.getExpr(), NULL), $1);
                        }
    | SUCCESS '(' dataSet ',' action ')'
                        {
                            $$.setExpr(createDatasetF(no_executewhen, $3.getExpr(), $5.getExpr(), createAttribute(successAtom), NULL), $1);
                        }
    //Slightly unusual arrangement of the productions there to resolve s/r errors
    | AGGREGATE '(' startLeftDelaySeqFilter ',' startRightRowsRecord ',' transform beginList ')' endRowsGroup endSelectorSequence
                        {
                            $$.setExpr(parser->processUserAggregate($1, $3, $5, $7, NULL, NULL, $10, $11), $1);
                        }
    | AGGREGATE '(' startLeftDelaySeqFilter ',' startRightRowsRecord ',' transform beginList ',' sortList ')' endRowsGroup endSelectorSequence
                        {
                            $$.setExpr(parser->processUserAggregate($1, $3, $5, $7, NULL, &$10, $12, $13), $1);
                        }
    | AGGREGATE '(' startLeftDelaySeqFilter ',' startRightRowsRecord ',' transform beginList ',' transform ')' endRowsGroup endSelectorSequence
                        {
                            $$.setExpr(parser->processUserAggregate($1, $3, $5, $7, &$10, NULL, $12, $13), $1);
                        }
    | AGGREGATE '(' startLeftDelaySeqFilter ',' startRightRowsRecord ',' transform beginList ',' transform ',' sortList ')' endRowsGroup endSelectorSequence
                        {
                            $$.setExpr(parser->processUserAggregate($1, $3, $5, $7, &$10, &$12, $14, $15), $1);
                        }
/*
  //This may cause s/r problems with the attribute version if a dataset name clashes with a hint id
    | HINT '(' dataSet ','  hintList ')'
                        {
                            HqlExprArray hints;
                            $5.unwindCommaList(hints);
                            OwnedHqlExpr hint = createExprAttribute(hintAtom, hints);

                            //Add a hint attribute to the dataset
                            OwnedHqlExpr ds = $3.getExpr();
                            $$.setExpr(appendOwnedOperand(ds, hint.getClear()), $1);

                            //An alternative implementation is to add an annotation to the dataset, but these will then get commoned
                            //up, and generally I suspect hints should be treated as not commoned up.  Maybe there should be a flag!
                            //The code generator also doesn't yet preserve annotations from multiple branches.
                            //$$.setExpr(createMetaAnnotation($3.getExpr(), args), $1);
                        }

*/
    ;

    
dataSetList
    : dataSet
    | dataSet ',' dataSetList
                        {
                            $$.setExpr(createComma($1.getExpr(), $3.getExpr()));
                            $$.setPosition($1);
                        }
    ;

mergeDataSetList
    : mergeDataSetItem
    | mergeDataSetList ',' mergeDataSetItem
                        {
                            $$.setExpr(createComma($1.getExpr(), $3.getExpr()));
                            $$.setPosition($1);
                        }
    ;

mergeDataSetItem
    : dataSet
    | commonAttribute
    | DEDUP             {
                            $$.setExpr(createAttribute(dedupAtom));
                            $$.setPosition($1);
                        }
    | SORTED '(' startSortOrder heterogeneous_expr_list ')' endSortOrder
                        {
                            HqlExprArray args;
                            $4.unwindCommaList(args);
                            $$.setExpr(createExprAttribute(sortedAtom, args));
                            $$.setPosition($1);
                        }
    ;

cogroupDataSetList
    : cogroupDataSetItem
    | cogroupDataSetList ',' cogroupDataSetItem
                        {
                            $$.setExpr(createComma($1.getExpr(), $3.getExpr()));
                            $$.setPosition($1);
                        }
    ;

cogroupDataSetItem
    : dataSet
    | commonAttribute
    | GROUPBY '(' beginList sortList  ')'
                        {
                            HqlExprArray sortItems;
                            parser->endList(sortItems);
                            IHqlExpression * sortlist = parser->processSortList($4, no_sortlist, NULL, sortItems, NULL, NULL);
                            $$.setExpr(createExprAttribute(groupAtom, sortlist), $1);
                        }
    ;

sideEffectOptions
    :
                        {
                            $$.setNullExpr();
                        }
    | ',' BEFORE
                        {
                            $$.setExpr(createAttribute(beforeAtom), $2);
                        }
    | ',' SUCCESS
                        {
                            $$.setExpr(createAttribute(successAtom), $2);
                        }
    | ',' FAILURE
                        {
                            $$.setExpr(createAttribute(failureAtom), $2);
                        }
    | ',' PARALLEL
                        {
                            $$.setExpr(createAttribute(parallelAtom), $2);
                        }
    ;

limitOptions
    :                   {   $$.setNullExpr(); }
    | ',' limitOption limitOptions
                        {
                            $$.setExpr(createComma($2.getExpr(), $3.getExpr()));
                        }
    ;

limitOption
    : KEYED             {   $$.setExpr(createAttribute(keyedAtom)); $$.setPosition($1); }
    | COUNT             {   $$.setExpr(createAttribute(countAtom)); $$.setPosition($1); }
    | SKIP              {   $$.setExpr(createAttribute(skipAtom)); $$.setPosition($1); }
    | ONFAIL '(' transform ')'
                        {
                            $$.setExpr(createExprAttribute(onFailAtom, $3.getExpr()));
                            $$.setPosition($1);
                        }
    | failAction
    | commonAttribute
    ;

catchOption
    : SKIP              {   $$.setExpr(createAttribute(skipAtom)); $$.setPosition($1); }
    | ONFAIL '(' transform ')'
                        {
                            $$.setExpr(createExprAttribute(onFailAtom, $3.getExpr()));
                            $$.setPosition($1);
                        }
    | failAction
    ;

projectOptions
    :                       { $$.setNullExpr(); }
    | projectOptions ',' projectOption
                        {
                            $$.setExpr(createComma($1.getExpr(), $3.getExpr()));
                        }
    ;

projectOption
    : commonAttribute
    | prefetchAttribute
    | KEYED
                        {
                            $$.setExpr(createAttribute(keyedAtom));
                            $$.setPosition($1);
                        }
    ;

prefetchAttribute
    : PREFETCH
                        {
                            $$.setExpr(createExprAttribute(prefetchAtom));
                            $$.setPosition($1);
                        }
    | PREFETCH '(' expression ')'
                        {
                            parser->normalizeExpression($3, type_int, false);
                            $$.setExpr(createExprAttribute(prefetchAtom, $3.getExpr()));
                            $$.setPosition($1);
                        }
    | PREFETCH '(' expression ',' PARALLEL ')'
                        {
                            parser->normalizeExpression($3, type_int, false);
                            $$.setExpr(createExprAttribute(prefetchAtom, $3.getExpr(), createExprAttribute(parallelAtom)));
                            $$.setPosition($1);
                        }
    ;

loopOptions
    :                   {
                            $$.setNullExpr();
                        }
    | loopOptions ',' loopOption
                        {
                            $$.setExpr(createComma($1.getExpr(), $3.getExpr()));
                        }
    ;

loopOption
    : PARALLEL '(' expression ')'
                        {
                            parser->normalizeExpression($3);    // could check more...
                            $$.setExpr(createExprAttribute(parallelAtom, $3.getExpr()));
                            $$.setPosition($1);
                        }
    | PARALLEL '(' expression ',' expression ')'
                        {
                            parser->normalizeExpression($3, type_set, false);
                            parser->normalizeExpression($5, type_int, false);
                            $$.setExpr(createExprAttribute(parallelAtom, $3.getExpr(), $5.getExpr()));
                            $$.setPosition($1);
                        }
    | commonAttribute
    ;

graphOptions
    :                   {
                            $$.setNullExpr();
                        }
    | graphOptions ',' graphOption
                        {
                            $$.setExpr(createComma($1.getExpr(), $3.getExpr()));
                        }
    ;

graphOption
    : PARALLEL
                        {
                            $$.setExpr(createExprAttribute(parallelAtom));
                            $$.setPosition($1);
                        }
    | commonAttribute
    ;

remoteOptions
    :
                        {
                            $$.setNullExpr();
                        }
    | ',' LIMIT '(' expression limitOptions ')'
                        {
                            parser->normalizeExpression($4, type_int, false);
                            $$.setExpr(createExprAttribute(rowLimitAtom, $4.getExpr(), $5.getExpr()));
                            $$.setPosition($2);
                        }
    ;

distributedFlags
    :                   {
                            $$.setNullExpr();
                        }
    | distributedFlags ',' distributedFlag
                        {
                            $$.setExpr(createComma($1.getExpr(), $3.getExpr()));
                        }
    ;

distributedFlag
    : TOK_ASSERT        {
                            $$.setExpr(createAttribute(assertAtom));
                            $$.setPosition($1);
                        }
    ;

optStepFlags
    :                   { $$.setNullExpr(); }
    | ',' stepFlag optStepFlags
                        {
                            $$.setExpr(createComma($2.getExpr(), $3.getExpr()), $2);
                        }
    ;

stepFlag
    : PRIORITY '(' expression ')'
                        {
                            parser->normalizeExpression($3, type_real, false);
                            $$.setExpr(createExprAttribute(priorityAtom, $3.getExpr()), $1);
                        }
    | PREFETCH
                        {
                            $$.setExpr(createExprAttribute(prefetchAtom), $1);
                        }
    | PREFETCH '(' expression ')'
                        {
                            parser->normalizeExpression($3, type_int, false);
                            $$.setExpr(createExprAttribute(prefetchAtom, $3.getExpr()), $1);
                        }
    | FILTERED
                        {
                            $$.setExpr(createExprAttribute(filteredAtom), $1);
                        }
    | hintAttribute
    ;


sectionArguments
    :                   {   $$.setNullExpr(); }
    | ',' sectionArgument sectionArguments
                        {
                            $$.setExpr(createComma($2.getExpr(), $3.getExpr()));
                            $$.setPosition($1);
                        }
    ;

sectionArgument
    : dataSet
    | GRAPH
                        {
                            $$.setExpr(createAttribute(graphAtom), $1);
                        }
    | PRIVATE
                        {
                            $$.setExpr(createAttribute(privateAtom), $1);
                        }
    ;

enumDef
    : enumBegin enumFirst enumValues ')'
                        {
                            $$.setExpr(parser->leaveEnum($1), $1);
                        }
    ;

enumBegin
    : ENUM '('
                        {
                            OwnedITypeInfo type = makeIntType(4, false);
                            parser->enterEnum($1, type);
                            $$.clear();
                        }
    ;

enumFirst
    : enumValue
    | scalarType
                        {
                            OwnedITypeInfo type = $1.getType();
                            parser->setEnumType($1, type);
                            $$.clear();
                        }
    ;


enumValues
    :
    | enumValues ',' enumValue
    ;

enumValue
    : UNKNOWN_ID
                        {
                            parser->processEnum($1, NULL);
                            $$.clear();
                        }
    | UNKNOWN_ID EQ expression
                        {
                            parser->normalizeExpression($3, type_numeric, false);
                            OwnedHqlExpr nextValue = $3.getExpr();
                            parser->processEnum($1, nextValue);
                            $$.clear();
                        }
    | SCOPE_ID
                        {
                            $1.setId(parser->getNameFromExpr($1));
                            parser->processEnum($1, NULL);
                            $$.clear();
                        }
    | SCOPE_ID EQ expression
                        {
                            $1.setId(parser->getNameFromExpr($1));
                            parser->normalizeExpression($3, type_numeric, false);
                            OwnedHqlExpr nextValue = $3.getExpr();
                            parser->processEnum($1, nextValue);
                            $$.clear();
                        }
    ;

indexTopRecordAndName
    : optRecordDef ',' thorFilenameOrList
                        {
                            parser->normalizeExpression($3, type_string, false);
                            OwnedHqlExpr record = $1.getExpr();
                            OwnedHqlExpr name = $3.getExpr();
                            if (record->getOperator() == no_null)
                            {
                                if (parser->topScopes.ordinality())
                                    record.setown(parser->createRecordFromDataset(&parser->topScopes.tos()));
                                else
                                {
                                    parser->reportError(ERR_RECORD_EMPTYDEF, $1, "Record cannot be omitted when dataset not supplied");
                                    record.set(queryNullRecord());
                                }
                            }

                            parser->pushTopScope(record);
                            $$.setExpr(createComma(record.getClear(), name.getClear()));
                        }
    | optRecordDef ',' transform ',' thorFilenameOrList
                        {
                            parser->normalizeExpression($5, type_string, false);
                            OwnedHqlExpr record = $1.getExpr();
                            OwnedHqlExpr transform = $3.getExpr();
                            OwnedHqlExpr name = $5.getExpr();
                            if (record->getOperator() == no_null)
                            {
                                if (parser->topScopes.ordinality())
                                    record.setown(parser->createRecordFromDataset(&parser->topScopes.tos()));
                                else
                                {
                                    parser->reportError(ERR_RECORD_EMPTYDEF, $1, "Record cannot be omitted when dataset not supplied");
                                    record.set(queryNullRecord());
                                }
                            }

                            parser->pushTopScope(record);
                            $$.setExpr(createComma(record.getClear(), transform.getClear(), name.getClear()));
                        }
    | optRecordDef ',' nullRecordDef ',' thorFilenameOrList
                        {
                            parser->normalizeExpression($5, type_string, false);
                            OwnedHqlExpr record = $1.getExpr();
                            if (record->getOperator() == no_null)
                            {
                                parser->reportError(ERR_RECORD_EMPTYDEF, $1, "Record cannot be omitted");
                                record.set(queryNullRecord());
                            }
                            OwnedHqlExpr payload = $3.getExpr();
                            OwnedHqlExpr extra = $5.getExpr();
                            parser->modifyIndexPayloadRecord(record, payload, extra, $1);

                            parser->pushTopScope(record);
                            $$.setExpr(createComma(record.getClear(), extra.getClear()));
                        }
    ;


nullRecordDef
    : recordDef
    | startrecord endrecord
                        {
                            $$.setExpr($2.getExpr());
                        }
    ;

failDatasetParam
    : ',' beginList expression ignoreDummyList ',' expression
                        {
                            parser->normalizeExpression($3, type_int, false);
                            parser->normalizeExpression($6, type_string, false);
                            $$.setExpr(createValue(no_fail, makeVoidType(), $3.getExpr(), $6.getExpr()));
                        }
    | ',' beginList expression ignoreDummyList
                        {
                            parser->normalizeExpression($3, type_scalar, false);
                            parser->checkIntegerOrString($3);
                            $$.setExpr(createValue(no_fail, makeVoidType(), $3.getExpr()));
                        }
    | ',' beginList actionlist  
                        {
                            HqlExprArray actions;
                            parser->endList(actions);
                            $$.setExpr(createActionList(actions), $1);
                        }
    ;

mode
    : FLAT              {   $$.setExpr(createValue(no_flat));   }
    | CSV               {   $$.setExpr(createValue(no_csv));    }
    | CSV '(' csvOptions ')'
                        {   
                            HqlExprArray args;
                            $3.unwindCommaList(args);
                            $$.setExpr(createValue(no_csv, makeNullType(), args));
                        }
    | SQL               {   $$.setExpr(createValue(no_sql));    }
    | THOR              {   $$.setExpr(createValue(no_thor));   }
    | THOR  '(' expression ')'
                        {
                            throwUnexpected();
                            parser->normalizeExpression($3);
                            $$.setExpr(createValue(no_thor, $3.getExpr()));
                        }
    | XML_TOKEN         {   $$.setExpr(createValue(no_xml));    }
    | XML_TOKEN '(' xmlOptions ')'
                        {
                            HqlExprArray args;
                            $3.unwindCommaList(args);

                            //Create expression in a form that is backward compatible
                            IHqlExpression * name = queryAttribute(rowAtom, args);
                            if (name)
                            {
                                args.add(*LINK(name->queryChild(0)), 0);
                                args.zap(*name);
                            }
                            else
                                args.add(*createConstant("xml"), 0);
                            $$.setExpr(createValue(no_xml, makeNullType(), args));
                        }
    | JSON_TOKEN         {   $$.setExpr(createValue(no_json));    }
    | JSON_TOKEN '(' xmlOptions ')'
                        {
                            HqlExprArray args;
                            $3.unwindCommaList(args);

                            //Create expression in a form that is backward compatible
                            IHqlExpression * name = queryAttribute(rowAtom, args);
                            if (name)
                            {
                                args.add(*LINK(name->queryChild(0)), 0);
                                args.zap(*name);
                            }
                            else
                                args.add(*createConstant("json"), 0);
                            $$.setExpr(createValue(no_json, makeNullType(), args));
                        }
    | pipe
    ;

dsOption
    : OPT               {   $$.setExpr(createAttribute(optAtom)); }
    | UNSORTED          {   $$.setExpr(createAttribute(unsortedAtom)); }
    | RANDOM            {   $$.setExpr(createAttribute(randomAtom)); }
    | SEQUENTIAL        {   $$.setExpr(createAttribute(sequentialAtom)); }
    | TOK_BITMAP        {   $$.setExpr(createAttribute(bitmapAtom)); }
    | __COMPRESSED__    {   $$.setExpr(createAttribute(__compressed__Atom)); }
    | __GROUPED__       {   $$.setExpr(createAttribute(groupedAtom)); }
    | PRELOAD           {   $$.setExpr(createAttribute(preloadAtom)); }
    | PRELOAD '(' constExpression ')'
                        {   $$.setExpr(createExprAttribute(preloadAtom, $3.getExpr())); }
    | ENCRYPT '(' expression ')'
                        {
                            parser->normalizeExpression($3, type_data, false);
                            $$.setExpr(createExprAttribute(encryptAtom, $3.getExpr()));
                            $$.setPosition($1);
                        }
    | DYNAMIC
                        {
                            $$.setExpr(createAttribute(dynamicAtom));
                            $$.setPosition($1);
                        }
    | COUNT '(' constExpression ')'
                        {   $$.setExpr(createExprAttribute(countAtom, $3.getExpr()), $1); }
    | MAXCOUNT '(' constExpression ')'
                        {   $$.setExpr(createExprAttribute(maxCountAtom, $3.getExpr()), $1); }
    | AVE '(' constExpression ')'
                        {   $$.setExpr(createExprAttribute(aveAtom, $3.getExpr()), $1); }
    | commonAttribute
    ;

dsOptions
    : dsOption
    | dsOptions ',' dsOption    
                        {   $$.setExpr(createComma($1.getExpr(), $3.getExpr())); }
    ;

optDsOptions
    :                   {   $$.setNullExpr(); }
    | ',' dsOptions     {   $$.setExpr($2.getExpr()); }
    ;

thorFilenameOrList
    : expression        {
                            parser->convertAllToAttribute($1);
                            $$.inherit($1);
                        }
    | DYNAMIC '(' expression ')'
                        {
                            parser->normalizeExpression($3, type_string, false);
                            $$.setExpr(createComma($3.getExpr(), createAttribute(dynamicAtom)));
                            $$.setPosition($1);
                        }
    ;

indexListOpt
    :                   { $$.setNullExpr(); }
    | indexListOpt ',' dataSet
                        {
                            IHqlExpression * index = $3.getExpr();
                            if (!isKey(index))
                                parser->reportError(ERR_EXPECTED_INDEX, $3, "Expected INDEX as parameter to KEYED");
                            $$.setExpr(createComma($1.getExpr(), index));
                        }
    ;

csvOptions
    : csvOption
    | csvOption ',' csvOptions
                        {   $$.setExpr(createComma($1.getExpr(), $3.getExpr())); }
    ;

csvOption
    : EBCDIC            {
                            $$.setExpr(createAttribute(ebcdicAtom));
                            $$.setPosition($1);
                        }
    | ASCII             {
                            $$.setExpr(createAttribute(asciiAtom));
                            $$.setPosition($1);
                        }
    | SIMPLE_TYPE       {
                            Owned<ITypeInfo> type = $1.getType();
                            if (type->getTypeCode() != type_unicode && type->getTypeCode() != type_utf8)
                                parser->reportError(ERR_EXPECTED, $1, "Expected UNICODE");
                            $$.setExpr(createAttribute(unicodeAtom));
                            $$.setPosition($1);
                        }
    | HEADING
                        {
                            $$.setExpr(createAttribute(headingAtom));
                            $$.setPosition($1);
                        }
    | HEADING '(' startHeadingAttrs headingOptions ')'
                        {
                            HqlExprArray args;
                            $4.unwindCommaList(args);
                            HqlExprArray orderedArgs;
                            reorderAttributesToEnd(orderedArgs, args);
                            $$.setExpr(createExprAttribute(headingAtom, orderedArgs), $1);
                            $$.setPosition($1);
                        }
    | MAXLENGTH '(' constExpression ')'
                        {
                            $$.setExpr(createExprAttribute(maxLengthAtom, $3.getExpr()));
                            $$.setPosition($1);
                        }
    | MAXSIZE '(' constExpression ')'
                        {
                            $$.setExpr(createExprAttribute(maxLengthAtom, $3.getExpr()));
                            $$.setPosition($1);
                        }
    | QUOTE '(' expression ')'
                        {
                            parser->normalizeExpression($3);
                            $$.setExpr(createExprAttribute(quoteAtom, $3.getExpr()));
                            $$.setPosition($1);
                        }
    | SEPARATOR '(' expression ')'
                        {
                            parser->normalizeExpression($3);
                            $$.setExpr(createExprAttribute(separatorAtom, $3.getExpr()));
                            $$.setPosition($1);
                        }
    | TERMINATOR '(' expression ')'
                        {
                            parser->normalizeExpression($3);
                            $$.setExpr(createExprAttribute(terminatorAtom, $3.getExpr()));
                            $$.setPosition($1);
                        }
    | TERMINATOR '(' expression ',' QUOTE ')'
                        {
                            parser->normalizeExpression($3);
                            $$.setExpr(createExprAttribute(terminatorAtom, createComma($3.getExpr(), createAttribute(quoteAtom))));
                            $$.setPosition($1);
                        }
    | ESCAPE '(' expression ')'
                        {
                            parser->normalizeExpression($3);
                            $$.setExpr(createExprAttribute(escapeAtom, $3.getExpr()));
                            $$.setPosition($1);
                        }
    | NOTRIM
                        {
                            $$.setExpr(createAttribute(noTrimAtom));
                            $$.setPosition($1);
                        }
    ;

headingOptions
    : headingOption
    | headingOptions ',' headingOption
                        {
                            $$.setExpr(createComma($1.getExpr(), $3.getExpr()), $1);
                        }
    ;

headingOption
    : SINGLE            {   $$.setExpr(createAttribute(singleAtom), $1); }
    | MANY              {   $$.setExpr(createAttribute(manyAtom), $1); }
    | expression
                        {
                            parser->normalizeExpression($1);
                            $$.inherit($1);
                        }
    | FORMAT_ATTR '(' valueFunction ')'
                        {
                            //MORE: This really should check the prototype matches what we expect
                            $$.setExpr(createExprAttribute(formatAtom, $3.getExpr()), $1);
                        }
    ;

xmlOptions
    : xmlOption
    | xmlOptions ',' xmlOption
                        {   $$.setExpr(createComma($1.getExpr(), $3.getExpr())); }
    ;

xmlOption
    : MAXLENGTH '(' constExpression ')'
                        {
                            $$.setExpr(createExprAttribute(maxLengthAtom, $3.getExpr()));
                        }
    | MAXSIZE '(' constExpression ')'
                        {
                            $$.setExpr(createExprAttribute(maxLengthAtom, $3.getExpr()));
                        }
    | NOROOT            {   $$.setExpr(createAttribute(noRootAtom)); }
    | HEADING '(' expression optCommaExpression ')'
                        {
                            parser->normalizeExpression($3, type_string, false);
                            if ($4.queryExpr())
                                parser->normalizeExpression($4, type_string, false);
                            $$.setExpr(createExprAttribute(headingAtom, $3.getExpr(), $4.getExpr()));
                            $$.setPosition($1);
                        }
    | expression        {
                            parser->normalizeExpression($1, type_string, false);
                            $$.setExpr(createExprAttribute(rowAtom, $1.getExpr()));
                            $$.setPosition($1);
                        }
    | TRIM              {
                            $$.setExpr(createAttribute(trimAtom));
                            $$.setPosition($1);
                        }
    | OPT               {
                            $$.setExpr(createAttribute(optAtom));
                            $$.setPosition($1);
                        }
    ;

optPipeOptions
    :                   {   $$.setNullExpr(); }
    | ',' pipeOptions   {   $$.inherit($2); }

pipeOptions
    : pipeOption
    | pipeOptions ',' pipeOption
                        {   $$.setExpr(createComma($1.getExpr(), $3.getExpr()), $1); }
    ;

pipeOption
    : REPEAT            {   $$.setExpr(createAttribute(repeatAtom)); }
    | pipeFormatOption
    | OUTPUT '(' pipeFormatOption ')'   
                        {
                            $$.setExpr(createExprAttribute(outputAtom, $3.getExpr()), $1);
                        }
    | GROUP             {   $$.setExpr(createAttribute(groupAtom)); }
    | OPT               {   $$.setExpr(createAttribute(optAtom)); }
    ;
    
pipeFormatOption
    : CSV               {
                            $$.setExpr(createAttribute(csvAtom), $1);
                        }
    | CSV '(' csvOptions ')'
                        {
                            HqlExprArray args;
                            $3.unwindCommaList(args);
                            $$.setExpr(createExprAttribute(csvAtom, args), $1);
                        }
    | XML_TOKEN         {
                            $$.setExpr(createAttribute(xmlAtom), $1);
                        }
    | XML_TOKEN '(' xmlOptions ')'
                        {
                            HqlExprArray args;
                            $3.unwindCommaList(args);
                            $$.setExpr(createExprAttribute(xmlAtom, args), $1);
                        }
    | FLAT              {   $$.setExpr(createAttribute(flatAtom), $1);   }
    | THOR              {   $$.setExpr(createAttribute(thorAtom), $1);   }
    ;

setCountList
    : mapSpec
    | mapSpec ',' expression
                        { 
                            parser->normalizeExpression($3, type_int, false);
                            $$.setExpr(createComma($1.getExpr(), $3.getExpr()));
                        }
    | mapSpec ',' choosesetAttr
                        { 
                            $$.setExpr(createComma($1.getExpr(), $3.getExpr()));
                        }
    | mapSpec ',' expression ',' choosesetAttr
                        { 
                            parser->normalizeExpression($3, type_int, false);
                            $$.setExpr(createComma($1.getExpr(), createComma($3.getExpr(), $5.getExpr()))); 
                        }
    ;

choosesetAttr
    : EXCLUSIVE         { $$.setExpr(createAttribute(exclusiveAtom)); }
    | ENTH              { $$.setExpr(createAttribute(enthAtom)); }
    | LAST              { $$.setExpr(createAttribute(lastAtom)); }
    ;


pipe
    : PIPE '(' expression optPipeOptions ')'    
                        {   
                            parser->normalizeExpression($3, type_string, false);
                            $$.setExpr(createComma(createValue(no_pipe, makeNullType(), $3.getExpr()), $4.getExpr()));
                        }
    ;


choosenExtra
    :                   {
                            $$.setNullExpr();
                        }
    | ',' choosenFlags
                        {
                            $$.inherit($2);
                        }
    | ',' expression
                        {
                            parser->normalizeExpression($2, type_int, false);
                            $$.inherit($2);
                        }
    | ',' expression ',' choosenFlags
                        {
                            parser->normalizeExpression($2, type_int, false);
                            $$.setExpr(createComma($2.getExpr(), $4.getExpr()));
                            $$.setPosition($2);
                        }
    ;

choosenFlags
    : choosenFlag
    | choosenFlags ',' choosenFlag
                        {
                            $$.setExpr(createComma($1.getExpr(), $3.getExpr()));
                            $$.setPosition($1);
                        }
    ;

choosenFlag
    : GROUPED
                        {
                            $$.setExpr(createAttribute(groupedAtom));
                            $$.setPosition($1);
                        }
    | commonAttribute
    | FEW
                        {
                            $$.setExpr(createAttribute(fewAtom));
                            $$.setPosition($1);
                        }
    ;

inlineFieldValue2
    : expression
                        {
                            parser->normalizeExpression($1);
                            $$.inherit($1);
                        }
    | dataSet
    | inlineDatasetValue
    | dataRow
    | '[' beginList inlineDatasetValueList ']'
                        {
                            HqlExprArray values;
                            parser->endList(values);
                            $$.setExpr(createValue(no_recordlist, makeNullType(), values));
                        }
    ;

inlineFieldValue
    : inlineFieldValue2 {
                            parser->addListElement($1.getExpr());
                            $$.clear();
                        }
    ;

inlineFieldValueGoesTo
        : GOESTO        {
                            parser->addListElement(createAttribute(_payload_Atom));
                            $$.clear();
                            $$.setPosition($1);
                        }
        ;

inlineFieldValues
    : inlineFieldValue
    | inlineFieldValues ';' inlineFieldValue
    | inlineFieldValues ',' inlineFieldValue
    ;

inlineFieldValuesWithGoesto
    : inlineFieldValues optSemiComma
    | inlineFieldValues inlineFieldValueGoesTo  inlineFieldValues optSemiComma
    ;

inlineDatasetValue
    : '{' beginList inlineFieldValuesWithGoesto  '}'
                        {
                            HqlExprArray args;
                            parser->endList(args);
                            setPayloadAttribute(args);
//                          args.append(*createLocationAttr($1));           // improves the error reporting, but slows it down, and changes the expression crcs
                            $$.setExpr(createValue(no_rowvalue, makeNullType(), args));
                        }
    ;

inlineDatasetValueList
    : inlineDatasetValue 
                        {   parser->addListElement($1.getExpr()); $$.clear(); }
    | simpleRecord 
                        {   parser->addListElement($1.getExpr()); $$.clear(); }
    | inlineDatasetValueList ',' inlineDatasetValue 
                        {   parser->addListElement($3.getExpr()); $$.clear(); }
    | inlineDatasetValueList ',' simpleRecord           
                        {   parser->addListElement($3.getExpr()); $$.clear(); }
    ;

transformList
    : transform
                        {   
                            parser->addListElement($1.getExpr()); 
                            $$.clear(); 
                        }
    | transformList ',' transform
                        {   
                            parser->addListElement($3.getExpr()); 
                            $$.clear(); 
                        }
    ;

optJoinFlags
    :                   {   $$.setNullExpr(); }
    | ',' JoinFlags     {   $$.setExpr($2.getExpr()); }
    ;

JoinFlags
    : JoinFlag
    | JoinFlags ',' JoinFlag    
                        {   $$.setExpr(createComma($1.getExpr(), $3.getExpr()), $1); }
    ;

JoinFlag
    : LEFT OUTER        {   $$.setExpr(createAttribute(leftouterAtom)); $$.setPosition($1); }
    | RIGHT OUTER       {   $$.setExpr(createAttribute(rightouterAtom)); $$.setPosition($1); }
    | LEFT RIGHT OUTER  {   $$.setExpr(createAttribute(fullouterAtom)); $$.setPosition($1); }
    | FULL OUTER        {   $$.setExpr(createAttribute(fullouterAtom)); $$.setPosition($1); }
    | LEFT ONLY         {   $$.setExpr(createAttribute(leftonlyAtom)); $$.setPosition($1); }
    | ONLY LEFT         {   $$.setExpr(createAttribute(leftonlyAtom)); $$.setPosition($1); }
    | RIGHT ONLY        {   $$.setExpr(createAttribute(rightonlyAtom)); $$.setPosition($1); }
    | ONLY RIGHT        {   $$.setExpr(createAttribute(rightonlyAtom)); $$.setPosition($1); }
    | LEFT RIGHT ONLY   {   $$.setExpr(createAttribute(fullonlyAtom)); $$.setPosition($1); }
    | FULL ONLY         {   $$.setExpr(createAttribute(fullonlyAtom)); $$.setPosition($1); }
    | INNER             {   $$.setExpr(createAttribute(innerAtom)); $$.setPosition($1); }
    | HASH              {
                            $$.setExpr(createAttribute(hashAtom));
                            $$.setPosition($1);
                        }
    | KEYED '(' dataSet ')'
                        {
                            IHqlExpression * index = $3.getExpr();
                            if (!isKey(index))
                                parser->reportError(ERR_EXPECTED_INDEX, $3, "Expected INDEX as parameter to KEYED");
                            $$.setExpr(createExprAttribute(keyedAtom, index));
                            $$.setPosition($1);
                        }
    | KEYED
                        {
                            $$.setExpr(createAttribute(keyedAtom));
                            $$.setPosition($1);
                        }
    | FEW
                        {
                            $$.setExpr(createAttribute(fewAtom));
                            $$.setPosition($1);
                        }
    | commonAttribute
    | MANY LOOKUP       {   $$.setExpr(createComma(createAttribute(lookupAtom), createAttribute(manyAtom))); $$.setPosition($1); }
    | GROUPED           {   $$.setExpr(createComma(createAttribute(lookupAtom), createAttribute(manyAtom), createAttribute(groupedAtom))); $$.setPosition($1); }
    | MANY              {   $$.setExpr(createAttribute(manyAtom)); $$.setPosition($1); }
    | LOOKUP            {   $$.setExpr(createAttribute(lookupAtom)); $$.setPosition($1); }
    | SMART             {   $$.setExpr(createAttribute(smartAtom)); $$.setPosition($1); }
    | NOSORT            {   $$.setExpr(createAttribute(noSortAtom)); $$.setPosition($1); }
    | NOSORT '(' LEFT ')'
                        {   $$.setExpr(createAttribute(noSortAtom, createAttribute(leftAtom))); $$.setPosition($1); }
    | NOSORT '(' RIGHT ')' 
                        {   $$.setExpr(createAttribute(noSortAtom, createAttribute(rightAtom))); $$.setPosition($1); }
    | ATMOST '(' expression ')'
                        {
                            parser->normalizeExpression($3, type_numeric, false);
                            if ($3.isZero())
                                parser->reportError(ERR_BAD_JOINFLAG, $3, "ATMOST(0) doesn't make any sense");
                            $$.setExpr(createExprAttribute(atmostAtom, $3.getExpr()));
                            $$.setPosition($1);
                        }
    | ATMOST '(' expression ',' expression ')'
                        {
                            parser->normalizeExpression($3, type_boolean, false);
                            parser->normalizeExpression($5, type_numeric, false);
                            if ($5.isZero())
                                parser->reportError(ERR_BAD_JOINFLAG, $5, "ATMOST(0) doesn't make any sense");
                            $$.setExpr(createExprAttribute(atmostAtom, $3.getExpr(), $5.getExpr()));
                            $$.setPosition($1);
                        }
    | ATMOST '(' expression ',' sortListExpr ',' expression ')'
                        {
                            parser->normalizeExpression($3, type_boolean, false);
                            parser->normalizeExpression($7, type_numeric, false);
                            if ($7.isZero())
                                parser->reportError(ERR_BAD_JOINFLAG, $7, "ATMOST(0) doesn't make any sense");
                            $$.setExpr(createExprAttribute(atmostAtom, $3.getExpr(), $5.getExpr(), $7.getExpr()));
                            $$.setPosition($1);
                        }
    | ATMOST '(' sortListExpr ',' expression ')'
                        {
                            parser->normalizeExpression($5, type_numeric, false);
                            if ($5.isZero())
                                parser->reportError(ERR_BAD_JOINFLAG, $5, "ATMOST(0) doesn't make any sense");
                            $$.setExpr(createExprAttribute(atmostAtom, $3.getExpr(), $5.getExpr()));
                            $$.setPosition($1);
                        }
    | KEEP '(' expression ')'
                        {
                            parser->normalizeExpression($3, type_numeric, false);
                            $$.setExpr(createExprAttribute(keepAtom, $3.getExpr()));
                            $$.setPosition($1);
                        }
    | PARTITION LEFT
                        {
                            $$.setExpr(createAttribute(partitionLeftAtom));
                            $$.setPosition($1);
                        }
    | PARTITION RIGHT
                        {
                            $$.setExpr(createAttribute(partitionRightAtom));
                            $$.setPosition($1);
                        }
    | THRESHOLD '(' expression ')'
                        {
                            parser->normalizeExpression($3, type_numeric, true);
                            $$.setExpr(createAttribute(thresholdAtom, $3.getExpr()));
                            $$.setPosition($1);
                        }
    | ALL               {   $$.setExpr(createAttribute(allAtom)); }
    | skewAttribute
    | LIMIT '(' expression joinLimitOptions ')'
                        {
                            parser->normalizeExpression($3, type_numeric, false);

                            HqlExprArray args;
                            args.append(*$3.getExpr());
                            $4.unwindCommaList(args);

                            IHqlExpression * onFail = queryAttribute(onFailAtom, args);
                            if (onFail)
                            {
                                onFail->Link();
                                args.zap(*onFail);
                            }
                            IHqlExpression * attr = createExprAttribute(rowLimitAtom, args);
                            $$.setExpr(createComma(attr, onFail));
                            $$.setPosition($1);
                        }
    | onFailAction
    | PARALLEL
                        {
                            $$.setExpr(createAttribute(parallelAtom));
                            $$.setPosition($1);
                        }
    | SEQUENTIAL
                        {
                            $$.setExpr(createAttribute(sequentialAtom));
                            $$.setPosition($1);
                        }
    | UNORDERED
                        {
                            $$.setExpr(createAttribute(unorderedAtom));
                            $$.setPosition($1);
                        }
    | GROUP '(' startSortOrder heterogeneous_expr_list ')' endSortOrder
                        {
                            HqlExprArray args;
                            $4.unwindCommaList(args);
                            OwnedHqlExpr sortlist = createSortList(args);
                            OwnedHqlExpr groupAttr = createExprAttribute(groupAtom, sortlist.getClear());
                            OwnedHqlExpr impliedAttr = createComma(createAttribute(lookupAtom), createAttribute(manyAtom));
                            $$.setExpr(createComma(groupAttr.getClear(), impliedAttr.getClear()), $1);
                        }
    | STABLE
                        {
                            $$.setExpr(createExprAttribute(stableAtom));
                            $$.setPosition($1);
                        }
    | UNSTABLE
                        {
                            $$.setExpr(createExprAttribute(unstableAtom));
                            $$.setPosition($1);
                        }
    | STREAMED          {   $$.setExpr(createAttribute(streamedAtom)); $$.setPosition($1); }
    ;


joinLimitOptions
    :                   {   $$.setNullExpr(); }
    | joinLimitOptions ',' joinLimitOption
                        {   $$.setExpr(createComma($1.getExpr(), $3.getExpr())); }
    ;

joinLimitOption
    : SKIP              { $$.setExpr(createAttribute(skipAtom)); }
    | COUNT             { $$.setExpr(createAttribute(countAtom)); }
    | failAction
    | transform
                        {
                            $$.setExpr(createExprAttribute(onFailAtom, $1.getExpr()));
                            $$.setPosition($1);
                        }

    ;

mergeJoinFlags
    : mergeJoinFlag
    | mergeJoinFlags ',' mergeJoinFlag
                        {
                            $$.setExpr(createComma($1.getExpr(), $3.getExpr()));
                            $$.setPosition($1);
                        }
    ;

mergeJoinFlag
    : MOFN '(' expression ')'
                        {
                            parser->normalizeExpression($3, type_numeric, false);
                            $$.setExpr(createExprAttribute(mofnAtom, $3.getExpr()));
                            $$.setPosition($1);
                        }
    | MOFN '(' expression ',' expression ')'
                        {
                            parser->normalizeExpression($3, type_numeric, false);
                            parser->normalizeExpression($5, type_numeric, false);
                            $$.setExpr(createExprAttribute(mofnAtom, $3.getExpr(), $5.getExpr()));
                            $$.setPosition($1);
                        }
    | DEDUP
                        {
                            $$.setExpr(createAttribute(dedupAtom));
                            $$.setPosition($1);
                        }
    | LEFT ONLY
                        {
                            $$.setExpr(createAttribute(leftonlyAtom));
                            $$.setPosition($1);
                        }
    | LEFT OUTER
                        {
                            $$.setExpr(createAttribute(leftouterAtom));
                            $$.setPosition($1);
                        }
    | TOK_ASSERT SORTED
                        {
                            $$.setExpr(createAttribute(assertAtom));
                            $$.setPosition($1);
                        }
    | SORTED '(' startSortOrder heterogeneous_expr_list ')' endSortOrder
                        {
                            HqlExprArray args;
                            $4.unwindCommaList(args);
                            $$.setExpr(createExprAttribute(sortedAtom, args));
                            $$.setPosition($1);
                        }
    | INTERNAL '(' constExpression ')'
                        {
                            parser->normalizeExpression($3, type_int, false);
                            $$.setExpr(createExprAttribute(internalFlagsAtom, $3.getExpr()));
                            $$.setPosition($1);
                        }
    | PARTITION '(' heterogeneous_expr_list ')'
                        {
                            HqlExprArray args;
                            $3.unwindCommaList(args);
                            $$.setExpr(createExprAttribute(skewAtom, args));
                            $$.setPosition($1);
                        }
    | commonAttribute
    ;

skewAttribute
    : SKEW '(' expression ')'
                        {
                            parser->normalizeExpression($3, type_numeric, true);
                            $$.setExpr(createExprAttribute(skewAtom, $3.getExpr()));
                        }
    | SKEW '(' optExpression ',' expression ')'
                        {
                            if ($3.queryExpr())
                            {
                                parser->normalizeExpression($3, type_numeric, true);
                            }
                            else
                                $3.setExpr(createValue(no_null));

                            parser->normalizeExpression($5, type_any, true);
                            parser->normalizeExpression($5, type_numeric, false);
                            $$.setExpr(createExprAttribute(skewAtom, $3.getExpr(), $5.getExpr()));
                        }
    ;

optDistributionFlags
    :                   {   $$.setNullExpr(); }
    | ',' DistributionFlags
                        {
                            $$.setExpr($2.getExpr());
                            $$.setPosition($1);
                        }
    ;

DistributionFlags
    : DistributionFlag
    | DistributionFlags ',' DistributionFlag    {   $$.setExpr(createComma($1.getExpr(), $3.getExpr())); }
    ;

DistributionFlag
    : ATMOST '(' expression ')'
                        {
                            parser->normalizeExpression($3, type_numeric, true);
                            $$.setExpr(createExprAttribute(atmostAtom, $3.getExpr()));
                        }
    | SKEW
                        {
                            $$.setExpr(createAttribute(skewAtom));
                        }
    | NAMED '(' constExpression ')'
                        {
                            parser->normalizeStoredNameExpression($3);
                            $$.setExpr(createExprAttribute(namedAtom, $3.getExpr()));
                            $$.setPosition($1);
                        }
//  | commonAttribute
    ;

optTrimFlags
    :                   {   $$.setNullExpr(); }
    | TrimFlags
    ;

TrimFlags
    : commaTrimFlag
    | TrimFlags commaTrimFlag   
                        {   $$.setExpr(createComma($1.getExpr(), $2.getExpr())); }
    ;

commaTrimFlag
    : ',' TrimFlag      { $$.setExpr($2.getExpr()); }
    ;

TrimFlag
    : LEFT              {   $$.setExpr(createAttribute(leftAtom)); }
    | RIGHT             {   $$.setExpr(createAttribute(rightAtom)); }
    | ALL               {   $$.setExpr(createAttribute(allAtom)); }
    ;

optSortList
    :                   {   $$.clear(); }
    | ',' sortList
    ;

doParseFlags
    : parseFlags
    ;

parseFlags
    :                   {   $$.setNullExpr(); }
    | parseFlags ',' parseFlag
                        {   $$.setExpr(createComma($1.getExpr(), $3.getExpr())); }
    ;

parseFlag
    : ALL               {   $$.setExpr(createAttribute(allAtom)); }
    | FIRST             {   $$.setExpr(createAttribute(firstAtom)); }
    | WHOLE             {   $$.setExpr(createAttribute(wholeAtom)); }
    | NOSCAN            {   $$.setExpr(createAttribute(noScanAtom)); }
    | SCAN              {   $$.setExpr(createAttribute(scanAtom)); }
    | SCAN ALL          {   $$.setExpr(createAttribute(scanAllAtom)); }
    | NOCASE            {   $$.setExpr(createAttribute(noCaseAtom)); }
    | CASE              {   $$.setExpr(createAttribute(caseAtom)); }
    | SKIP '(' pattern ')'
                        {   $$.setExpr(createExprAttribute(separatorAtom, $3.getExpr())); }
    | TERMINATOR '(' expression ')'
                        {
                            parser->normalizeExpression($3, type_set, true);
                            parser->validateParseTerminate($3.queryExpr(), $3);
                            $$.setExpr(createExprAttribute(terminatorAtom, $3.getExpr()));
                        }

    | ATMOST '(' constExpression ')'
                        {
                            parser->normalizeExpression($3, type_numeric, false);
                            $$.setExpr(createExprAttribute(atmostAtom, $3.getExpr()));
                        }
    | KEEP '(' constExpression ')'
                        {
                            parser->normalizeExpression($3, type_numeric, false);
                            $$.setExpr(createExprAttribute(keepAtom, $3.getExpr()));
                        }
    | MATCHED '(' ALL ')'
                        {
                            $$.setExpr(createValue(no_matched, makeNullType(), createValue(no_all, makeVoidType())));
                        }
    | MATCHED '(' patternOrRuleId ')'
                        {
                            $$.setExpr(createValue(no_matched, makeNullType(), $3.getExpr()));
                        }
    | MAX               {   $$.setExpr(createAttribute(maxAtom)); }
    | MIN               {   $$.setExpr(createAttribute(minAtom)); }
    | USE '(' globalPatternAttribute ')'
                        {
                            $$.setExpr(createValue(no_pat_use, $3.getExpr()));
                        }
    | BEST              {   $$.setExpr(createAttribute(bestAtom)); }
    | MANY BEST         {   $$.setExpr(createComma(createAttribute(bestAtom), createAttribute(manyAtom))); }
    | MANY MIN          {   $$.setExpr(createComma(createAttribute(minAtom), createAttribute(manyAtom))); }
    | MANY MAX          {   $$.setExpr(createComma(createAttribute(maxAtom), createAttribute(manyAtom))); }
    | NOT MATCHED       {   $$.setExpr(createAttribute(notMatchedAtom)); }
    | NOT MATCHED ONLY  
                        {   $$.setExpr(createAttribute(notMatchedOnlyAtom)); }
    | PARSE             {   $$.setExpr(createAttribute(tomitaAtom)); }
    | GROUP             {   $$.setExpr(createAttribute(groupAtom)); }
    | MANY              {   $$.setExpr(createAttribute(manyAtom)); }
    | MAXLENGTH '(' constExpression ')'
                        {
                            parser->normalizeExpression($3, type_numeric, false);
                            $$.setExpr(createExprAttribute(maxLengthAtom, $3.getExpr()));
                        }
    | MAXSIZE '(' constExpression ')'
                        {
                            parser->normalizeExpression($3, type_numeric, false);
                            $$.setExpr(createExprAttribute(maxLengthAtom, $3.getExpr()));
                        }
    | PARALLEL
                        {
                            $$.setExpr(createAttribute(parallelAtom));
                            $$.setPosition($1);
                        }
    | hintAttribute
    ;

xmlParseFlags
    :                   {   $$.setNullExpr(); }
    | xmlParseFlags ',' xmlParseFlag
                        {   $$.setExpr(createComma($1.getExpr(), $3.getExpr())); }
    ;

xmlParseFlag
    : XML_TOKEN         {   $$.setExpr(createAttribute(xmlAtom)); }
    | XML_TOKEN '(' constExpression ')'
                        {
                            parser->normalizeExpression($3, type_string, false);
                            $$.setExpr(createExprAttribute(xmlAtom, $3.getExpr()));
                        }
    ;

startGROUP
    : ','               {
                            $$.clear();
                        }
    ;

endGROUP
    : ')'               {   parser->popTopScope(); $$.clear(); }
    ;

startSortOrder
    :                   {
                            parser->sortDepth++;
                            $$.clear();
                        }
    ;

endSortOrder
    :                   {   parser->sortDepth--; $$.clear(); }
    ;

startTopFilter
    : dataSet           {   parser->pushTopScope($1.queryExpr()); $$.setExpr($1.getExpr()); }
    ;

startRightFilter
    : dataSet           {   parser->setRightScope($1.queryExpr()); $$.inherit($1); }
    ;

startRightRowsRecord
    : recordDef
                        {
                            parser->setRightScope($1.queryExpr());  
                            parser->beginRowsScope(no_right);
                            $$.inherit($1);
                        }
    ;

startLeftRightSeqFilter
    : dataSet
                        {
                            parser->pushLeftRightScope($1.queryExpr(), NULL); // selector only depends on left
                            parser->setRightScope($1.queryExpr());
                            $$.inherit($1);
                        }
    ;

startTopLeftSeqFilter
    : dataSet
                        {
                            parser->pushTopScope($1.queryExpr());
                            parser->pushLeftRightScope($1.queryExpr(), NULL);
                            $$.inherit($1);
                        }
    ;

startTopLeftRightSeqFilter
    : dataSet
                        {
                            parser->pushTopScope($1.queryExpr());
                            parser->pushLeftRightScope($1.queryExpr(), NULL); // selector only depends on left
                            parser->setRightScope($1.queryExpr());
                            $$.inherit($1);
                        }
    ;

startTopLeftRightSeqSetDatasets
    : setOfDatasets
                        {
                            parser->pushLeftRightScope($1.queryExpr(), NULL); // selector only depends on left
                            parser->setRightScope($1.queryExpr());
                            OwnedHqlExpr pseudoTop = parser->getSelector($1, no_left);
                            parser->pushTopScope(pseudoTop);
                            $$.inherit($1);
                        }
    ;

startPointerToMember
    : 
                        {
                             parser->pushTopScope(queryNullRecord());
                        } 
       LT
                        {
                            $$.inherit($2);
                        }
    ;

endPointerToMember
    : 
                        { 
                            parser->popTopScope();
                        }
      GT
                        {
                            $$.inherit($2);
                        }
    ;
    
startSimpleFilter
    : simpleDataSet     {   parser->pushTopScope($1.queryExpr()); $$.setExpr($1.getExpr()); }
    ;

startLeftSeqRow
    : dataRow
                        {
                            parser->pushLeftRightScope($1.queryExpr(), NULL);
                            $$.inherit($1);
                        }
    ;

startRightRow
    : dataRow               {
                            parser->setRightScope($1.queryExpr());
                            $$.setExpr($1.getExpr());
                        }
    ;

startLeftRowsSeqFilter
    : dataSet
                        {
                            parser->pushLeftRightScope($1.queryExpr(), NULL);
                            parser->beginRowsScope(no_left);
                            $$.inherit($1);
                        }
    ;

startLeftSeqFilter
    : dataSet
                        {
                            parser->pushLeftRightScope($1.queryExpr(), NULL);
                            $$.inherit($1);
                        }
    ;

startLeftDelaySeqFilter
    : dataSet
                        {
                            //Since the SEQUENCE is based on left and right left and right aren't valid until we have seen both datasets
                            parser->pushPendingLeftRightScope($1.queryExpr(), NULL);
                            $$.inherit($1);
                        }
    ;

//Used for the RHS of a distribute.  Top filter has already been processed, so selector id should be based on both.
startRightDistributeSeqFilter
    : dataSet
                        {
                            parser->pushLeftRightScope(parser->queryTopScope(), $1.queryExpr());
                            $$.inherit($1);
                        }
    ;


endSelectorSequence
    :                   {
                            $$.setExpr(parser->popLeftRightScope());
                        }
    ;


startLeftRowsGroup
    : GROUP             {
                            parser->beginRowsScope(no_left);
                            $$.clear();
                        }
    ;

startLeftRows
    :                   {
                            parser->beginRowsScope(no_left);
                            $$.clear();
                        }
    ;

startRightRowsGroup
    : GROUP             {
                            parser->beginRowsScope(no_right);
                            $$.clear();
                        }
    ;

endRowsGroup
    :                   {   $$.setExpr(parser->endRowsScope()); }
    ;

endSimpleFilter
    :                   {   parser->popTopScope(); $$.clear(); }
    ;

endTopFilter
    :                   {   parser->popTopScope(); $$.clear(); }
    ;

endTopLeftFilter
    :                   {   parser->popTopScope(); $$.clear(); }
    ;

endTopLeftRightFilter
    :                   {   parser->popTopScope(); $$.clear(); }
    ;

scopedDatasetId
    : globalScopedDatasetId
    | dotScope DATASET_ID leaveScope
                        {
                            IHqlExpression *e1 = $1.getExpr();
                            IHqlExpression *e2 = $2.getExpr();
                            if (e1 && (e1->getOperator() != no_record) && (e2->getOperator() == no_field))
                                $$.setExpr(parser->createSelect(e1, e2, $2));
                            else
                            {
                                ::Release(e1);
                                $$.setExpr(e2);
                            }
                        }
    | datasetFunction '('
                        {
                            parser->beginFunctionCall($1);
                        }
    actualParameters ')'
                        {
                            $$.setExpr(parser->bindParameters($1, $4.getExpr()));
                        }
    ;

globalScopedDatasetId
    : DATASET_ID
    | moduleScopeDot DATASET_ID leaveScope
                        {
                            OwnedHqlExpr scope = $1.getExpr();
                            $$.setExpr($2.getExpr());
                        }
    ;


setOfDatasets
    : scopedListDatasetId
    | RANGE '(' setOfDatasets ',' expression ')'
                        {
                            parser->normalizeExpression($5, type_set, false);
                            ITypeInfo * childType = $5.queryExprType()->queryChildType();
                            if (!childType || !isIntegralType(childType))
                                parser->reportError(ERR_TYPEMISMATCH_INTREAL, $4, "Type mismatch - expected a list of integral type");
                            OwnedHqlExpr list = $3.getExpr();
                            $$.setExpr(createValue(no_rowsetrange, list->getType(), LINK(list), $5.getExpr()));
                            $$.setPosition($1);
                        }
    | '[' beginList dataSetList ignoreDummyList ']'
                        {
                            HqlExprArray args;
                            $3.unwindCommaList(args);
                            parser->checkRecordsMatch($1, args);
                            //check all the datasets have the same record structure at a minimum.
                            ITypeInfo * setType = makeSetType(args.item(0).getType());
                            $$.setExpr(createValue(no_datasetlist, setType, args)); // This should probably be a pseudo-dataset
                            $$.setPosition($1);
                        }
    | ROWSET '(' dataRow ')'
                        {
                            $$.setExpr(parser->processRowset($3), $1);
                        }
    | startCompoundExpression beginInlineFunctionToken optDefinitions RETURN setOfDatasets ';' endInlineFunctionToken
                        {
                            Owned<ITypeInfo> retType = $1.getType();
                            $$.setExpr(parser->leaveLamdaExpression($5), $7);
                        }
    ;

scopedListDatasetId
    : LIST_DATASET_ID
    | dotScope LIST_DATASET_ID leaveScope
                        {
                            IHqlExpression *e1 = $1.getExpr();
                            IHqlExpression *e2 = $2.getExpr();
                            if (e1 && (e1->getOperator() != no_record) && (e2->getOperator() == no_field))
                                $$.setExpr(parser->createSelect(e1, e2, $2));
                            else
                            {
                                ::Release(e1);
                                $$.setExpr(e2);
                            }
                        }
    | moduleScopeDot LIST_DATASET_ID leaveScope
                        {
                            OwnedHqlExpr scope = $1.getExpr();
                            $$.setExpr($2.getExpr());
                        }
    | listDatasetFunction '('
                        {
                            parser->beginFunctionCall($1);
                        }
    actualParameters ')'
                        {
                            $$.setExpr(parser->bindParameters($1, $4.getExpr()));
                        }
    ;


actualParameters
    : actualList        {   $$.setExpr(parser->endFunctionCall(), $1); }
    ;

actualList
    : optActualValue
    | actualList ',' optActualValue
    ;

optActualValue
    :                   {
                            parser->addActual($$, createOmittedValue());
                            $$.clear();
                        }
    | actualValue       {
                            parser->addActual($1, $1.getExpr());
                            $$.clear();
                        }
    | namedActual
    ;

namedActual
    : UNKNOWN_ID ASSIGN actualValue
                        {
                            parser->addNamedActual($1, $1.getId(), $3.getExpr());
                            $$.clear();
                            $$.setPosition($1);
                        }
    | NAMED UNKNOWN_ID ASSIGN actualValue
                        {
                            parser->addNamedActual($1, $2.getId(), $4.getExpr());
                            $$.clear();
                            $$.setPosition($1);
                        }
    | goodObject ASSIGN actualValue
                        {
                            OwnedHqlExpr nameExpr = $1.getExpr();
                            IIdAtom * name = nameExpr->queryId();
                            if (!name)
                            {
                                name = unknownId;
                                parser->reportError(ERR_EXPECTED_ID, $1, "Expected a parameter name");
                            }
                            parser->addNamedActual($1, name, $3.getExpr());
                            $$.clear();
                            $$.setPosition($1);
                        }
    ;


actualValue
    : expression
                        {
                            parser->normalizeExpression($1);
                            $$.inherit($1);
                        }
    | dataSet optFieldMaps
                        {
                            IHqlExpression* expr = $1.getExpr();
                            IHqlExpression* map = $2.getExpr();
                            $$.setExpr(parser->bindFieldMap(expr,map));
                        }
    | dataRow
    | dictionary
    | TOK_PATTERN pattern
                        {   $$.setExpr($2.getExpr()); }
    | TOKEN pattern     {   $$.setExpr($2.getExpr()); }
    | RULE pattern      {   $$.setExpr($2.getExpr()); }
    | anyFunction
    | abstractModule
    | setOfDatasets
    | recordDef
    | fieldSelectedFromRecord
    ;

anyFunction
    : DATAROW_FUNCTION
    | moduleScopeDot DATAROW_FUNCTION leaveScope
                        {
                            $1.release();
                            $$.setExpr($2.getExpr());
                        }
    | DATASET_FUNCTION
    | moduleScopeDot DATASET_FUNCTION leaveScope
                        {
                            $1.release();
                            $$.setExpr($2.getExpr());
                        }
    | valueFunction
    | ACTION_FUNCTION
    | moduleScopeDot ACTION_FUNCTION leaveScope
                        {
                            $1.release();
                            $$.setExpr($2.getExpr());
                        }
    | PATTERN_FUNCTION
    | moduleScopeDot PATTERN_FUNCTION leaveScope
                        {
                            $1.release();
                            $$.setExpr($2.getExpr());
                        }
    | RECORD_FUNCTION
    | moduleScopeDot RECORD_FUNCTION leaveScope
                        {
                            $1.release();
                            $$.setExpr($2.getExpr());
                        }
    | EVENT_FUNCTION
    | moduleScopeDot EVENT_FUNCTION leaveScope
                        {
                            $1.release();
                            $$.setExpr($2.getExpr());
                        }
    | scopeFunction
    | TRANSFORM_FUNCTION
    | moduleScopeDot TRANSFORM_FUNCTION leaveScope
                        {
                            $1.release();
                            $$.setExpr($2.getExpr());
                        }
    ;

valueFunction
    : VALUE_FUNCTION
    | moduleScopeDot VALUE_FUNCTION leaveScope
                        {
                            $1.release();
                            $$.setExpr($2.getExpr());
                        }
    | startCompoundExpression reqparmdef beginInlineFunctionToken optDefinitions RETURN expression ';' endInlineFunctionToken
                        {
                            Owned<ITypeInfo> retType = $1.getType();
                            $$.setExpr(parser->leaveLamdaExpression($6), $8);
                        }
    ;

actionFunction
    : ACTION_FUNCTION
    | moduleScopeDot ACTION_FUNCTION leaveScope
                        {
                            $1.release();
                            $$.setExpr($2.getExpr());
                        }
    | startCompoundExpression reqparmdef beginInlineFunctionToken optDefinitions RETURN action ';' endInlineFunctionToken
                        {
                            Owned<ITypeInfo> retType = $1.getType();
                            $$.setExpr(parser->leaveLamdaExpression($6), $8);
                        }
    ;

datarowFunction
    : DATAROW_FUNCTION
    | moduleScopeDot DATAROW_FUNCTION leaveScope
                        {
                            $1.release();
                            $$.setExpr($2.getExpr());
                        }
    | startCompoundExpression reqparmdef beginInlineFunctionToken optDefinitions RETURN dataRow ';' endInlineFunctionToken
                        {
                            Owned<ITypeInfo> retType = $1.getType();
                            $$.setExpr(parser->leaveLamdaExpression($6), $8);
                        }
    ;

datasetFunction
    : DATASET_FUNCTION
    | moduleScopeDot DATASET_FUNCTION leaveScope
                        {
                            $1.release();
                            $$.setExpr($2.getExpr());
                        }
    | startCompoundExpression reqparmdef beginInlineFunctionToken optDefinitions RETURN dataSet ';' endInlineFunctionToken
                        {
                            Owned<ITypeInfo> retType = $1.getType();
                            $$.setExpr(parser->leaveLamdaExpression($6), $8);
                        }
    ;

dictionaryFunction
    : DICTIONARY_FUNCTION
    | moduleScopeDot DICTIONARY_FUNCTION leaveScope
                        {
                            $1.release();
                            $$.setExpr($2.getExpr());
                        }
    | startCompoundExpression reqparmdef beginInlineFunctionToken optDefinitions RETURN dictionary ';' endInlineFunctionToken
                        {
                            Owned<ITypeInfo> retType = $1.getType();
                            $$.setExpr(parser->leaveLamdaExpression($6), $8);
                        }
    ;

scopeFunction
    : SCOPE_FUNCTION
    | moduleScopeDot SCOPE_FUNCTION leaveScope
                        {
                            $1.release();
                            $$.setExpr($2.getExpr(), $2);
                        }
    | startCompoundExpression reqparmdef beginInlineFunctionToken optDefinitions RETURN abstractModule ';' endInlineFunctionToken
                        {
                            Owned<ITypeInfo> retType = $1.getType();
                            $$.setExpr(parser->leaveLamdaExpression($6), $8);
                        }
    ;


transformFunction
    : TRANSFORM_FUNCTION
    | moduleScopeDot TRANSFORM_FUNCTION leaveScope
                        {
                            $1.release();
                            $$.setExpr($2.getExpr(), $2);
                        }
    | startCompoundExpression reqparmdef beginInlineFunctionToken optDefinitions RETURN transform ';' endInlineFunctionToken
                        {
                            Owned<ITypeInfo> retType = $1.getType();
                            $$.setExpr(parser->leaveLamdaExpression($6), $8);
                        }
    ;


recordFunction
    : RECORD_FUNCTION
    | moduleScopeDot RECORD_FUNCTION leaveScope
                        {
                            $1.release();
                            $$.setExpr($2.getExpr(), $2);
                        }
    | startCompoundExpression reqparmdef beginInlineFunctionToken optDefinitions RETURN recordDef ';' endInlineFunctionToken
                        {
                            Owned<ITypeInfo> retType = $1.getType();
                            $$.setExpr(parser->leaveLamdaExpression($6), $8);
                        }
    ;

listDatasetFunction
    : LIST_DATASET_FUNCTION
    | moduleScopeDot LIST_DATASET_FUNCTION leaveScope
                        {
                            $1.release();
                            $$.setExpr($2.getExpr());
                        }
    | startCompoundExpression reqparmdef beginInlineFunctionToken optDefinitions RETURN setOfDatasets ';' endInlineFunctionToken
                        {
                            Owned<ITypeInfo> retType = $1.getType();
                            $$.setExpr(parser->leaveLamdaExpression($6), $8);
                        }
    ;


eventFunction
    : EVENT_FUNCTION
    | moduleScopeDot EVENT_FUNCTION leaveScope
                        {
                            $1.release();
                            $$.setExpr($2.getExpr());
                        }
    | startCompoundExpression reqparmdef beginInlineFunctionToken optDefinitions RETURN eventObject ';' endInlineFunctionToken
                        {
                            Owned<ITypeInfo> retType = $1.getType();
                            $$.setExpr(parser->leaveLamdaExpression($6), $8);
                        }
    ;



optFieldMaps
    : '{' beginList fieldMaps '}'
                        {
                            HqlExprArray args;
                            parser->endList(args);
                            $$.setExpr(createSortList(args), $1);
                        }
    |                   { $$.setNullExpr(); }
    | '{' beginList error '}'
                        {
                            HqlExprArray args;
                            parser->endList(args);
                            $$.setNullExpr();
                        }
    ;

fieldMaps
    : fieldMaps1 optSemiComma
    ;

fieldMaps1
    : fieldMap
    | fieldMaps1 semiComma fieldMap
    ;

fieldMap
    :  UNKNOWN_ID ASSIGN UNKNOWN_ID
                        {
                            IHqlExpression* attr1 = createId($1.getId());
                            IHqlExpression* attr2 = createId($3.getId());
                            parser->addListElement(createComma(attr1, attr2));
                            $$.clear();
                        }
    ;

sortListOptCurleys
    : sortList
    | '{' sortList '}'
    | '{' sortList '}' ',' sortList         /* Allow trailing attributes */
    ;

sortList
    : sortItem
                        {
                            parser->addListElement($1.getExpr());
                            $$.clear();
                        }
    |   sortList ',' sortItem
                        {   
                            parser->addListElement($3.getExpr());
                            $$.clear();
                        }
    |   sortList ';' sortItem
                        {   
                            parser->addListElement($3.getExpr());
                            $$.clear();
                        }
    ;

nonDatasetList
    : nonDatasetExpr
                        {
                            parser->addListElement($1.getExpr());
                            $$.clear();
                        }
    |   nonDatasetList ',' nonDatasetExpr
                        {
                            parser->addListElement($3.getExpr());
                            $$.clear();
                        }
    |   nonDatasetList ';' nonDatasetExpr
                        {
                            parser->addListElement($3.getExpr());
                            $$.clear();
                        }
    ;

nonDatasetExpr
    : expression            
                        {
                            node_operator op = $1.getOperator();
                            if (op == no_sortlist) //|| (op == no_list))
                            {
                                OwnedHqlExpr list = $1.getExpr();
                                HqlExprArray args;
                                unwindChildren(args, list);
                                ForEachItemIn(i, args)
                                    parser->addListElement(&OLINK(args.item(i)));
                                $$.setExpr(NULL);
                            }
                            else
                            {
                                parser->normalizeExpression($1);
                                //ALL in a sort list is treated as an attribute.
                                parser->convertAllToAttribute($1);
                                $$.inherit($1);
                            }
                        }
    | dataRow
    | '-' dataRow       {
                            //This is an example of a semantic check that should really take place one everything is
                            //parsed.  The beingSortOrder productions are a pain, and not strictly correct.
                            if (parser->sortDepth == 0)
                                parser->normalizeExpression($2, type_numeric, false);

                            IHqlExpression * e = $2.getExpr();
                            $$.setExpr(createValue(no_negate, makeVoidType(), e));
                            $$.setPosition($1);
                        }
    | dictionary
    ;

sortItem
    : nonDatasetExpr
    | dataSet
    | FEW               {   $$.setExpr(createAttribute(fewAtom));   }
    | MANY              {   $$.setExpr(createAttribute(manyAtom));  }
    | MERGE             {   $$.setExpr(createAttribute(mergeAtom)); }
    | SORTED            {   $$.setExpr(createAttribute(sortedAtom));    }
    | UNSORTED          {   $$.setExpr(createAttribute(unsortedAtom)); }
    | skewAttribute
    | TOK_ASSERT        {
                            $$.setExpr(createAttribute(assertAtom));
                            $$.setPosition($1);
                        }
    | JOINED '(' dataSet ')'
                        {
                            IHqlExpression *dataset = $3.getExpr();
                            if ((dataset->getOperator() != no_sort) && (dataset->getOperator() != no_sorted))
                            {
                                parser->reportError(ERR_JOINED_NOSORTED, $3, "JOINED must specify a sorted dataset");
                                dataset->Release();
                                $$.setNullExpr();
                            }
                            else
                                $$.setExpr(createValue(no_joined, (ITypeInfo*)NULL, dataset));
                        }
    | THRESHOLD '(' expression ')'
                        {
                            parser->normalizeExpression($3, type_numeric, true);
                            $$.setExpr(createAttribute(thresholdAtom, $3.getExpr()));
                        }
    | WHOLE RECORD      {   $$.setExpr(createAttribute(recordAtom)); }
    | RECORD            {   $$.setExpr(createAttribute(recordAtom)); }
    | EXCEPT expression {   
                            parser->normalizeExpression($2);
                            $$.setExpr(createExprAttribute(exceptAtom, $2.getExpr()));
                        }
    | EXCEPT dataRow    {   $$.setExpr(createExprAttribute(exceptAtom, $2.getExpr())); }
    | EXCEPT dataSet    {   $$.setExpr(createExprAttribute(exceptAtom, $2.getExpr())); }
    | BEST '(' heterogeneous_expr_list ')'  
                        {
                            HqlExprArray args;
                            $3.unwindCommaList(args);
                            $$.setExpr(createExprAttribute(bestAtom, args));
                        }
    | mergeJoinFlag
    | STABLE
                        {
                            $$.setExpr(createExprAttribute(stableAtom));
                            $$.setPosition($1);
                        }
    | STABLE '(' expression ')'
                        {
                            parser->normalizeExpression($3, type_string, false);
                            $$.setExpr(createExprAttribute(stableAtom, $3.getExpr()));
                            $$.setPosition($1);
                        }
    | UNSTABLE
                        {
                            $$.setExpr(createExprAttribute(unstableAtom));
                            $$.setPosition($1);
                        }
    | KEYED
                        {
                            $$.setExpr(createAttribute(keyedAtom));
                            $$.setPosition($1);
                        }
    | GROUPED
                        {
                            $$.setExpr(createAttribute(groupedAtom), $1);
                        }
    | UNSTABLE '(' expression ')'
                        {
                            parser->normalizeExpression($3, type_string, false);
                            $$.setExpr(createExprAttribute(unstableAtom, $3.getExpr()));
                            $$.setPosition($1);
                        }
    | PARALLEL
                        {
                            $$.setExpr(createAttribute(parallelAtom), $1);
                        }
    | prefetchAttribute
    | expandedSortListByReference
    ;

expandedSortListByReference
    : startPointerToMember SORTLIST_ID endPointerToMember
                        {
                            $$.setExpr(parser->expandedSortListByReference(NULL, $2), $1);
                        }
    | dotScope startPointerToMember leaveScope SORTLIST_ID endPointerToMember
                        {
                            $$.setExpr(parser->expandedSortListByReference(&$1, $4), $1);
                        }
    ;

optDedupFlags
    : dedupFlags
    |                   {   $$.setNullExpr(); }
    ;

dedupFlags
    : ',' dedupFlag     {   $$.setExpr($2.getExpr()); }
    | dedupFlags ',' dedupFlag
                        {   $$.setExpr(createComma($1.getExpr(), $3.getExpr())); }
    ;

dedupFlag
    : KEEP expression   {
                            parser->normalizeExpression($2, type_int, false);
                            $$.setExpr(createExprAttribute(keepAtom, $2.getExpr()));
                        }
    | commonAttribute
    | expression
                        {
                            //MORE:SORTLIST  Allow a sortlist to be expanded!
                            parser->normalizeExpression($1);
                            parser->convertAllToAttribute($1);
                            $$.inherit($1);
                        }
    | WHOLE RECORD      {   $$.setExpr(createAttribute(recordAtom)); }
    | RECORD            {   $$.setExpr(createAttribute(recordAtom)); }
    | EXCEPT expression {   
                            //MORE:SORTLIST  Allow sort list as an exception
                            parser->normalizeExpression($2);
                            $$.setExpr(createExprAttribute(exceptAtom, $2.getExpr()));
                        }
    | MANY              {   $$.setExpr(createAttribute(manyAtom)); }
    | HASH              {   $$.setExpr(createAttribute(hashAtom)); }
    | dataRow
                        {
                            //Backward compatibility... special case use of LEFT and RIGHT as modifiers.
                            OwnedHqlExpr row = $1.getExpr();
                            if (row->getOperator() == no_left)
                                row.setown(createAttribute(leftAtom));
                            else if (row->getOperator() == no_right)
                                row.setown(createAttribute(rightAtom));
                            $$.setExpr(row.getClear(), $1);
                        }
    | dataSet
    ;

rollupExtra
    : rollupFlags
    ;

rollupFlags
    : ',' rollupFlag    {   $$.inherit($2); }
    | rollupFlags ',' rollupFlag
                        {   $$.setExpr(createComma($1.getExpr(), $3.getExpr())); }
    ;

rollupFlag
    : commonAttribute
    | expression
                        {
                            //MORE:SORTLIST  Allow a sortlist to be expanded!
                            parser->normalizeExpression($1);
                            $$.inherit($1);
                        }
    | WHOLE RECORD      {   $$.setExpr(createAttribute(recordAtom)); }
    | RECORD                {   $$.setExpr(createAttribute(recordAtom)); }
    | EXCEPT expression {   
                            //MORE:SORTLIST  Allow sort list as an exception
                            parser->normalizeExpression($2);
                            $$.setExpr(createExprAttribute(exceptAtom, $2.getExpr()));
                        }
    | dataRow
    | dataSet
    ;

conditions
    : '(' condList ')'  {
                            $$.setExpr($2.getExpr());
                        }
    | '(' ')'           {
                            $$.setNullExpr();
                        }
    ;

mapSpec
    : mapItem
    | mapSpec ',' mapItem   
                        {
                            ITypeInfo *type = parser->checkType($1, $3);
                            $$.setExpr(createValue(no_comma, type, $1.getExpr(), $3.getExpr()));
                        }
    ;

mapItem
    : booleanExpr GOESTO expression 
                        {
                            parser->normalizeExpression($3);
                            parser->applyDefaultPromotions($3, true);
                            IHqlExpression *e3 = $3.getExpr();
                            $$.setExpr(createValue(no_mapto, e3->getType(), $1.getExpr(), e3));
                            $$.setPosition($3);
                        }
    ;


beginList
    :                   {
                            parser->beginList();
                            $$.clear();
                        }
    ;

sortListExpr
    : beginList '{' sortList '}'
                        {
                            HqlExprArray elements;
                            parser->endList(elements);
                            $$.setExpr(createSortList(elements));
                        }
    ;

ignoreDummyList
    :
                        {
                            HqlExprArray args;
                            parser->endList(args);
                            assertex(args.ordinality() == 0);
                            $$.clear();
                        }
    ;

caseSpec
    : caseItem
    | caseSpec ',' caseItem
    ;

caseItem
    : expression GOESTO expression  
                        {
                            parser->normalizeExpression($1);
                            parser->normalizeExpression($3);
                            //MORE: Should call checkType?
                            parser->applyDefaultPromotions($1, true);
                            parser->applyDefaultPromotions($3, true);
                            IHqlExpression *e3 = $3.getExpr();
                            parser->addListElement(createValue(no_mapto, e3->getType(), $1.getExpr(), e3));
                            $$.clear();
                            $$.setPosition($3);
                        }
    ;

mapDatasetSpec
    : mapDatasetItem
    | mapDatasetSpec ',' mapDatasetItem
                        {
                            ITypeInfo *type = parser->checkType($1, $3);
                            $$.setExpr(createValue(no_comma, type, $1.getExpr(), $3.getExpr()));
                        }
    ;

mapDatasetItem
    : booleanExpr GOESTO dataSet
                        {
                            IHqlExpression *e3 = $3.getExpr();
                            $$.setExpr(createDataset(no_mapto, $1.getExpr(), e3));
                            $$.setPosition($3);
                        }
    ;

mapDictionarySpec
    : mapDictionaryItem
    | mapDictionarySpec ',' mapDictionaryItem
                        {
                            ITypeInfo *type = parser->checkType($1, $3);
                            $$.setExpr(createValue(no_comma, type, $1.getExpr(), $3.getExpr()));
                        }
    ;

mapDictionaryItem
    : booleanExpr GOESTO dictionary
                        {
                            IHqlExpression *e3 = $3.getExpr();
                            $$.setExpr(createDictionary(no_mapto, $1.getExpr(), e3));
                            $$.setPosition($3);
                        }
    ;

caseDatasetSpec
    : caseDatasetItem
    | caseDatasetSpec ',' caseDatasetItem
    ;

caseDatasetItem
    : expression GOESTO dataSet
                        {
                            parser->normalizeExpression($1);
                            parser->applyDefaultPromotions($1, true);
                            IHqlExpression *e3 = $3.getExpr();
                            parser->addListElement(createDataset(no_mapto, $1.getExpr(), e3));
                            $$.clear();
                            $$.setPosition($3);
                        }
    ;

caseDictionarySpec
    : caseDictionaryItem
    | caseDictionarySpec ',' caseDictionaryItem
    ;

caseDictionaryItem
    : expression GOESTO dictionary
                        {
                            parser->normalizeExpression($1);
                            parser->applyDefaultPromotions($1, true);
                            IHqlExpression *e3 = $3.getExpr();
                            parser->addListElement(createDictionary(no_mapto, $1.getExpr(), e3));
                            $$.clear();
                            $$.setPosition($3);
                        }
    ;

mapDatarowSpec
    : mapDatarowItem
    | mapDatarowSpec ',' mapDatarowItem
                        {
                            ITypeInfo *type = parser->checkType($1, $3);
                            $$.setExpr(createValue(no_comma, type, $1.getExpr(), $3.getExpr()));
                        }
    ;

mapDatarowItem
    : booleanExpr GOESTO dataRow
                        {
                            IHqlExpression *e3 = $3.getExpr();
                            $$.setExpr(createRow(no_mapto, $1.getExpr(), e3));
                            $$.setPosition($3);
                        }
    ;

caseDatarowSpec
    : caseDatarowItem
    | caseDatarowSpec ',' caseDatarowItem
    ;

caseDatarowItem
    : expression GOESTO dataRow
                        {
                            parser->normalizeExpression($1);
                            parser->applyDefaultPromotions($1, true);
                            IHqlExpression *e3 = $3.getExpr();
                            parser->addListElement(createRow(no_mapto, $1.getExpr(), e3));
                            $$.clear();
                            $$.setPosition($3);
                        }
    ;

mapActionSpec
    : mapActionItem
    | mapActionSpec ',' mapActionItem
                        {
                            $$.setExpr(createComma($1.getExpr(), $3.getExpr()));
                        }
    ;

mapActionItem
    : booleanExpr GOESTO action
                        {
                            IHqlExpression *e3 = $3.getExpr();
                            $$.setExpr(createValue(no_mapto, makeVoidType(), $1.getExpr(), e3));
                            $$.setPosition($3);
                        }
    ;

caseActionSpec
    : caseActionItem
    | caseActionSpec ',' caseActionItem
    ;

caseActionItem
    : expression GOESTO action
                        {
                            parser->normalizeExpression($1);
                            parser->applyDefaultPromotions($1, true);
                            IHqlExpression *e3 = $3.getExpr();
                            parser->addListElement(createValue(no_mapto, makeVoidType(), $1.getExpr(), e3));
                            $$.clear();
                            $$.setPosition($3);
                        }
    ;

const
    : stringConstExpr
    | INTEGER_CONST
    | DATA_CONST
    | REAL_CONST
    | UNICODE_CONST
    | TOK_TRUE          {
                            $$.setExpr(createConstant(true), $1);
                        }
    | TOK_FALSE         {
                            $$.setExpr(createConstant(false), $1);
                        }
    | BOOL_CONST
    ;

stringConstExpr
    : STRING_CONST
    ;

stringOrUnicodeConstExpr
    : stringConstExpr
    | UNICODE_CONST
    ;


optConstExpression
    :                   {   $$.setNullExpr(); }
    |   ',' constExpression
                        {   $$.setExpr($2.getExpr()); }
    ;

pattern
    : beginList patternOr
                        {
                            HqlExprArray args;
                            parser->endList(args);
                            $$.setExpr(parser->createPatternOr(args, $2));
                        }
    ;

optPatternOr
    : patternOr
    |                   {
                            parser->addListElement(parser->createNullPattern());
                            $$.clear();
                        }
    ;

patternOr
    : patternOrItem
    | optPatternOr '|' optPatternOrItem
    | optPatternOr OR optPatternOrItem
    ;


optPatternOrItem
    : patternOrItem
    |                   {
                            parser->addListElement(parser->createNullPattern());
                            $$.clear();
                        }
    ;

patternOrItem
    : beginPatternScope patternOrItemValue
                        {
                            parser->leavePatternScope($2);
                            HqlExprArray args;
                            parser->endList(args);
                            OwnedHqlExpr production = $2.getExpr();
                            if (production->getOperator() != no_pat_production)
                                parser->checkProduction(args, $2);
                            parser->addListElement(production.getClear());
                            $$.clear();
                            $$.setPosition($2);
                        }
    ;

beginPatternScope
    :                   {
                            parser->beginList();        // this list is only maintained for scope checking - value is accumulated separately
                            parser->enterPatternScope(parser->curList);
                            $$.clear();
                        }
    ;


startRootPattern
    :   globalPatternAttribute
                        {
                            parser->beginList();
                            parser->enterPatternScope(parser->curList);
                            parser->addListElement(LINK($1.queryExpr()));
                            $$.inherit($1);
                        }
    ;

endRootPattern
    :                   {
                            parser->leavePatternScope($$);
                            HqlExprArray args;
                            parser->endList(args);
                            $$.clear();
                        }
    ;

patternOrItemValue
    : pattern2
                        {
                            parser->checkPattern($1, false);
                            $$.inherit($1);
                        }
    | pattern2 transform
                        {
                            parser->checkPattern($1, false);
                            IHqlExpression * transform = $2.getExpr();
                            $$.setExpr(createValue(no_pat_production, makeRuleType(LINK(transform->queryRecordType())), $1.getExpr(), transform));
                            $$.setPosition($1);
                        }
    ;

pattern2
    : pattern2 checkedPattern           
                        {
                            parser->checkPattern($1, true);
                            parser->checkPattern($2, true);
                            IHqlExpression * left = $1.getExpr();
                            IHqlExpression * right = $2.getExpr();
                            parser->addListElement(LINK(right));
                            ITypeInfo * type = parser->getCompoundRuleType(left->queryType(), right->queryType());
                            $$.setExpr(createValue(no_pat_follow, type, left, right));
                        }
    | checkedPattern                
                        {
                            parser->addListElement(LINK($1.queryExpr()));
                            $$.inherit($1);
                        }
    ;

optNotAttr
    :                   {   $$.setNullExpr(); }
    | NOT               {   $$.setExpr(createAttribute(notAtom)); }
    ;

//The following define tokens or patterns... but those tokens are then checked against another pattern
checkedPattern
    : pattern0
    | checkedPattern optNotAttr BEFORE pattern0
                        {
                            parser->checkPattern($1, true);
                            parser->checkSubPattern($4);
                            IHqlExpression * pattern = $1.getExpr();
                            $$.setExpr(createValue(no_pat_x_before_y, pattern->getType(), pattern, $4.getExpr(), $2.getExpr()));
                        }
    | checkedPattern optNotAttr AFTER pattern0
                        {
                            parser->checkPattern($1, true);
                            parser->checkSubPattern($4);
                            IHqlExpression * pattern = $1.getExpr();
                            $$.setExpr(createValue(no_pat_x_after_y, pattern->getType(), pattern, $4.getExpr(), $2.getExpr()));
                        }
    | TOK_ASSERT optNotAttr BEFORE pattern0
                        {
                            parser->checkSubPattern($4);
                            $$.setExpr(createValue(no_pat_before_y, makePatternType(), $4.getExpr(), $2.getExpr()));
                        }
    | TOK_ASSERT optNotAttr AFTER pattern0
                        {
                            parser->checkSubPattern($4);
                            $$.setExpr(createValue(no_pat_after_y, makePatternType(), $4.getExpr(), $2.getExpr()));
                        }
    | checkedPattern optNotAttr TOK_IN pattern0
                        {
                            parser->checkPattern($1, false);
                            parser->checkSubPattern($4);
                            IHqlExpression * pattern = $1.getExpr();
                            $$.setExpr(createValue(no_pat_checkin, pattern->getType(), pattern, $4.getExpr(), $2.getExpr()));
                        }
    | checkedPattern optNotAttr EQ pattern0
                        {
                            parser->checkPattern($1, false);
                            parser->checkSubPattern($4);
                            IHqlExpression * pattern = $1.getExpr();
                            $$.setExpr(createValue(no_pat_checkin, pattern->getType(), pattern, $4.getExpr(), $2.getExpr()));
                        }
    | checkedPattern NE pattern0
                        {
                            parser->checkPattern($1, false);
                            parser->checkSubPattern($3);
                            IHqlExpression * pattern = $1.getExpr();
                            $$.setExpr(createValue(no_pat_checkin, pattern->getType(), pattern, $3.getExpr(), createAttribute(notAtom)));
                        }
    | checkedPattern LENGTH '(' rangeExpr ')'
                        {
                            parser->checkPattern($1, false);
                            parser->normalizeExpression($4, type_any, true);
                            IHqlExpression * pattern = $1.getExpr();
                            $$.setExpr(createValue(no_pat_checklength, pattern->getType(), pattern, $4.getExpr()));
                        }
    | VALIDATE '(' pattern ',' booleanExpr ')'
                        {
                            parser->checkPattern($3, false);
                            IHqlExpression * pattern = $3.getExpr();
                            $$.setExpr(createValue(no_pat_validate, pattern->getType(), pattern, $5.getExpr()));
                        }
    | VALIDATE '(' pattern ',' booleanExpr ',' booleanExpr ')'
                        {
                            parser->checkPattern($3, false);
                            IHqlExpression * pattern = $3.getExpr();
                            $$.setExpr(createValue(no_pat_validate, pattern->getType(), pattern, $5.getExpr(), $7.getExpr()));
                        }
    ;



pattern0
    : FIRST             { $$.setExpr(createValue(no_pat_first, makePatternType())); }
    | LAST              { $$.setExpr(createValue(no_pat_last, makePatternType())); }
    | REPEAT '(' pattern optMinimal ')'
                        {
                            parser->checkPattern($3, true);
                            if ($3.queryExpr()->queryRecord())
                                parser->reportError(ERR_AMBIGUOUS_PRODUCTION, $1, "Cannot use REPEAT on a rule with an associated row");

                            IHqlExpression * pattern = $3.getExpr();
                            $$.setExpr(createValue(no_pat_repeat, parser->getCompoundRuleType(pattern), pattern, $4.getExpr()));
                        }
    | REPEAT '(' pattern ',' expression optMinimal ')'
                        {
                            parser->checkPattern($3, true);
                            parser->normalizeExpression($5, type_int, true);
                            if ($3.queryExpr()->queryRecord())
                                parser->reportError(ERR_AMBIGUOUS_PRODUCTION, $1, "Cannot use REPEAT on a rule with an associated row");

                            IHqlExpression * pattern = $3.getExpr();
                            $$.setExpr(createValue(no_pat_repeat, parser->getCompoundRuleType(pattern), pattern, $5.getExpr(), $6.getExpr()));
                        }
    | REPEAT '(' pattern ',' expression ',' ANY optMinimal ')'
                        {
                            parser->checkPattern($3, true);
                            parser->normalizeExpression($5, type_int, true);
                            if ($3.queryExpr()->queryRecord())
                                parser->reportError(ERR_AMBIGUOUS_PRODUCTION, $1, "Cannot use REPEAT on a rule with an associated row");

                            IHqlExpression * pattern = $3.getExpr();
                            $$.setExpr(createValue(no_pat_repeat, parser->getCompoundRuleType(pattern), pattern, $5.getExpr(), createValue(no_any, makeNullType()), $8.getExpr()));
                        }
    | REPEAT '(' pattern ',' expression ',' expression optMinimal ')'
                        {
                            parser->checkPattern($3, true);
                            parser->normalizeExpression($5, type_int, true);
                            parser->normalizeExpression($7, type_int, true);
                            if ($3.queryExpr()->queryRecord())
                                parser->reportError(ERR_AMBIGUOUS_PRODUCTION, $1, "Cannot use REPEAT on a rule with an associated row");

                            IHqlExpression * pattern = $3.getExpr();
                            $$.setExpr(createValue(no_pat_repeat, parser->getCompoundRuleType(pattern), pattern, $5.getExpr(), $7.getExpr(), $8.getExpr()));
                        }
    | OPT '(' pattern optConstExpression ')'
                        {
                            parser->checkPattern($3, true);
                            if ($4.queryExpr() && $3.queryExpr()->queryRecord())
                                parser->reportError(ERR_AMBIGUOUS_PRODUCTION, $1, "Cannot use OPT with count on a rule with an associated row");

                            IHqlExpression * pattern = $3.getExpr();
                            IHqlExpression * max = $4.getExpr();
                            if (!max) max = createConstantOne();
                            $$.setExpr(createValue(no_pat_repeat, parser->getCompoundRuleType(pattern), pattern, createConstant(0), max));
                        }
  //Not sure how much of a good idea these are....
    | pattern0 '*'      {
                            parser->checkPattern($1, true);
                            if ($1.queryExpr()->queryRecord())
                                parser->reportError(ERR_AMBIGUOUS_PRODUCTION, $1, "Cannot use * on a rule with an associated row");

                            IHqlExpression * pattern = $1.getExpr();
                            $$.setExpr(createValue(no_pat_repeat, parser->getCompoundRuleType(pattern), pattern));
                        }
    | pattern0 '+'      {
                            parser->checkPattern($1, true);
                            if ($1.queryExpr()->queryRecord())
                                parser->reportError(ERR_AMBIGUOUS_PRODUCTION, $1, "Cannot use + on a rule with an associated row");

                            IHqlExpression * pattern = $1.getExpr();
                            $$.setExpr(createValue(no_pat_repeat, parser->getCompoundRuleType(pattern), pattern, createConstantOne(), createValue(no_any, makeNullType())));
                        }
    | pattern0 '?'      {
                            parser->checkPattern($1, true);
                            IHqlExpression * pattern = $1.getExpr();
                            if ((pattern->getOperator() == no_pat_repeat) && (!pattern->hasAttribute(minimalAtom)))
                            {
                                HqlExprArray args;
                                unwindChildren(args, pattern);
                                args.append(*createAttribute(minimalAtom));
                                $$.setExpr(pattern->clone(args));
                                pattern->Release();
                            }
                            else
                                $$.setExpr(createValue(no_pat_repeat, parser->getCompoundRuleType(pattern), pattern, createConstant(0), createConstantOne()));
                        }
    | pattern0 '*' INTEGER_CONST
                        {
                            parser->normalizeExpression($3, type_int, true);
                            parser->checkPattern($1, true);
                            if ($1.queryExpr()->queryRecord())
                                parser->reportError(ERR_AMBIGUOUS_PRODUCTION, $1, "Cannot use * on a rule with an associated row");

                            IHqlExpression * pattern = $1.getExpr();
                            IHqlExpression * count = $3.getExpr();
                            $$.setExpr(createValue(no_pat_repeat, parser->getCompoundRuleType(pattern), pattern, count, LINK(count)));
                        }
    | '(' pattern ')'   {   $$.setExpr($2.getExpr()); }
    | TOKEN '(' pattern ')'     
                        {
                            $$.setExpr(createValue(no_pat_token, makeTokenType(), $3.getExpr()));
                            parser->checkPattern($$, false);
                        }
    | ANY               {
                            $$.setExpr(createValue(no_pat_anychar, makePatternType()));
                            parser->checkPattern($$, false);
                        }
    | MIN '(' pattern ')'           
                        {
                            OwnedHqlExpr pattern = $3.getExpr();
                            if (pattern->getOperator() == no_pat_repeat)
                            {
                                HqlExprArray args;
                                unwindChildren(args, pattern);
                                args.append(*createAttribute(minimalAtom));
                                pattern.setown(pattern->clone(args));
                            }
                            else if (pattern->getOperator() == no_pat_instance)
                                parser->reportError(ERR_EXPECTED_REPEAT, $3, "Expected a repeat (not an attribute)");
                            else
                                parser->reportError(ERR_EXPECTED_REPEAT, $3, "Expected a repeat");
                            $$.setExpr(pattern.getClear());
                        }
    | '[' beginList optPatternList ']'  
                        {
                            HqlExprArray args;
                            parser->endList(args);
                            parser->checkPattern($$, args);
                            $$.setExpr(parser->createPatternOr(args, $3));
                        }
    | TOK_PATTERN '(' expr ')'
                        {
                            parser->normalizeExpression($3, type_stringorunicode, true);
                            if (!$3.queryExpr()->isConstant())
                                parser->reportError(ERR_EXPECTED_CONST, $3, "Pattern requires a constant argument");

                            IHqlExpression * pattern = parser->convertPatternToExpression($3);
                            $$.setExpr(createValue(no_pat_pattern, makePatternType(), $3.getExpr(), pattern));
                            parser->checkPattern($$, false);
                        }
    | stringOrUnicodeConstExpr  
                        {
                            //wrap the constant in another item so that the types are correct.
                            $$.setExpr(createValue(no_pat_const, makePatternType(), $1.getExpr()));
                            parser->checkPattern($$, false);
                        }
    | globalValueAttribute
                        {
                            $$.setExpr(parser->processExprInPattern($1));
                            parser->checkPattern($$, false);
                        }
    | globalFeaturedPatternAttribute
    | SELF              {
                            ITypeInfo * selfType = LINK(parser->current_type);
                            if (!parser->current_type)
                            {
                                parser->reportError(ERR_SELFOUTSIDERULE, $1, "SELF used outside of a definition");
                                selfType = makePatternType();
                            }
                            $$.setExpr(createValue(no_self, selfType));
                        }
    | USE '(' stringConstExpr ')'
                        {
                            parser->checkUseLocation($1);
                            $$.setExpr(createValue(no_pat_use, makeRuleType(NULL), $3.getExpr()));
                        }
    | USE '(' UNKNOWN_ID ')'
                        {
                            parser->checkUseLocation($1);
                            IAtom * moduleName = parser->globalScope->queryName();
                            IHqlExpression * module = moduleName ? createAttribute(moduleAtom, createAttribute(moduleName)) : NULL;
                            IHqlExpression * id = createAttribute(nameAtom, createId($3.getId()));
                            $$.setExpr(createValue(no_pat_use, makeRuleType(NULL), id, module));
                        }
    | USE '(' UNKNOWN_ID '.' UNKNOWN_ID ')'
                        {
                            parser->checkUseLocation($1);
                            IHqlExpression * module = createAttribute(moduleAtom, createId($3.getId()));
                            IHqlExpression * id = createAttribute(nameAtom, createId($5.getId()));
                            $$.setExpr(createValue(no_pat_use, makeRuleType(NULL), module, id));
                        }
    | USE '(' recordDef ',' stringConstExpr ')'
                        {
                            parser->checkUseLocation($1);
                            OwnedHqlExpr record = $3.getExpr();
                            $$.setExpr(createValue(no_pat_use, makeRuleType(record->getType()), $5.getExpr()));
                        }
    | USE '(' recordDef ',' UNKNOWN_ID ')'
                        {
                            parser->checkUseLocation($1);
                            IAtom * moduleName = parser->globalScope->queryName();
                            IHqlExpression * module = moduleName ? createAttribute(moduleAtom, createAttribute(moduleName)) : NULL;
                            IHqlExpression * id = createAttribute(nameAtom, createId($5.getId()));
                            OwnedHqlExpr record = $3.getExpr();
                            $$.setExpr(createValue(no_pat_use, makeRuleType(record->getType()), id, module));
                        }
    | USE '(' recordDef ',' UNKNOWN_ID '.' UNKNOWN_ID ')'
                        {
                            parser->checkUseLocation($1);
                            IHqlExpression * module = createAttribute(moduleAtom, createId($5.getId()));
                            IHqlExpression * id = createAttribute(nameAtom, createId($7.getId()));
                            OwnedHqlExpr record = $3.getExpr();
                            $$.setExpr(createValue(no_pat_use, makeRuleType(record->getType()), module, id));
                        }
    | DEFINE '(' pattern ',' stringConstExpr ')'
                        {
                            OwnedHqlExpr pattern = $3.getExpr();
                            $$.setExpr(createValue(no_define, pattern->getType(), LINK(pattern), $5.getExpr()));
                            $$.setPosition($1);
                        }
    | PENALTY   '(' constExpression ')'
                        {
                            parser->normalizeExpression($3, type_int, false);
                            $$.setExpr(createValue(no_penalty, makePatternType(), $3.getExpr()));
                            parser->checkPattern($$, false);
                        }
    | GUARD '(' featureGuards ')'   
                        {
                            $$.setExpr(createValue(no_pat_guard, makePatternType(), $3.getExpr()));
                        }
    | CASE '(' pattern ')'      
                        {
                            OwnedHqlExpr pattern = $3.getExpr();
                            $$.setExpr(createValue(no_pat_case, pattern->getType(), LINK(pattern)));
                        }
    | NOCASE '(' pattern ')'        
                        {
                            OwnedHqlExpr pattern = $3.getExpr();
                            $$.setExpr(createValue(no_pat_nocase, pattern->getType(), LINK(pattern)));
                        }
    ;

globalFeaturedPatternAttribute
    : globalPatternAttribute
    | globalPatternAttribute featureModifiers
                        {
                            //Insert a node to represent the actual feature parameters
                            OwnedHqlExpr expr = $1.getExpr();
                            IHqlExpression * features = $2.getExpr();
                            IHqlExpression * cur = createValue(no_pat_featureactual, expr->getType(), LINK(expr->queryChild(0)), features);
                            $$.setExpr(cur);
                        }
    ;

globalPatternAttribute
    : patternOrRuleId
                        {
                            IHqlExpression * pattern = $1.getExpr();
                            $$.setExpr(pattern);
                        }
    | patternOrRuleFunction
                        {
                            parser->beginFunctionCall($1);
                        }
   '(' patternParameters ')'
                        {
                            IHqlExpression * pattern = parser->bindParameters($1, $4.getExpr());
                            $$.setExpr(pattern);
                        }
    ;

patternParameters
    : beginPatternParameters patternActualList endPatternParameters
                        {
                            $$.setExpr(parser->endFunctionCall(), $2);
                        }
    ;

beginPatternParameters
    :                   {   
                            parser->savedType.append(parser->current_type); 
                            parser->current_type = NULL; 
                            $$.clear();
                        }
    ;

endPatternParameters
    :                   {   
                            parser->current_type = (ITypeInfo *)parser->savedType.popGet();
                            $$.clear();
                        }
    ;

patternActualList
    : optPatternActualValue
    | patternActualList ',' optPatternActualValue
    ;

optPatternActualValue
    :                   {
                            parser->addActual($$, createOmittedValue());
                        }
    |   patternParamval {
                            parser->addActual($1, $1.getExpr());
                            $$.clear();
                        }
    | namedPatternActual
    ;

namedPatternActual
    : UNKNOWN_ID ASSIGN patternParamval
                        {
                            parser->addNamedActual($1, $1.getId(), $3.getExpr());
                            $$.clear($1);
                        }
    | NAMED UNKNOWN_ID ASSIGN patternParamval
                        {
                            parser->addNamedActual($1, $2.getId(), $4.getExpr());
                            $$.clear($1);
                        }
    ;

patternParamval
    : pattern
    | simpleType expression 
                        {
                            parser->normalizeExpression($2);
                            $$.inherit($2);
                        }
    ;

optMinimal
    :                   {   $$.setNullExpr(); }
    | ',' MIN           {   $$.setExpr(createAttribute(minimalAtom)); }
    ;

optPatternList
    :
    | patternList
    ;

patternList
    :   pattern         {
                            parser->addListElement($1.getExpr());
                            $$.clear();
                        }
    | patternList ',' pattern
                        {
                            parser->addListElement($3.getExpr());
                            $$.clear();
                        }
    ;

patternReference
    : patternSelector
    | patternReference '/' patternSelector
                        {
                            IHqlExpression * pattern = $3.getExpr();
                            $$.setExpr(createValue(no_pat_select, pattern->getType(), $1.getExpr(), pattern));
                        }
    ;

patternSelector
    : patternOrRuleRef
    | patternOrRuleRef  '[' expression ']'
                        {
                            parser->normalizeExpression($3, type_int, true);
                            IHqlExpression * pattern = $1.getExpr();
                            $$.setExpr(createValue(no_pat_index, pattern->getType(), pattern, $3.getExpr()));
                        }
    ;


patternOrRuleRef
    : patternOrRuleId
    | patternOrRuleFunction
                        {   
                            $1.release();
                            parser->reportError(ERR_PATTERNFUNCREF,$1,"Cannot yet reference pattern functions");
                            $$.clear();
                        }
    ;

patternOrRuleId
    : PATTERN_ID
    | moduleScopeDot PATTERN_ID leaveScope
                        {
                            $1.release();
                            $$.setExpr($2.getExpr());
                        }
    ;

patternOrRuleFunction
    : PATTERN_FUNCTION
    | moduleScopeDot PATTERN_FUNCTION leaveScope
                        {
                            $1.release();
                            $$.setExpr($2.getExpr());
                        }
    ;


//-----------------------------------------------------------------------------------

defineFeatureIdWithOptScope
    : defineFeatureId   
                        {
                            $$.setDefineId(parser->createDefineId(0, $1.getType()));
                            $$.setPosition($1);
                        }
    | scopeFlag defineFeatureId  
                        {
                            $$.setDefineId(parser->createDefineId((int)$1.getInt(), $2.getType()));
                            $$.setPosition($1);
                        }
    ;

defineFeatureId
    : FEATURE knownOrUnknownId  
                        {
                            ITypeInfo *type = makeFeatureType();
                            parser->beginDefineId($2.getId(), type);
                            $$.setType(type);
                            $$.setPosition($1);
                        }
    ;


featureParameters
    :
    | '{' featureIdList '}' 
                        {   parser->setFeatureParamsOwn($2.getExpr()); $$.clear(); }
    ;

featureIdList
    : featureId         {   $$.setExpr($1.getExpr()); }
    | featureIdList ',' featureId
                        {
                            $$.setExpr(createComma($1.getExpr(), $3.getExpr()));
                        }
    ;

featureDefine
    : simpleType        {
                            $$.setExpr(createValue(no_null, makeFeatureType(), createValue(no_featuretype, $1.getType())));
                        }
    | featureCompound
    ;

featureCompound
    : featureId
    | featureCompound '|' featureId
                        {
                            $$.setExpr(createValue(no_pat_or, makeFeatureType(), $1.getExpr(), $3.getExpr()));
                        }
    ;

featureList
    : featureCompound
    | featureList ',' featureCompound
                        {
                            $$.setExpr(createComma($1.getExpr(), $3.getExpr()));
                        }
    ;

featureGuards
    : featureGuard
    | featureGuards ',' featureGuard
                        {
                            $$.setExpr(createComma($1.getExpr(), $3.getExpr()));
                        }
    ;

featureGuard
    : featureId EQ featureCompound
                        {
                            IHqlExpression * feature = $1.getExpr();
                            IHqlExpression * value = $3.getExpr();
                            $$.setExpr(createValue(no_eq, makeBoolType(), feature, value));
                        }
    | featureId TOK_IN '[' featureList ']'
                        {
                            IHqlExpression * feature = $1.getExpr();
                            IHqlExpression * value = $4.getExpr();
                            $$.setExpr(createValue(no_eq, makeBoolType(), feature, value));
                        }
    | featureCompound               
                        {
                            IHqlExpression * value = $1.getExpr();
                            IHqlExpression * feature = parser->deduceGuardFeature(value, $1);
                            $$.setExpr(createValue(no_eq, makeBoolType(), feature, value));
                        }
    | featureId EQ constExpression
                        {
                            IHqlExpression * feature = $1.getExpr();
                            IHqlExpression * value = $3.getExpr();
                            $$.setExpr(createValue(no_eq, makeBoolType(), feature, value));
                        }
    | constExpression
                        {
                            parser->checkIntegerOrString($1);
                            IHqlExpression * value = $1.getExpr();
                            IHqlExpression * feature = parser->deduceGuardFeature(value, $1);
                            $$.setExpr(createValue(no_eq, makeBoolType(), feature, value));
                        }
    ;

featureId
    : FEATURE_ID
    | moduleScopeDot FEATURE_ID leaveScope
                        {
                            $1.release();
                            $$.setExpr($2.getExpr());
                        }
    ;


featureValue
    : featureCompound
    | constExpression
    ;

featureValueList
    : featureValue
    | featureValueList ',' featureValue
                        {
                            $$.setExpr(createComma($1.getExpr(), $3.getExpr()));
                        }
    ;


featureModifiers
    : '{' featureValueList '}'
                        {
                            parser->reportWarning(CategorySyntax, SeverityError, WRN_FEATURE_NOT_REPEAT, $1.pos, "Curly brackets are not used for repeats - they are reserved for future functionality");
                            $$.setExpr($2.getExpr());
                        }
    ;

//================================== productions to enable attributes ==========================

startDistributeAttrs :  { parser->enableAttributes(DISTRIBUTE); $$.clear(); } ;
startHeadingAttrs:      { parser->enableAttributes(HEADING); $$.clear(); } ;
startStoredAttrs:       { parser->enableAttributes(STORED); $$.clear(); } ;


//================================== end of syntax section ==========================


%%

/*
 * Try and process the next token, and see if it would result in the token being shifted,
 * rather than entering an error state.
 * Derived from the logic of the main parsing loop, but must not modify the state stack
 * - so uses a small temporary state stack to cope with chains of reductions of empty productions
 */
 
bool HqlGram::canFollowCurrentState(int yychar, const short * activeState)
{
  /* The state stack.  */
  const yytype_int16 * yyssp = activeState;
  const yytype_int16 * maxyyssp = yyssp;
  const unsigned MaxTempStates = 4;
  yytype_int16 tempstate[MaxTempStates];

  int yystate = *yyssp;
  int yyn;
  /* Lookahead token as an internal (translated) token number.  */
  int yytoken;
  /* The variables used to return semantic value and location from the
     action routines.  */


  /* The number of symbols on the RHS of the reduced rule.
     Keep to zero when no symbol should be popped.  */
  int yylen = 0;

  yytoken = 0;
  goto yybackup;

/*------------------------------------------------------------.
| yynewstate -- Push a new state, which is found in yystate.  |
`------------------------------------------------------------*/
 yynewstate:
  /* In all cases, when you get here, the value and location stacks
     have just been pushed.  So pushing a state here evens the stacks.  */
  yyssp++;

  assertex(yyssp > maxyyssp);
  if (yyssp - maxyyssp > MaxTempStates)
    return false;
  tempstate[yyssp-maxyyssp-1] = yystate;

  if (yystate == YYFINAL)
    return false;           // Accept -> fail since it will accept rather than shift

  goto yybackup;

/*-----------.
| yybackup.  |
`-----------*/
yybackup:

  /* Do appropriate processing given the current state.  Read a
     lookahead token if we need one and don't already have one.  */

  /* First try to decide what to do without reference to lookahead token.  */
  yyn = yypact[yystate];
  if (yyn == YYPACT_NINF)
    goto yydefault;

yyprocesschar:
  yytoken = YYTRANSLATE (yychar);

  /* If the proper action on seeing token YYTOKEN is to reduce or to
     detect an error, take that action.  */
  yyn += yytoken;
  if (yyn < 0 || YYLAST < yyn || yycheck[yyn] != yytoken)
    goto yydefault;
  yyn = yytable[yyn];
  if (yyn <= 0)
    {
      if (yyn == 0 || yyn == YYTABLE_NINF)
        return false;       // Error -> cannot follow
      yyn = -yyn;
      goto yyreduce;
    }
  /* Shift the lookahead token.  */
  return true;


/*-----------------------------------------------------------.
| yydefault -- do the default action for the current state.  |
`-----------------------------------------------------------*/
yydefault:
  yyn = yydefact[yystate];
  if (yyn == 0)
    return false;           // Error -> cannot follow
  goto yyreduce;


/*-----------------------------.
| yyreduce -- Do a reduction.  |
`-----------------------------*/
yyreduce:
  /* yyn is the number of a rule to reduce with.  */
  yylen = yyr2[yyn];

  yyssp -= (yylen);
  yylen = 0;

  /* Now `shift' the result of the reduction.  Determine what state
     that goes to, based on the state we popped back to and the rule
     number reduced by.  */

  yyn = yyr1[yyn];

  int prevstate = yyssp <= maxyyssp ? *yyssp : tempstate[yyssp - maxyyssp -1];
  yystate = yypgoto[yyn - YYNTOKENS] + prevstate;
  maxyyssp = yyssp;
  if (0 <= yystate && yystate <= YYLAST && yycheck[yystate] == prevstate)
    yystate = yytable[yystate];
  else
    yystate = yydefgoto[yyn - YYNTOKENS];

  goto yynewstate;

}

int yyuntranslate(int token)
{
    for (unsigned i=0; i< _elements_in(yytranslate); i++)
    {
        if (yytranslate[i] == token)
            return i;
    }
    return '?';
}

/* Cloned and modified from the verbose yyerror implementation */
static void eclsyntaxerror(HqlGram * parser, const char * s, short yystate, int token)
{
  int yyn = yypact[yystate];

  if (! (YYPACT_NINF < yyn && yyn <= YYLAST))
  {
    parser->syntaxError(s, token, NULL);
    return;
  }

  /* Start YYX at -YYN if negative to avoid negative indexes in YYCHECK.  */
  int yyxbegin = yyn < 0 ? -yyn : 0;

  /* Stay within bounds of both yycheck and yytname.  */
  int yychecklim = YYLAST - yyn + 1;
  int yyxend = yychecklim < YYNTOKENS ? yychecklim : YYNTOKENS;
  int expected[YYNTOKENS];
  int curExpected = 0;
  for (int yyx = yyxbegin; yyx < yyxend; ++yyx)
  {
    if (yycheck[yyx + yyn] == yyx && yyx != YYTERROR)
        expected[curExpected++] = yyuntranslate(yyx);
  }
  expected[curExpected++] = 0;
  parser->syntaxError(s, token, expected);
}
