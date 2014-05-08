/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */
%option 8bit never-interactive
/*%option c++ yyclass="HqlLex"*/
%option prefix="eclyy"
%option nounistd
%option reentrant
%option noyywrap
%option case-insensitive

%{
#include "jmisc.hpp"
#include "hqlerrors.hpp"
#include "hql.hpp"
#include "hqlgram.hpp"
#include "eclrtl.hpp"

#include "hqlgram.h"

#define YY_DECL int HqlLex::doyyFlex(YYSTYPE & returnToken, yyscan_t yyscanner, HqlLex * lexer, bool lookup, const short * activeState)

//#define DEBUG_TOKEN 

#ifdef DEBUG_TOKEN
#define TraceReturnToken(id) PrintLog("Returning " #id " %s", name->str())
#else 
#define TraceReturnToken(id)
#endif

#ifdef FLEX_SCANNER
#define updatepos1          lexer->updatePosition(yyleng)
#define CUR_TOKEN_TEXT      yytext
#define CUR_TOKEN_LENGTH    yyleng
#else
#define updatepos1          lexer->updatePosition(CUR_TOKEN_LENGTH)
#define CUR_TOKEN_TEXT      lexer->yyText
#define CUR_TOKEN_LENGTH    lexer->yyLeng
#endif

#define setupdatepos        { lexer->setTokenPosition(returnToken); updatepos1; }
extern void hex2str(char * target, const char * digits, unsigned len);

#define LOOKUPSYM(sym)      (lookup && lexer->parserExpecting(sym, activeState))
#define RETURNSYM(sym)  \
        setupdatepos; \
        int mapped = lookup ? lexer->mapToken(sym) : sym; \
        if (mapped && LOOKUPSYM(mapped)) return mapped; \
        return lookupIdentifierToken(returnToken, lexer, lookup, activeState, CUR_TOKEN_TEXT)
        
#define RETURNHARD(sym) \
        setupdatepos; return sym;

int HqlLex::lookupIdentifierToken(YYSTYPE & returnToken, HqlLex * lexer, bool lookup, const short * activeState, const char * tokenText)
{
    if ((tokenText[0] == '$') && tokenText[1]==0)
        return '$';
        
    /* check $ */
    IIdAtom * cname = createIdAtom(tokenText);
    IAtom * name = cname->queryLower();
    if (lexer->macroParms)
    {
        StringBuffer macroval;
        if (lexer->macroParms->getProp(name->str(), macroval))
        {
            lexer->pushText(macroval.str());
            return lexer->yyLex(returnToken, lookup, activeState);
        }
    }
    
    IHqlExpression *expr; 
    //fprintf(stderr, "\nLookupSymbol %s\n",name->str());
    //Lookup expr last otherwise it leaks.
    if ( !lookup  || lexer->macroGathering || lexer->skipNesting|| !(expr=lexer->lookupSymbol(cname,returnToken)))
    {
        TraceReturnToken(UNKNOWN_ID);
        returnToken.setId(cname);
        return UNKNOWN_ID;
    }

    IHqlExpression * deprecated = queryMetaAttribute(deprecatedAtom, expr);
    if (deprecated)
    {
        IHqlExpression * alternative = deprecated->queryChild(0);
        StringBuffer alternativeText;
        if (alternative && alternative->queryValue())
            alternative->queryValue()->getStringValue(alternativeText);
        lexer->reportWarning(returnToken, ERR_DEPRECATED_ATTR, "Definition %s is marked as deprecated.  %s", name->str(), alternativeText.str());
    }                   
    
#if defined(TRACE_MACRO)
    PrintLog("MACRO>> set expr at %d:%d\n",lexer->yyLineNo,lexer->yyColumn);
#endif

    if (expr->isMacro())
    {
        int ret_id; 
    
        if (expr->isAction())
            ret_id = DEFINITIONS_MACRO;
        else
            ret_id = VALUE_MACRO;
        
        OwnedHqlExpr ownedExpr = expr;
        lexer->pushMacro(expr);
        return ret_id;
    }
    
    returnToken.setExpr(expr);
    node_operator op = expr->getOperator();
    int token = UNKNOWN_ID;
    bool isFunction = expr->isFunction();
    ITypeInfo * exprType = expr->queryType();
    if (isFunction)
        exprType = exprType->queryChildType();
    type_t etc = exprType->getTypeCode();

    switch (etc)
    {
    case type_sortlist:
        token = SORTLIST_ID;
        break;
    case type_void:
        token = isFunction ? ACTION_FUNCTION : ACTION_ID;
        break;
    case type_transform:
        token = isFunction ? TRANSFORM_FUNCTION : TRANSFORM_ID;
        break;
    case type_dictionary:
        if (op == no_typedef)
            token = DICTIONARY_TYPE_ID;
        else
            token = isFunction ? DICTIONARY_FUNCTION : DICTIONARY_ID;
        break;
    case type_groupedtable:
    case type_table:
        if (op == no_typedef)
            token = DATASET_TYPE_ID;
        else
            token = isFunction ? DATASET_FUNCTION : DATASET_ID;
        break;
    case type_record:
        token = isFunction ? RECORD_FUNCTION : RECORD_ID;
        break;
    case type_row:
        token = isFunction ? DATAROW_FUNCTION : DATAROW_ID;
        break;
    case type_pattern:
    case type_rule:
    case type_token:
        if (op == no_typedef)
            token = PATTERN_TYPE_ID;
        else
            token = isFunction ? PATTERN_FUNCTION : PATTERN_ID;
        break;
    case type_feature:
        token = FEATURE_ID;
        break;
    case type_event:
        token = isFunction ? EVENT_FUNCTION : EVENT_ID;
        break;
    case type_scope:
        token = isFunction ? SCOPE_FUNCTION : SCOPE_ID;
        break;
    case type_set:
        if (op == no_typedef)
            token = SET_TYPE_ID;
        else
        {
            ITypeInfo * childType = exprType->queryChildType();
            if (childType && isDatasetType(childType))
                token = isFunction ? LIST_DATASET_FUNCTION : LIST_DATASET_ID;
            else
                token = isFunction ? VALUE_FUNCTION : VALUE_ID;
        }
        break;
    default:
        {
            if (expr->isType())
            {
                token = ALIEN_ID;
            }
            else if (op == no_typedef)
            {
                token = TYPE_ID;
            }                           
            else if (op == no_enum)
            {
                token = ENUM_ID;
            }                           
            else
            { 
                token = isFunction ? VALUE_FUNCTION : VALUE_ID;
            }
        }
    }

    if (((op == no_param) && expr->hasAttribute(fieldAtom)) || (op == no_indirect))
    {
        switch (token)
        {
        case VALUE_ID:
            token = VALUE_ID_REF;
            break;
        }
    }
    
    //This only returns a known token if the parser is expecting it at that point, otherwise it returns UNKNOWN_ID - if that
    //is expected.  It allows a much cleaner grammar, and solves issues with names clashing because they've already been used.
    if (!lexer->parserExpecting(token, activeState) && lexer->parserExpecting(UNKNOWN_ID, activeState))
    {
        returnToken.release();
        returnToken.setId(cname);
        token = UNKNOWN_ID;
    }

    TraceReturnToken(token);
    return token;
}

%}


letter        [a-z_A-Z]
digit         [0-9]
bindigit      [0-1]
hexdigit      [a-fA-F0-9]
alphanum      [a-z_A-Z$_0-9]
alphanumcolon [a-z_A-Z$_0-9:@]
blank         [ \t\r]
slash         [/]
star          [*]
percent       [%]
lcurly        [\{]
rcurly        [\}]
dot           [\.]
hexpairs      ({hexdigit}{hexdigit})+
err_hexpairs  {hexdigit}({hexdigit}{hexdigit})*
xpathchars    [a-z_A-Z$_0-9:/\[\]@=!]
xpathseq      ([^}\r\n])+

%x COMMENT
%x CPP
%x SLSL
%%

"/*"                { setupdatepos; BEGIN(COMMENT); lexer->inComment = true; }
<COMMENT>"*/"       { 
                        updatepos1; 
                        BEGIN(0); 
                        lexer->inComment = false; 

                        int startpos = returnToken.pos.position;
                        int endpos = lexer->yyPosition;
                        if (lexer->yyBuffer[startpos+2] == '*' && isspace((unsigned char)lexer->yyBuffer[startpos+3]))
                            lexer->javaDocComment.clear().append(endpos-startpos-5, lexer->yyBuffer+startpos+3);
                    }
<COMMENT>[^*\n]+    { updatepos1; }
<COMMENT>\n         { updatepos1; lexer->updateNewline(); }
<COMMENT>"*"        { updatepos1; }

"*/"                { 
                        /* error pattern */
                        setupdatepos; 
                        assertex(!lexer->inComment); 
                        lexer->reportError(returnToken, ERR_COMMENT_NOTSTARTED,"Comment is not started: \"*/\" is illegal");
                    }                   

\r                  { updatepos1; }
\n                  { 
                        setupdatepos; lexer->updateNewline();
                        
                        #if defined(TRACE_MACRO)
                            PrintLog("MACRO>> Newline occurs: now at %d:%d\n", lexer->yyLineNo, lexer->yyColumn);
                        #endif
                    }

{blank}+            { setupdatepos; }

"//"                { setupdatepos; BEGIN(SLSL); }
<SLSL>\n            { updatepos1; lexer->updateNewline(); BEGIN(0); }
<SLSL>[^\n]+        { updatepos1; }

#ERROR              {
                        setupdatepos; 
                        if (lexer->macroGathering || lexer->skipNesting)
                            return SKIPPED;
                        lexer->doError(returnToken, true); 
                        return lexer->yyLex(returnToken, lookup, activeState);
                    }
#WARNING            {
                        setupdatepos; 
                        if (lexer->macroGathering || lexer->skipNesting)
                            return SKIPPED;
                        lexer->doError(returnToken, false); 
                        return lexer->yyLex(returnToken, lookup, activeState); 
                    }
#FOR                {
                        setupdatepos; 
                        if (lexer->macroGathering)
                            return SKIPPED;
                        lexer->beginNestedHash(HashStmtFor);
                        if (lexer->skipNesting)
                        {
                            lexer->skipNesting++;
                            return SKIPPED;
                        }
                        lexer->doFor(returnToken, false); 
                        return lexer->yyLex(returnToken, lookup, activeState); 
                    }
#FORALL             {
                        setupdatepos; 
                        if (lexer->macroGathering)
                            return SKIPPED;
                        lexer->beginNestedHash(HashStmtForAll);
                        if (lexer->skipNesting)
                        {
                            lexer->skipNesting++;
                            return SKIPPED;
                        }
                        lexer->doFor(returnToken, true); 
                        return lexer->yyLex(returnToken, lookup, activeState); 
                    }
#LOOP               {
                        setupdatepos; 
                        if (lexer->macroGathering)
                            return SKIPPED;
                        lexer->beginNestedHash(HashStmtLoop);
                        if (lexer->skipNesting)
                        {
                            lexer->skipNesting++;
                            return SKIPPED;
                        }
                        lexer->doLoop(returnToken); 
                        return lexer->yyLex(returnToken, lookup, activeState); 
                    }
#BREAK              {
                        setupdatepos; 
                        if (lexer->macroGathering)
                            return SKIPPED;
                        lexer->hasHashbreak = true;
                        if (lexer->skipNesting)
                            return SKIPPED;
                        return HASHBREAK; 
                    }
#IF                 {
                        setupdatepos; 
                        if (lexer->macroGathering)
                            return SKIPPED;
                        lexer->beginNestedHash(HashStmtIf);
                        if (lexer->skipNesting)
                        {
                            lexer->skipNesting++;
                            return SKIPPED;
                        }
                        lexer->doIf(returnToken, false);
                        return lexer->yyLex(returnToken, lookup, activeState); 
                    }
#EXPAND             {
                        setupdatepos; 
                        if (lexer->skipNesting || lexer->macroGathering)
                            return SKIPPED; 
                        lexer->doExpand(returnToken); 
                        return lexer->yyLex(returnToken, lookup, activeState); 
                    }
#DECLARE            { 
                        setupdatepos; 
                        if (lexer->skipNesting || lexer->macroGathering)
                            return SKIPPED; 
                        lexer->doDeclare(returnToken); 
                        return lexer->yyLex(returnToken, lookup, activeState); 
                    }
#SET                { 
                        setupdatepos; 
                        if (lexer->skipNesting || lexer->macroGathering)
                            return SKIPPED; 
                        lexer->doSet(returnToken, false); 
                        return lexer->yyLex(returnToken, lookup, activeState); 
                    }
#TRACE              { 
                        setupdatepos; 
                        if (lexer->skipNesting || lexer->macroGathering)
                            return SKIPPED; 
                        lexer->doTrace(returnToken); 
                        return lexer->yyLex(returnToken, lookup, activeState); 
                    }
#EXPORT             { 
                        setupdatepos; 
                        if (lexer->skipNesting || lexer->macroGathering)
                            return SKIPPED; 
                        lexer->doExport(returnToken, false); 
                        return lexer->yyLex(returnToken, lookup, activeState); 
                    }
#EXPORTXML              { 
                        setupdatepos; 
                        if (lexer->skipNesting || lexer->macroGathering)
                            return SKIPPED; 
                        lexer->doExport(returnToken, true); 
                        return lexer->yyLex(returnToken, lookup, activeState); 
                    }
#MANGLE             { 
                        setupdatepos; 
                        if (lexer->skipNesting || lexer->macroGathering)
                            return SKIPPED; 
                        lexer->doMangle(returnToken, false); 
                        return lexer->yyLex(returnToken, lookup, activeState); 
                    }
#DEMANGLE           { 
                        setupdatepos; 
                        if (lexer->skipNesting || lexer->macroGathering)
                            return SKIPPED; 
                        lexer->doMangle(returnToken, true); 
                        return lexer->yyLex(returnToken, lookup, activeState); 
                    }
#APPLY              { 
                        setupdatepos; 
                        if (lexer->skipNesting || lexer->macroGathering)
                            return SKIPPED; 
                        lexer->doApply(returnToken); 
                        return lexer->yyLex(returnToken, lookup, activeState); 
                    }
#APPEND             {
                        setupdatepos; 
                        if (lexer->skipNesting || lexer->macroGathering)
                            return SKIPPED; 
                        lexer->doSet(returnToken, true); 
                        return lexer->yyLex(returnToken, lookup, activeState); 
                    }
#CONSTANT           { 
                        setupdatepos;
                        if (lexer->skipNesting || lexer->macroGathering)
                            return SKIPPED; 
                        return(HASH_CONSTANT);
                    }
#IFDEFINED          {
                        setupdatepos; 
                        if (lexer->skipNesting || lexer->macroGathering)
                            return SKIPPED; 
                        lexer->doDefined(returnToken);
                        return lexer->yyLex(returnToken, lookup, activeState); 
                    }
#ELSE               {
                        setupdatepos;
                        if (lexer->macroGathering)
                            return SKIPPED;
                        return lexer->doElse(returnToken, lookup, activeState, false);
                    }
#ELSEIF             {
                        setupdatepos;
                        if (lexer->macroGathering)
                            return SKIPPED;
                        return lexer->doElse(returnToken, lookup, activeState, true);
                    }
#ELSIF              {
                        setupdatepos;
                        if (lexer->macroGathering)
                            return SKIPPED;
                        return lexer->doElse(returnToken, lookup, activeState, true);
                    }
#ELIF               {
                        setupdatepos;
                        if (lexer->macroGathering)
                            return SKIPPED;
                        return lexer->doElse(returnToken, lookup, activeState, true);
                    }
#DEBUG              { 
                        setupdatepos;
                        if (lexer->skipNesting || lexer->macroGathering)
                            return SKIPPED; 
                        return(HASH_OPTION); 
                    }
#GETDATATYPE        {
                        setupdatepos; 
                        if (lexer->skipNesting || lexer->macroGathering)
                            return SKIPPED; 
                        lexer->doGetDataType(returnToken); 
                        return lexer->yyLex(returnToken, lookup, activeState); 
                    }

#INMODULE           {
                        setupdatepos; 
                        if (lexer->skipNesting || lexer->macroGathering)
                            return SKIPPED; 
                        lexer->doInModule(returnToken); 
                        return lexer->yyLex(returnToken, lookup, activeState); 
                    }
#ISVALID            {
                        setupdatepos; 
                        if (lexer->skipNesting || lexer->macroGathering)
                            return SKIPPED; 
                        lexer->doIsValid(returnToken); 
                        return lexer->yyLex(returnToken, lookup, activeState); 
                    }
#ISDEFINED          {
                        setupdatepos; 
                        if (lexer->skipNesting || lexer->macroGathering)
                            return SKIPPED; 
                        bool defined = lexer->doIsDefined(returnToken);
//                      RETURNHARD(defined ? TOK_TRUE : TOK_FALSE);
                        lexer->pushText(defined ? "true" : "false");
                        return lexer->yyLex(returnToken, lookup, activeState);
                    }
#LINE               {
                        setupdatepos;
                        lexer->doLine(returnToken);
                        return lexer->yyLex(returnToken, lookup, activeState); 
                    }
#LINK               {
                        setupdatepos;
                        if (lexer->skipNesting || lexer->macroGathering)
                            return SKIPPED; 
                        return(HASH_LINK); 
                    }
#ONWARNING          { 
                        setupdatepos;
                        if (lexer->skipNesting || lexer->macroGathering)
                            return SKIPPED; 
                        return(HASH_ONWARNING); 
                    }
#OPTION             { 
                        setupdatepos;
                        if (lexer->skipNesting || lexer->macroGathering)
                            return SKIPPED; 
                        return(HASH_OPTION); 
                    }
#STORED             { 
                        setupdatepos;
                        if (lexer->skipNesting || lexer->macroGathering)
                            return SKIPPED; 
                        return(HASH_STORED); 
                    }
#TEXT               { 
                        setupdatepos; 
                        if (lexer->skipNesting || lexer->macroGathering || !lookup)
                            return SKIPPED; 
                        return lexer->doHashText(returnToken); 
                    }
#UNIQUENAME         {
                        setupdatepos; 
                        if (lexer->skipNesting || lexer->macroGathering)
                        {   
                            //PrintLog("lexer->skipNesting #UNIQUENAME");
                            return SKIPPED; 
                        }
                        lexer->doUniqueName(returnToken); 
                        return lexer->yyLex(returnToken, lookup, activeState); 
                    }
#WORKUNIT           { 
                        setupdatepos;
                        if (lexer->skipNesting || lexer->macroGathering)
                            return SKIPPED; 
                        return(HASH_WORKUNIT); 
                    }
__LINE__            {
                        setupdatepos; 
                        returnToken.setExpr(createIntegerConstant(lexer->yyLineNo, false));
                        return (INTEGER_CONST);
                    }
#REGION[^\n]*       { 
                        updatepos1;
                        return INTERNAL_READ_NEXT_TOKEN; 
                    }
#ENDREGION          {
                        updatepos1;
                        return INTERNAL_READ_NEXT_TOKEN; 
                    }
#END                { 
//Place #END last so the specialised versions are hit first
                        setupdatepos; 
                        if (lexer->macroGathering)
                            return SKIPPED;
                        return lexer->doEnd(returnToken, lookup, activeState);
                    }
#{letter}{alphanum}* {
//Trap any unknown #commands
                        /* otherwise, unknown # command */
                        setupdatepos; 
                        if (lexer->skipNesting)
                            return SKIPPED; 
                        StringBuffer msg("Unknown # command: ");
                        msg.append(CUR_TOKEN_TEXT);
                        lexer->reportError(returnToken, ERR_TMPLT_UNKNOWNCOMMAND, "%s", msg.str());
                        return lexer->yyLex(returnToken, lookup, activeState); 
                    }
        
ABS                 { RETURNSYM(ABS); }
ACOS                { RETURNSYM(ACOS); }
AFTER               { RETURNSYM(AFTER); }
AGGREGATE           { RETURNSYM(AGGREGATE); }
__ALIAS__           { RETURNSYM(ALIAS); }
ALL                 { RETURNSYM(ALL); }
ALLNODES            { RETURNSYM(ALLNODES); }
ANY                 { RETURNSYM(ANY); }
APPLY               { RETURNSYM(APPLY); }
_ARRAY_             { RETURNSYM(_ARRAY_); }
AS                  { RETURNSYM(AS); }
ASCII               { RETURNSYM(ASCII); }
ASIN                { RETURNSYM(ASIN); }
ASSERT              { RETURNSYM(TOK_ASSERT); }
ASSTRING            { RETURNSYM(ASSTRING); }
ATAN                { RETURNSYM(ATAN); }
ATAN2               { RETURNSYM(ATAN2); }
ATMOST              { RETURNSYM(ATMOST); }
AVE                 { RETURNSYM(AVE); }
BACKUP              { RETURNSYM(BACKUP); }
BEFORE              { RETURNSYM(BEFORE); }
BEST                { RETURNSYM(BEST); }
BETWEEN             { RETURNSYM(BETWEEN); }
BITMAP              { RETURNSYM(TOK_BITMAP); }
BIG_ENDIAN          { RETURNSYM(BIG); }
BLOB                { RETURNSYM(BLOB); }
BNOT                { RETURNSYM(BNOT); }
BUILD               { RETURNSYM(BUILD); }
BUILDINDEX          { RETURNSYM(BUILD); }
CARDINALITY         { RETURNSYM(CARDINALITY); }
CASE                { RETURNSYM(CASE); }
CATCH               { RETURNSYM(TOK_CATCH); }
CHECKPOINT          { RETURNSYM(CHECKPOINT); }
CHOOSE              { RETURNSYM(CHOOSE); }
CHOOSEN             { RETURNSYM(CHOOSEN); }
CHOOSEN:ALL         { RETURNSYM(CHOOSENALL); }
CHOOSESETS          { RETURNSYM(CHOOSESETS); }
CLUSTER             { RETURNSYM(CLUSTER); }
CLUSTERSIZE         { RETURNSYM(CLUSTERSIZE); }
COGROUP             { RETURNSYM(COGROUP); }
COMBINE             { RETURNSYM(COMBINE); }
__COMMON__          { RETURNSYM(__COMMON__); }
__COMPOUND__        { RETURNSYM(__COMPOUND__); }
COMPRESSED          { RETURNSYM(COMPRESSED); }
__COMPRESSED__      { RETURNSYM(__COMPRESSED__); }
COS                 { RETURNSYM(COS); }
COSH                { RETURNSYM(COSH); }
COUNT               { RETURNSYM(COUNT); }
COUNTER             { RETURNSYM(COUNTER); }
CRON                { RETURNSYM(CRON); }
CSV                 { RETURNSYM(CSV); }
DATASET             { RETURNSYM(DATASET); }
DEDUP               { RETURNSYM(DEDUP); }
DEFAULT             { RETURNSYM(DEFAULT); }
DEFINE              { RETURNSYM(DEFINE); }
DENORMALIZE         { RETURNSYM(DENORMALIZE); }
DEPRECATED          { RETURNSYM(DEPRECATED); }
DESC                { RETURNSYM(DESC); }
DESCEND             { RETURNSYM(DESC); }
DICTIONARY          { RETURNSYM(DICTIONARY); }
DISTRIBUTE          { RETURNSYM(DISTRIBUTE); }
DISTRIBUTED         { RETURNSYM(DISTRIBUTED); }
DISTRIBUTION        { RETURNSYM(DISTRIBUTION); }
DIV                 { RETURNSYM(DIV); }
DYNAMIC             { RETURNSYM(DYNAMIC); }
EBCDIC              { RETURNSYM(EBCDIC); }
ECLCRC              { RETURNSYM(ECLCRC); }
ELSE                { RETURNSYM(ELSE); }
ELSIF               { RETURNSYM(ELSEIF); }
ELSEIF              { RETURNSYM(ELSEIF); }
EMBED               { RETURNSYM(EMBED); }
EMBEDDED            { RETURNSYM(EMBEDDED); }
_EMPTY_             { RETURNSYM(_EMPTY_); }
ENCODING            { RETURNSYM(ENCODING); }
ENCRYPT             { RETURNSYM(ENCRYPT); }
END                 { RETURNHARD(END); }    // hard reserved to aid resyncing
ENUM                { RETURNSYM(ENUM); }
ENTH                { RETURNSYM(ENTH); }
ERROR               { RETURNSYM(TOK_ERROR); }
EVALUATE            { RETURNSYM(EVALUATE); }
EVENT               { RETURNSYM(EVENT); }
EVENTEXTRA          { RETURNSYM(EVENTEXTRA); }
EVENTNAME           { RETURNSYM(EVENTNAME); }
EXCEPT              { RETURNSYM(EXCEPT); }
EXCLUSIVE           { RETURNSYM(EXCLUSIVE); }
EXISTS              { RETURNSYM(EXISTS); }
EXP                 { RETURNSYM(EXP); }
EXPIRE              { RETURNSYM(EXPIRE); }
EXPORT              { RETURNHARD(EXPORT); }
EXTEND              { RETURNSYM(EXTEND); }
FAIL                { RETURNSYM(FAIL); }
FAILCODE            { RETURNSYM(FAILCODE); }
FAILMESSAGE         { RETURNSYM(FAILMESSAGE); }
FAILURE             { RETURNSYM(FAILURE); }
FEATURE             { RETURNSYM(FEATURE); }
FETCH               { RETURNSYM(FETCH); }
FEW                 { RETURNSYM(FEW); }
FILEPOSITION        { RETURNSYM(FILEPOSITION); }
FILTERED            { RETURNSYM(FILTERED); }
FIRST               { RETURNSYM(FIRST); }
FIXED               { RETURNSYM(TOK_FIXED); }
FLAT                { RETURNSYM(FLAT); }
FROM                { RETURNSYM(FROM); }
FORMAT              { RETURNSYM(FORMAT_ATTR); }
FORWARD             { RETURNSYM(FORWARD); }
FROMUNICODE         { RETURNSYM(FROMUNICODE); }
FROMXML             { RETURNSYM(FROMXML); }
FULL                { RETURNSYM(FULL); }
FUNCTION            { RETURNHARD(FUNCTION); }
GETENV              { RETURNSYM(GETENV); }
GLOBAL              { RETURNSYM(GLOBAL); }
GRAPH               { RETURNSYM(GRAPH); }
GROUP               { RETURNSYM(GROUP); }
GROUPBY             { RETURNSYM(GROUPBY); }
GROUPED             { RETURNSYM(GROUPED); }
GUARD               { RETURNSYM(GUARD); }
__GROUPED__         { RETURNSYM(__GROUPED__); }
HAVING              { RETURNSYM(HAVING); }
HEADING             { RETURNSYM(HEADING); }
HINT                { RETURNSYM(HINT); }
HOLE                { RETURNSYM(HOLE); }
HTTPCALL            { RETURNSYM(HTTPCALL); }
HTTPHEADER          { RETURNSYM(HTTPHEADER); }
IF                  { RETURNSYM(IF); }
IFF                 { RETURNSYM(IFF); }
IFBLOCK             { RETURNHARD(IFBLOCK); }
IGNORE              { RETURNSYM(TOK_IGNORE); }
IMPLEMENTS          { RETURNSYM(IMPLEMENTS); }
IMPORT              { RETURNSYM(IMPORT); }
INDEPENDENT         { RETURNSYM(INDEPENDENT); }
INDEX               { RETURNSYM(INDEX); }
INLINE              { RETURNSYM(INLINE); }
INTERNAL            { RETURNSYM(INTERNAL); }
INTERFACE           { RETURNHARD(INTERFACE); }
INTFORMAT           { RETURNSYM(INTFORMAT); }
ISNULL              { RETURNSYM(ISNULL); }
ISVALID             { RETURNSYM(ISVALID); }
ITERATE             { RETURNSYM(ITERATE); }
JOIN                { RETURNSYM(JOIN); }
JOINED              { RETURNSYM(JOINED); }
KEEP                { RETURNSYM(KEEP); }
KEYDIFF             { RETURNSYM(KEYDIFF); }
KEYED               { RETURNSYM(KEYED); }
KEYPATCH            { RETURNSYM(KEYPATCH); }
KEYUNICODE          { RETURNSYM(KEYUNICODE); }
LABELED             { RETURNSYM(LABELED); }
LABELLED            { RETURNSYM(LABELED); }
LAST                { RETURNSYM(LAST); }
LEFT                { RETURNSYM(LEFT); }
LENGTH              { RETURNSYM(LENGTH); }
LIBRARY             { RETURNSYM(LIBRARY); }
LIMIT               { RETURNSYM(LIMIT); }
LINKCOUNTED         { RETURNSYM(LINKCOUNTED); }
_LINKCOUNTED_       { RETURNSYM(LINKCOUNTED); }
LITERAL             { RETURNSYM(LITERAL); }
LITTLE_ENDIAN       { RETURNSYM(LITTLE); }
LN                  { RETURNSYM(LN); }
LOADXML             { RETURNSYM(LOADXML); }
LOCAL               { RETURNSYM(LOCAL); }
LOCALE              { RETURNSYM(LOCALE); }
LOCALFILEPOSITION   { RETURNSYM(LOCALFILEPOSITION); }
LOG                 { RETURNSYM(TOK_LOG); }
LOGICALFILENAME     { RETURNSYM(LOGICALFILENAME); }
LOOKUP              { RETURNSYM(LOOKUP); }
LOOP                { RETURNSYM(LOOP); }
LZW                 { RETURNSYM(LZW); }
MANY                { RETURNSYM(MANY); }
MAP                 { RETURNSYM(MAP); }
MATCHED             { RETURNSYM(MATCHED); }
MATCHLENGTH         { RETURNSYM(MATCHLENGTH); }
MATCHPOSITION       { RETURNSYM(MATCHPOSITION); }
MATCHROW            { RETURNSYM(MATCHROW); }
MATCHTEXT           { RETURNSYM(MATCHTEXT); }
MATCHUNICODE        { RETURNSYM(MATCHUNICODE); }
MATCHUTF8           { RETURNSYM(MATCHUTF8); }
MAX                 { RETURNSYM(MAX); }
MAXCOUNT            { RETURNSYM(MAXCOUNT); }
MAXLENGTH           { RETURNSYM(MAXLENGTH); }
MAXSIZE             { RETURNSYM(MAXSIZE); }
MERGE               { RETURNSYM(MERGE); }
MERGEJOIN           { RETURNSYM(MERGEJOIN); }
MIN                 { RETURNSYM(MIN); }
MODULE              { RETURNHARD(MODULE); }
MOFN                { RETURNSYM(MOFN); }
MULTIPLE            { RETURNSYM(MULTIPLE); }
NAMED               { RETURNSYM(NAMED); }
__NAMEOF__          { RETURNSYM(NAMEOF); }
NAMESPACE           { RETURNSYM(NAMESPACE); }
NOBOUNDCHECK        { RETURNSYM(NOBOUNDCHECK); }
NOCASE              { RETURNSYM(NOCASE); }
NOFOLD              { RETURNSYM(NOFOLD); }
NOHOIST             { RETURNSYM(NOHOIST); }
NOLOCAL             { RETURNSYM(NOLOCAL); }
NONEMPTY            { RETURNSYM(NONEMPTY); }
NOOVERWRITE         { RETURNSYM(NOOVERWRITE); }
NORMALIZE           { RETURNSYM(NORMALIZE); }
NOROOT              { RETURNSYM(NOROOT); }
NOSCAN              { RETURNSYM(NOSCAN); }
NOSORT              { RETURNSYM(NOSORT); }
__NOSTREAMING__     { RETURNSYM(__NOSTREAMING__); }
NOTHOR              { RETURNSYM(NOTHOR); }
NOTIFY              { RETURNSYM(NOTIFY); }
NOTRIM              { RETURNSYM(NOTRIM); }
NOXPATH             { RETURNSYM(NOXPATH); }
OF                  { RETURNSYM(OF); }
OMITTED             { RETURNSYM(OMITTED); }
ONCE                { RETURNSYM(ONCE); }
ONFAIL              { RETURNSYM(ONFAIL); }
ONLY                { RETURNSYM(ONLY); }
ONWARNING           { RETURNSYM(ONWARNING); }
OPT                 { RETURNSYM(OPT); }
ORDERED             { RETURNSYM(ORDERED); }
OUTER               { RETURNSYM(OUTER); }
OUTPUT              { 
                        //RETURNSYM(OUTPUT);
                        setupdatepos; 
                        if (!lookup)
                            return lookupIdentifierToken(returnToken, lexer, lookup, activeState, CUR_TOKEN_TEXT);
                        return(OUTPUT); 
                    }
OVERWRITE           { RETURNSYM(OVERWRITE); }
__OWNED__           { RETURNSYM(__OWNED__); }
PACKED              { RETURNSYM(PACKED); }
PARALLEL            { RETURNSYM(PARALLEL); }
PARSE               { RETURNSYM(PARSE); }
PARTITION           { RETURNSYM(PARTITION); }
PATTERN             { RETURNSYM(TOK_PATTERN); }
PENALTY             { RETURNSYM(PENALTY); }
PERSIST             { RETURNSYM(PERSIST); }
PHYSICALFILENAME    { RETURNSYM(PHYSICALFILENAME); }
PIPE                { RETURNSYM(PIPE); }
__PLATFORM__        { RETURNSYM(__PLATFORM__); }
POWER               { RETURNSYM(POWER); }
PREFETCH            { RETURNSYM(PREFETCH); }
PRELOAD             { RETURNSYM(PRELOAD); }
PRIORITY            { RETURNSYM(PRIORITY); }
PRIVATE             { RETURNSYM(PRIVATE); }
PROCESS             { RETURNSYM(PROCESS); }
PROJECT             { RETURNSYM(PROJECT); }
PROXYADDRESS        { RETURNSYM(PROXYADDRESS); }
PULL                { RETURNSYM(PULL); }
PULLED              { RETURNSYM(PULLED); }
QUOTE               { RETURNSYM(QUOTE); }
RANGE               { RETURNSYM(RANGE); }
RANK                { RETURNSYM(RANK); }
RANKED              { RETURNSYM(RANKED); }
REALFORMAT          { RETURNSYM(REALFORMAT); }
RECORD              { RETURNHARD(RECORD); }
RECORDOF            { RETURNSYM(RECORDOF); }
RECOVERY            { RETURNSYM(RECOVERY); }
REGEXFIND           { RETURNSYM(REGEXFIND); }
REGEXREPLACE        { RETURNSYM(REGEXREPLACE); }
REGROUP             { RETURNSYM(REGROUP); }
REJECTED            { RETURNSYM(REJECTED); }
RELATIONSHIP        { RETURNSYM(RELATIONSHIP); }
REMOTE              { RETURNSYM(REMOTE); }
REPEAT              { RETURNSYM(REPEAT); }
RESPONSE            { RETURNSYM(RESPONSE); }
RETRY               { RETURNSYM(RETRY); }
RETURN              { RETURNSYM(RETURN); }
RIGHT               { RETURNSYM(RIGHT); }
RIGHT{digit}+       { 
                        returnToken.setInt(str2int64(CUR_TOKEN_LENGTH-5, (const char *)CUR_TOKEN_TEXT+5, 10)); 
                        RETURNSYM(RIGHT_NN); 
                    }
ROLLUP              { RETURNSYM(ROLLUP); }
ROUND               { RETURNSYM(ROUND); }
ROUNDUP             { RETURNSYM(ROUNDUP); }
ROW                 { RETURNSYM(ROW); }
ROWS                { RETURNSYM(ROWS); }
ROWSET              { RETURNSYM(ROWSET); }
ROWDIFF             { RETURNSYM(ROWDIFF); }
RULE                { RETURNSYM(RULE); }
SAMPLE              { RETURNSYM(SAMPLE); }
SCAN                { RETURNSYM(SCAN); }
SECTION             { RETURNSYM(SECTION); }
SELF                { RETURNSYM(SELF); }
SEPARATOR           { RETURNSYM(SEPARATOR); }
__SEQUENCE__        { RETURNSYM(__SEQUENCE__); }
SEQUENTIAL          { RETURNSYM(SEQUENTIAL); }
SERVICE             { RETURNHARD(SERVICE); }
SET                 { RETURNSYM(SET); }
__SET_DEBUG_OPTION__ { RETURNSYM(HASH_OPTION); }
SHARED              { RETURNHARD(SHARED); }
SIN                 { RETURNSYM(SIN); }
SINGLE              { RETURNSYM(SINGLE); }
SINH                { RETURNSYM(SINH); }
SIZEOF              { RETURNSYM(SIZEOF); }
SKEW                { RETURNSYM(SKEW); }
SKIP                { RETURNSYM(SKIP); }
SMART               { RETURNSYM(SMART); }
SOAPACTION          { RETURNSYM(SOAPACTION); }
SOAPCALL            { RETURNSYM(SOAPCALL); }
SORT                { RETURNSYM(SORT); }
SORTED              { RETURNSYM(SORTED); }
SQL                 { RETURNSYM(SQL); }
SQRT                { RETURNSYM(SQRT); }
STABLE              { RETURNSYM(STABLE); }
STEPPED             { RETURNSYM(STEPPED); }
STORED              { RETURNSYM(STORED); }
STREAMED            { RETURNSYM(STREAMED); }
SUBSORT             { RETURNSYM(SUBSORT); }
SUCCESS             { RETURNSYM(SUCCESS); }
SUM                 { RETURNSYM(SUM); }
TABLE               { RETURNSYM(TABLE); }
TAN                 { RETURNSYM(TAN); }
TANH                { RETURNSYM(TANH); }
TERMINATOR          { RETURNSYM(TERMINATOR); }
ESCAPE              { RETURNSYM(ESCAPE); }
THEN                { RETURNSYM(THEN); }
THISNODE            { RETURNSYM(THISNODE); }
THOR                { RETURNSYM(THOR); }
THRESHOLD           { RETURNSYM(THRESHOLD); }
TIMEOUT             { RETURNSYM(TIMEOUT); }
TIMELIMIT           { RETURNSYM(TIMELIMIT); }
TOKEN               { RETURNSYM(TOKEN); }
TOPN                { RETURNSYM(TOPN); }
TOUNICODE           { RETURNSYM(TOUNICODE); }
TOXML               { RETURNSYM(TOXML); }
TRANSFER            { RETURNSYM(TRANSFER); }
TRANSFORM           { RETURNHARD(TRANSFORM); }
TRIM                { RETURNSYM(TRIM); }
TRUNCATE            { RETURNSYM(TRUNCATE); }
TYPE                { RETURNSYM(TYPE); }
TYPEOF              { RETURNSYM(TYPEOF); }
UNICODEORDER        { RETURNSYM(UNICODEORDER); }
UNGROUP             { RETURNSYM(UNGROUP); }
UNORDERED           { RETURNSYM(UNORDERED); }
UNSORTED            { RETURNSYM(UNSORTED); }
UNSTABLE            { RETURNSYM(UNSTABLE); }
UPDATE              { RETURNSYM(UPDATE); }
USE                 { RETURNSYM(USE); }
VALIDATE            { RETURNSYM(VALIDATE); }
VIRTUAL             { RETURNSYM(VIRTUAL); }
WAIT                { RETURNSYM(WAIT); }
WARNING             { RETURNSYM(TOK_WARNING); }
WHEN                { RETURNSYM(WHEN); }
WHICH               { RETURNSYM(WHICH); }
WIDTH               { RETURNSYM(WIDTH); }
WILD                { RETURNSYM(WILD); }
WITHIN              { RETURNSYM(WITHIN); }
WHOLE               { RETURNSYM(WHOLE); }
WORKUNIT            { RETURNSYM(WORKUNIT); }
XML                 { RETURNSYM(XML_TOKEN); }
XMLDECODE           { RETURNSYM(XMLDECODE); }
XMLDEFAULT          { RETURNSYM(XMLDEFAULT); }
XMLENCODE           { RETURNSYM(XMLENCODE); }
XMLNS               { RETURNSYM(XMLNS); }
XMLPROJECT          { RETURNSYM(XMLPROJECT); }
XMLTEXT             { RETURNSYM(XMLTEXT); }
XMLUNICODE          { RETURNSYM(XMLUNICODE); }
XPATH               { RETURNSYM(XPATH); }

__DEBUG__       {
                        setupdatepos; 
                        return (__DEBUG__);
                    }
__ECL_VERSION__     {
                        setupdatepos; 
                        returnToken.setExpr(createConstant(LANGUAGE_VERSION));
                        return (STRING_CONST);
                    }
__ECL_VERSION_MAJOR__ {
                        setupdatepos; 
                        returnToken.setExpr(createIntegerConstant(LANGUAGE_VERSION_MAJOR, false));
                        return (INTEGER_CONST);
                    }
__ECL_VERSION_MINOR__ {
                        setupdatepos; 
                        returnToken.setExpr(createIntegerConstant(LANGUAGE_VERSION_MINOR, false));
                        return (INTEGER_CONST);
                    }
__ECL_VERSION_SUBMINOR__ {
                        setupdatepos; 
                        returnToken.setExpr(createIntegerConstant(LANGUAGE_VERSION_SUB, false));
                        return (INTEGER_CONST);
                    }
__ECL_LEGACY_MODE__ {
                        setupdatepos;
                        returnToken.setExpr(createConstant(queryLegacyImportSemantics()));
                        return (BOOL_CONST);
                    }
__OS__              { 
                        setupdatepos; 
                        if (!lookup)
                            return lookupIdentifierToken(returnToken, lexer, lookup, activeState, CUR_TOKEN_TEXT);
#ifdef _WIN32
                        returnToken.setExpr(createConstant("windows"));
#else
                        returnToken.setExpr(createConstant("linux"));
#endif
                        return (STRING_CONST);
                    }
__STAND_ALONE__     {
                        setupdatepos; 
                        return (__STAND_ALONE__);
                    }
                        
{percent}{alphanumcolon}*{percent} {
                        setupdatepos;
                        if (lexer->skipNesting || lexer->macroGathering)
                            return SKIPPED;
                        lexer->doPreprocessorLookup(returnToken, false, 0);
                        return lexer->yyLex(returnToken, lookup, activeState); 
                    };

{percent}'{alphanumcolon}*'{percent} {
                        setupdatepos;
                        if (lexer->skipNesting || lexer->macroGathering)
                            return SKIPPED;
                        lexer->doPreprocessorLookup(returnToken, true, 1);
                        return lexer->yyLex(returnToken, lookup, activeState); 
                    };
{percent}\{{xpathseq}\}{percent} {
                        setupdatepos;
                        if (lexer->skipNesting || lexer->macroGathering)
                            return SKIPPED;
                        lexer->doPreprocessorLookup(returnToken, false, 1);
                        return lexer->yyLex(returnToken, lookup, activeState); 
                    };

{percent}'\{{xpathseq}\}'{percent} {
                        setupdatepos;
                        if (lexer->skipNesting || lexer->macroGathering)
                            return SKIPPED;
                        lexer->doPreprocessorLookup(returnToken, true, 2);
                        return lexer->yyLex(returnToken, lookup, activeState); 
                    };
INTEGER{digit}+ { 
                      setupdatepos; 
                      if (!lookup)
                        return lookupIdentifierToken(returnToken, lexer, lookup, activeState, CUR_TOKEN_TEXT);
                      unsigned size = atoi(CUR_TOKEN_TEXT+7);
                      if ((size == 0) || (size > 8))
                      {
                          lexer->reportError(returnToken, ERR_ILLSIZE_INT, "Invalid size for INTEGER type: can only be in range 1 to 8");
                          size = DEFAULT_INT_SIZE;
                      }
                      returnToken.setType(makeIntType(size, true)); 
                      return(SIMPLE_TYPE); 
                    }
INTEGER             { 
                        setupdatepos; 
                        if (!lookup)
                            return lookupIdentifierToken(returnToken, lexer, lookup, activeState, CUR_TOKEN_TEXT);
                        returnToken.setType(makeIntType(DEFAULT_INT_SIZE, true)); 
                        return(SIMPLE_TYPE); 
                    }
SIZE_T              {
                        setupdatepos; 
                        if (!lookup)
                            return lookupIdentifierToken(returnToken, lexer, lookup, activeState, CUR_TOKEN_TEXT);
                        returnToken.setType(makeIntType(sizeof(size32_t), false));
                        return(SIMPLE_TYPE);
                    }
BOOLEAN             { 
                        setupdatepos; 
                        if (!lookup)
                            return lookupIdentifierToken(returnToken, lexer, lookup, activeState, CUR_TOKEN_TEXT);
                        returnToken.setType(makeBoolType()); 
                        return(SIMPLE_TYPE); 
                    }
REAL{digit}+        { 
                        setupdatepos; 
                        if (!lookup)
                            return lookupIdentifierToken(returnToken, lexer, lookup, activeState, CUR_TOKEN_TEXT);
                      unsigned size = atoi(CUR_TOKEN_TEXT+4);
                      switch (size)
                      {
                      case 4:
                      case 8:
                          break;
                      default:
                          lexer->reportError(returnToken, ERR_ILLSIZE_REAL, "Invalid size for REAL type: can be 4 or 8 only");
                          size = DEFAULT_REAL_SIZE;
                          break;
                      }
                      returnToken.setType(makeRealType(size)); 
                      return(SIMPLE_TYPE); 
                    }
REAL                { 
                        setupdatepos; 
                        if (!lookup)
                            return lookupIdentifierToken(returnToken, lexer, lookup, activeState, CUR_TOKEN_TEXT);
                        returnToken.setType(makeRealType(DEFAULT_REAL_SIZE)); 
                        return(SIMPLE_TYPE);
                    }
DATA{digit}*        { 
                        setupdatepos; 
                        if (!lookup)
                            return lookupIdentifierToken(returnToken, lexer, lookup, activeState, CUR_TOKEN_TEXT);

                      unsigned size = lexer->getTypeSize(4);
                      ITypeInfo *dataType = makeDataType(size);
                      if (!dataType)
                      {
                            lexer->reportError(returnToken, ERR_ILLSIZE_DATA, "Invalid size for DATA type");
                            dataType = makeDataType(0);
                      }
                      returnToken.setType(dataType); 
                      return(SIMPLE_TYPE); 
                    }
STRING{digit}*      { 
                        setupdatepos; 
                        if (!lookup)
                            return lookupIdentifierToken(returnToken, lexer, lookup, activeState, CUR_TOKEN_TEXT);

                      unsigned size = lexer->getTypeSize(6);
                      ITypeInfo *strType = makeStringType(size,NULL,NULL);
                      if (!strType)
                      {
                            lexer->reportError(returnToken, ERR_ILLSIZE_STRING, "Invalid size for STRING type");
                            strType = makeStringType(1, NULL, NULL);
                      }
                      returnToken.setType(strType); 
                      return(SIMPLE_TYPE); 
                    }
UNICODE(_{letter}+)?{digit}* {
                      //Note letter includes _ so this include en_US etc.
                      setupdatepos; 
                        if (!lookup)
                            return lookupIdentifierToken(returnToken, lexer, lookup, activeState, CUR_TOKEN_TEXT);

                      unsigned digitstart = 7;
                      StringBuffer locale;
                      if(CUR_TOKEN_TEXT[digitstart] == '_')
                      {
                          digitstart++;
                          while(islower((unsigned char)CUR_TOKEN_TEXT[digitstart]) || isupper((unsigned char)CUR_TOKEN_TEXT[digitstart]) || (CUR_TOKEN_TEXT[digitstart] == '_'))
                            digitstart++;
                          getNormalizedLocaleName(digitstart-8, CUR_TOKEN_TEXT+8, locale);
                      }

                      unsigned length = lexer->getTypeSize(digitstart);
                      ITypeInfo *uniType = makeUnicodeType(length, createLowerCaseAtom(locale.str()));
                      if (!uniType)
                      {
                            lexer->reportError(returnToken, ERR_ILLSIZE_UNICODE, "Invalid size or locale for UNICODE type");
                            uniType = makeUnicodeType(1, 0);
                      }
                      returnToken.setType(uniType); 
                      return(SIMPLE_TYPE); 
                    }
UTF8(_{letter}+)?(_{digit}+)? {
                        setupdatepos; 
                        if (!lookup)
                            return lookupIdentifierToken(returnToken, lexer, lookup, activeState, CUR_TOKEN_TEXT);

                      unsigned digitstart = 4;
                      StringBuffer locale;
                      if(CUR_TOKEN_TEXT[digitstart] == '_' && isalpha((unsigned char)CUR_TOKEN_TEXT[digitstart+1]))
                      {
                          digitstart++;
                          while(islower((unsigned char)CUR_TOKEN_TEXT[digitstart]) || isupper((unsigned char)CUR_TOKEN_TEXT[digitstart]) || 
                                ((CUR_TOKEN_TEXT[digitstart] == '_') && isalpha((unsigned char)CUR_TOKEN_TEXT[digitstart+1])))
                              digitstart++;
                          getNormalizedLocaleName(digitstart-5, CUR_TOKEN_TEXT+5, locale);
                      }

                      if(CUR_TOKEN_TEXT[digitstart] == '_')
                          digitstart++;
                      unsigned length = lexer->getTypeSize(digitstart);
                      ITypeInfo *uniType = makeUtf8Type(length, createLowerCaseAtom(locale.str()));
                      if (!uniType)
                      {
                            lexer->reportError(returnToken, ERR_ILLSIZE_UNICODE, "Invalid size or locale for UTF8 type");
                            uniType = makeUtf8Type(1, 0);
                      }
                      returnToken.setType(uniType); 
                      return(SIMPLE_TYPE); 
                    }
VARSTRING{digit}*   { 
                        setupdatepos; 
                        if (!lookup)
                            return lookupIdentifierToken(returnToken, lexer, lookup, activeState, CUR_TOKEN_TEXT);

                      unsigned size = lexer->getTypeSize(9);
                      ITypeInfo *vStrType = makeVarStringType(size);
                      if (!vStrType)
                      {
                            lexer->reportError(returnToken, ERR_ILLSIZE_VARSTRING, "Invalid size for VARSTRING type");
                            vStrType = makeVarStringType(1);
                      }
                      returnToken.setType(vStrType); 
                      return(SIMPLE_TYPE); 
                    }
VARUNICODE(_{letter}+)?{digit}* {
                        setupdatepos; 
                        if (!lookup)
                            return lookupIdentifierToken(returnToken, lexer, lookup, activeState, CUR_TOKEN_TEXT);

                      unsigned digitstart = 10;
                      StringBuffer locale;
                      if(CUR_TOKEN_TEXT[digitstart] == '_')
                      {
                          digitstart++;
                          while(islower((unsigned char)CUR_TOKEN_TEXT[digitstart]) || isupper((unsigned char)CUR_TOKEN_TEXT[digitstart]) || (CUR_TOKEN_TEXT[digitstart] == '_'))
                              digitstart++;
                          getNormalizedLocaleName(digitstart-11, CUR_TOKEN_TEXT+11, locale);
                      }

                      unsigned length = lexer->getTypeSize(digitstart);
                      ITypeInfo *vUniType = makeVarUnicodeType(length, createLowerCaseAtom(locale.str()));
                      if(!vUniType)
                      {
                            lexer->reportError(returnToken, ERR_ILLSIZE_UNICODE, "Invalid size for VARUNICODE type");
                            vUniType = makeVarUnicodeType(1, 0);
                      }
                      returnToken.setType(vUniType); 
                      return(SIMPLE_TYPE); 
                    }
QSTRING{digit}*     { 
                        setupdatepos; 
                        if (!lookup)
                            return lookupIdentifierToken(returnToken, lexer, lookup, activeState, CUR_TOKEN_TEXT);

                      unsigned size = lexer->getTypeSize(7);
                      ITypeInfo * type = makeQStringType(size);
                      assertex(type);
                      returnToken.setType(type); 
                      return(SIMPLE_TYPE); 
                    }
DECIMAL             {
                        setupdatepos; 
                        if (!lookup)
                            return lookupIdentifierToken(returnToken, lexer, lookup, activeState, CUR_TOKEN_TEXT);

                      returnToken.setType(makeDecimalType(UNKNOWN_LENGTH,UNKNOWN_LENGTH, true));
                      return(SIMPLE_TYPE); 
                    }
UDECIMAL             {
                        setupdatepos; 
                        if (!lookup)
                            return lookupIdentifierToken(returnToken, lexer, lookup, activeState, CUR_TOKEN_TEXT);

                      returnToken.setType(makeDecimalType(UNKNOWN_LENGTH,UNKNOWN_LENGTH, true));
                      return(SIMPLE_TYPE); 
                    }
(U|u)?DECIMAL{digit}+(_{digit}+)? {
                        setupdatepos; 
                        if (!lookup)
                            return lookupIdentifierToken(returnToken, lexer, lookup, activeState, CUR_TOKEN_TEXT);

                      bool isSigned = (CUR_TOKEN_TEXT[0] != 'U') && (CUR_TOKEN_TEXT[0] != 'u');
                      const char * trailing = isSigned ? CUR_TOKEN_TEXT+7 : CUR_TOKEN_TEXT+8;
                      const char * underscore = strchr(trailing,'_');
                      unsigned digits = atoi(trailing);
                      unsigned places = underscore ? atoi(underscore+1) : 0;
                      if (places > digits)
                      {
                          lexer->reportError(returnToken, ERR_ILLSIZE_DECIMAL, "Decimal type cannot have precision>digits");
                          places = 0;
                      }
                      unsigned leading = digits - places;
                      if (leading > MAX_DECIMAL_LEADING)
                      {
                          lexer->reportError(returnToken, ERR_ILLSIZE_DECIMAL, "Decimal type contains too many leading digits (>%d)", MAX_DECIMAL_LEADING);
                          leading = MAX_DECIMAL_LEADING;
                      }
                      if (places > MAX_DECIMAL_PRECISION)
                      {
                          lexer->reportError(returnToken, ERR_ILLSIZE_DECIMAL, "Decimal type contains too many trailing digits (>%d)", MAX_DECIMAL_PRECISION);
                          places = MAX_DECIMAL_LEADING;
                      }

                      ITypeInfo *decType = makeDecimalType(leading+places, places, isSigned);
                      returnToken.setType(decType); 
                      return(SIMPLE_TYPE); 
                    }
BITFIELD            {
                        /* error pattern: BITFIELD with no size */
                        setupdatepos; 
                        if (!lookup)
                            return lookupIdentifierToken(returnToken, lexer, lookup, activeState, CUR_TOKEN_TEXT);

                       lexer->reportError(returnToken, ERR_ILLSIZE_BITFIELD, "Invalid size for BITFIELD type: a size must be specified");
                       returnToken.setType(makeBitfieldType(1)); 
                       return(SIMPLE_TYPE);     
                    }
BITFIELD{digit}+    {
                        setupdatepos; 
                        if (!lookup)
                            return lookupIdentifierToken(returnToken, lexer, lookup, activeState, CUR_TOKEN_TEXT);

                      int size = atoi(CUR_TOKEN_TEXT+8);
                      if (size<=0 || size>64)
                      {
                         lexer->reportError(returnToken, ERR_ILLSIZE_BITFIELD, "Invalid size for BITFIELD type: valid size 1..64");
                         size = 1;
                      }

                      ITypeInfo * type = makeBitfieldType(size);
                      assert(type!=NULL);

                      returnToken.setType(type); 
                      return(SIMPLE_TYPE); 
                    }
BITFIELD{digit}+_{digit}+ {
                        setupdatepos; 
                        if (!lookup)
                            return lookupIdentifierToken(returnToken, lexer, lookup, activeState, CUR_TOKEN_TEXT);

                      int size = atoi(CUR_TOKEN_TEXT+8);
                      const char * sep = strchr(CUR_TOKEN_TEXT+8, '_');
                      int baseSize = atoi(sep+1);

                      if (size<=0 || size>64)
                      {
                         lexer->reportError(returnToken, ERR_ILLSIZE_BITFIELD, "Invalid size for BITFIELD type: valid size 1..64");
                         size = 1;
                      }
                      switch (baseSize)
                      {
                      case 1: case 2: case 4: case 8:
                         break;
                      default:
                         lexer->reportError(returnToken, ERR_ILLSIZE_BITFIELD, "Invalid size for BITFIELD type: valid size 1..64");
                         baseSize = 8;
                      }
                      if (size > baseSize*8)
                         lexer->reportError(returnToken, ERR_ILLSIZE_BITFIELD, "Invalid base size not large enough for BITFIELD ");

                      ITypeInfo * type = makeBitfieldType(size, makeIntType(baseSize,false));
                      assert(type!=NULL);

                      returnToken.setType(type); 
                      return(SIMPLE_TYPE); 
                    }
RECORDSET           { 
                        setupdatepos; 
                        if (!lookup)
                            return lookupIdentifierToken(returnToken, lexer, lookup, activeState, CUR_TOKEN_TEXT);

                        returnToken.setType(makeTableType(NULL));
                        return(SIMPLE_TYPE);
                    }
SWAPPED             { 
                        setupdatepos; 
                        if (!lookup)
                            return lookupIdentifierToken(returnToken, lexer, lookup, activeState, CUR_TOKEN_TEXT);
                        return(SWAPPED);
                    }
UNSIGNED{digit}+    { 
                        setupdatepos; 
                        if (!lookup)
                            return lookupIdentifierToken(returnToken, lexer, lookup, activeState, CUR_TOKEN_TEXT);
                      unsigned size = atoi(CUR_TOKEN_TEXT+8);
                      if ((size == 0) || (size > 8))
                      {
                          lexer->reportError(returnToken, ERR_ILLSIZE_UNSIGNED, "Invalid size for UNSIGNED type: can only be 1 to 8");
                          size = DEFAULT_INT_SIZE;
                      }
                      returnToken.setType(makeIntType(size, false)); 
                      return(SIMPLE_TYPE); 
                    }
UNSIGNED            { 
                        setupdatepos; 
                        if (!lookup)
                            return lookupIdentifierToken(returnToken, lexer, lookup, activeState, CUR_TOKEN_TEXT);
                        return(UNSIGNED); 
                    }

TRUE                { RETURNSYM(TOK_TRUE); }
FALSE               { RETURNSYM(TOK_FALSE); }

AND                 { RETURNSYM(AND); }
OR                  { RETURNSYM(OR); }
NOT                 { RETURNSYM(NOT); }
HASHCRC             { RETURNSYM(CRC); }
HASH                { RETURNSYM(HASH); }
HASH32              { RETURNSYM(HASH32); }
HASH64              { RETURNSYM(HASH64); }
HASHMD5             { RETURNSYM(HASHMD5); }
RANDOM              { RETURNSYM(RANDOM); }
COVARIANCE          { RETURNSYM(COVARIANCE); }
CORRELATION         { RETURNSYM(CORRELATION); }
VARIANCE            { RETURNSYM(VARIANCE); }
~                   { setupdatepos; return(NOT); }
IN                  { RETURNSYM(TOK_IN); }
INNER               { RETURNSYM(INNER); }
OUT                 { RETURNSYM(TOK_OUT); }
CONST               { RETURNSYM(TOK_CONST); }
ENDMACRO            { setupdatepos; return(ENDMACRO); }
ENDC\+\+            { setupdatepos; return(ENDCPP); }
ENDEMBED            { setupdatepos; return(ENDEMBED); }
ENCRYPTED           {
                        setupdatepos; 
                        if (!lookup || !LOOKUPSYM(ENCRYPTED) || lexer->macroGathering)
                            return lookupIdentifierToken(returnToken, lexer, lookup, activeState, CUR_TOKEN_TEXT);
                        lexer->processEncrypted();
                        return ENCRYPTED;
                    }
FUNCTIONMACRO|MACRO { 
                        setupdatepos; 
                        if (!lookup)
                            return lookupIdentifierToken(returnToken, lexer, lookup, activeState, CUR_TOKEN_TEXT);

                        int tok;
                        ECLlocation startLocation;
                        bool complex = false;
                        startLocation.set(returnToken.pos);
                        int startpos = lexer->yyPosition;
                        int endpos = startpos;
                        bool isFunctionMacro = (CUR_TOKEN_LENGTH != 5);
                        lexer->macroGathering++;
                        while ((tok = lexer->yyLex(returnToken, false, activeState)) != ENDMACRO)
                        {
                            if (tok==EOF)
                            {
                                returnToken.setPosition(startLocation);
                                lexer->reportError(returnToken, ERR_MACRO_NOENDMACRO, "No matching ENDMACRO found");
                                lexer->macroGathering--;
                                return EOF;
                            }
                            else if ((tok == ASSIGN) || (tok == ';'))
                                complex = true;
                            else if (tok == '@')
                                isFunctionMacro = true;
                            returnToken.release();
                        }
                        lexer->macroGathering--;
                        endpos = lexer->yyPosition;
                        if(complex || isFunctionMacro)
                            endpos -= 8; // remove the ENDMACRO token
                        
                        // keep the orginal format info (like blanks, newlines) for macro
                        
                        int len = endpos-startpos;
                        Owned<IFileContents> macroContents;
                        if (!isFunctionMacro)
                        {
                            macroContents.setown(createFileContentsFromText(len, lexer->yyBuffer+startpos, lexer->sourcePath));
                        }
                        else
                        {
                            complex = false;
                            const char * prefix = " @FUNCTION ";
                            const char * suffix = " END ENDMACRO ";
                            unsigned lenPrefix = strlen(prefix);
                            unsigned lenSuffix = strlen(suffix);

                            char * macroBuf = (char *) malloc(len+lenPrefix+lenSuffix);
                            memcpy(macroBuf, prefix, lenPrefix);
                            memcpy(macroBuf + lenPrefix, lexer->yyBuffer+startpos, len);
                            memcpy(macroBuf + lenPrefix + len, suffix, lenSuffix);
                            macroContents.setown(createFileContentsFromText(lenSuffix + len + lenPrefix, macroBuf, lexer->sourcePath));
                            free(macroBuf);
                        }

#if defined(TRACE_TOKEN)
                        PrintLog("TOKEN>> macro body read: \"%s\"",macroBuf);
#endif

                        returnToken.setContents(macroContents.getClear());
                        returnToken.setPosition(startLocation);
#if defined(TRACE_MACRO)
                        PrintLog("MACRO>> macro defined in line %d, column %d\n", start.lineno, start.column);
#endif
                        
                        return complex ? COMPLEX_MACRO : MACRO;
                    }
"BEGINC++"          { 
                        //Need to process using an exclusive start condition unlike MACRO because many
                        //tokens in C++ aren't legal in ECL, so lexer->yyLex() will generate errors.
                        setupdatepos; 
                        BEGIN(CPP);
                        lexer->inCpp = true;
                    }
<CPP>[^\n]*("ENDC++"|"ENDEMBED")[^\n]*  {
                        lexer->inCpp = false;
                        int endpos = lexer->yyPosition;
                        //skip to the position of ENDC++ on the line (case insensitive)
                        while (memicmp(lexer->yyBuffer+endpos, "ENDC++", 6) != 0 && memicmp(lexer->yyBuffer+endpos, "ENDEMBED", 8) != 0)
                            endpos++;
                        const int lastpos = endpos + (tolower(lexer->yyBuffer[endpos+3])=='c' ? 6 : 8);
                            
                        updatepos1; 
                        BEGIN(0);
                        int startpos = returnToken.pos.position;
                        if (endpos-startpos >= 8 && memicmp(lexer->yyBuffer+startpos, "BEGINC++", 8)==0)
                            startpos += 8;
                        else
                            startpos += 1;  // Skip the ) of EMBED(xxx)

                        // keep the orginal format info (like blanks, newlines)
                        while (endpos != startpos && (lexer->yyBuffer[endpos-1] == 13 || lexer->yyBuffer[endpos-1] == 10))
                            endpos--;
                        int len = endpos-startpos;
#if defined(TRACE_TOKEN)
                        PrintLog("TOKEN>> C++ read: \"%.*s\"",len,lexer->yyBuffer+startpos);
#endif
                        //Return any characters found after the ENDC++
                        unsigned delta = lexer->yyPosition - lastpos;
                        yyless(CUR_TOKEN_LENGTH - delta);
                        lexer->yyPosition -= delta;
                        lexer->yyColumn -= delta;
                        OwnedHqlExpr cppText = createConstant(createUnicodeValue(lexer->yyBuffer+startpos, len, "", true, false));
                        OwnedHqlExpr annotated = createLocationAnnotation(cppText.getClear(), returnToken.pos);
                        OwnedHqlExpr options = extractCppBodyAttrs(len, lexer->yyBuffer+startpos);
                        returnToken.setExpr(createComma(annotated.getClear(), options.getClear()));
                        return CPPBODY;
                    }
<CPP>[^\n]+         { updatepos1; }
<CPP>\n             { updatepos1; lexer->updateNewline(); }

"<)"                { setupdatepos; return(TYPE_RPAREN) ; }
"(>"                { setupdatepos; return(TYPE_LPAREN) ; }
"<=>"               { setupdatepos; return(ORDER) ; }
"<="                { setupdatepos; return(LE) ; }
"<"                 { setupdatepos; return(LT) ; }
">="                {
                        //x.<y>= n  really wants to be processed as x.<y> = n, not x.<y >= 
                        if (LOOKUPSYM(GT) && !LOOKUPSYM(GE))
                        {
                            yyless(1);
                            setupdatepos; 
                            return(GT); 
                        }
                        setupdatepos; 
                        return(GE); 
                    }
">"                 { setupdatepos; return(GT) ; }
"!="                { setupdatepos; return(NE) ; }
"<>"                { setupdatepos; return(NE) ; }
"="                 { setupdatepos; return(EQ) ; }
:=                  { setupdatepos; return(ASSIGN); }
=>                  { setupdatepos; return(GOESTO); }
{dot}{dot}          { setupdatepos; return(DOTDOT); }
"<<"                { setupdatepos; return(SHIFTL); }
">>"                { setupdatepos; return(SHIFTR); }
"&&"                { setupdatepos; return(ANDAND); }
"<?>"               { setupdatepos; return(FIELD_REF) ; }
"<??>"              { setupdatepos; return(FIELDS_REF) ; }

0x{hexdigit}+U?     {
                        setupdatepos;
                        bool isSigned = toupper(CUR_TOKEN_TEXT[CUR_TOKEN_LENGTH-1]) != 'U';
                        unsigned len = isSigned ? CUR_TOKEN_LENGTH-2 : CUR_TOKEN_LENGTH-3;
                        returnToken.setExpr(createIntegerConstant(str2int64(len,(const char *)CUR_TOKEN_TEXT+2, 16), isSigned));
                        return(INTEGER_CONST);
                    }
{digit}{hexdigit}*XU? {
                        setupdatepos;
                        bool isSigned = toupper(CUR_TOKEN_TEXT[CUR_TOKEN_LENGTH-1]) != 'U';
                        unsigned len = isSigned ? CUR_TOKEN_LENGTH-1 : CUR_TOKEN_LENGTH-2;
                        returnToken.setExpr(createIntegerConstant(str2int64(len,(const char *)CUR_TOKEN_TEXT, 16), isSigned));
                        return(INTEGER_CONST);
                    }
0b{bindigit}+U?     {
                        setupdatepos;
                        bool isSigned = toupper(CUR_TOKEN_TEXT[CUR_TOKEN_LENGTH-1]) != 'U';
                        unsigned len = isSigned ? CUR_TOKEN_LENGTH-2 : CUR_TOKEN_LENGTH-3;
                        returnToken.setExpr(createIntegerConstant(str2int64(len,(const char *)CUR_TOKEN_TEXT+2, 2), isSigned));
                        return(INTEGER_CONST);
                    }
{bindigit}+BU?      {
                        setupdatepos;
                        bool isSigned = toupper(CUR_TOKEN_TEXT[CUR_TOKEN_LENGTH-1]) != 'U';
                        unsigned len = isSigned ? CUR_TOKEN_LENGTH-1 : CUR_TOKEN_LENGTH-2;
                        returnToken.setExpr(createIntegerConstant(str2int64(len,(const char *)CUR_TOKEN_TEXT, 2), isSigned));
                        return(INTEGER_CONST);
                    }
{digit}+U?          {
                        setupdatepos;
                        bool isSigned = toupper(CUR_TOKEN_TEXT[CUR_TOKEN_LENGTH-1]) != 'U';
                        unsigned len = isSigned ? CUR_TOKEN_LENGTH : CUR_TOKEN_LENGTH-1;
                        returnToken.setExpr(createIntegerConstant(str2int64(len, (const char *)CUR_TOKEN_TEXT, 10), isSigned));
                        return(INTEGER_CONST);
                    }
{digit}+(d|D)       { 
                        setupdatepos;
                        unsigned digits = CUR_TOKEN_LENGTH-1;
                        if (digits > MAX_DECIMAL_LEADING)
                        {
                            lexer->reportError(returnToken, ERR_ILLSIZE_DECIMAL, "Decimal constant contains too many digits (>%d)", MAX_DECIMAL_LEADING);
                            digits = MAX_DECIMAL_LEADING;
                        }
                        Owned<ITypeInfo> type = makeDecimalType(digits, 0, true);
                        IValue * value = type->castFrom(CUR_TOKEN_LENGTH-1, CUR_TOKEN_TEXT);
                        returnToken.setExpr(createConstant(value));
                        return(REAL_CONST);
                    }
{digit}*\.{digit}+(d|D) { 
                        setupdatepos;
                        const char * dot = strchr(CUR_TOKEN_TEXT, '.');
                        unsigned before = (size32_t)(dot-CUR_TOKEN_TEXT);
                        unsigned after = CUR_TOKEN_LENGTH-2 - before;
                        if (before > MAX_DECIMAL_LEADING)
                        {
                            lexer->reportError(returnToken, ERR_ILLSIZE_DECIMAL, "Decimal constant contains too many integral digits (>%d)", MAX_DECIMAL_LEADING);
                            before = MAX_DECIMAL_LEADING;
                            after = 0;
                        }
                        if (after > MAX_DECIMAL_PRECISION)
                        {
                            lexer->reportWarning(returnToken, ERR_ILLSIZE_DECIMAL, "Decimal constant may lose significant digits (>%d)", MAX_DECIMAL_PRECISION);
                            after = MAX_DECIMAL_PRECISION;
                        }
                        Owned<ITypeInfo> type = makeDecimalType(before+after, after, true);
                        IValue * value = type->castFrom(CUR_TOKEN_LENGTH-1, CUR_TOKEN_TEXT);
                        returnToken.setExpr(createConstant(value));
                        return(REAL_CONST);
                    }
{digit}+\.{digit}+((e|E)("+"|"-")?{digit}+)? {
                        setupdatepos; 
                        double value = strtod((const char *)CUR_TOKEN_TEXT, NULL);
                        returnToken.setExpr(createConstant(value)); 
                        return(REAL_CONST);
                    }
\.{digit}+((e|E)("+"|"-")?{digit}+)? {
                        setupdatepos; 
                        returnToken.setExpr(createConstant((double) atof ((const char *)CUR_TOKEN_TEXT))); 
                        return(REAL_CONST);
                    }

(u|U)(8)?\'([^'\r\n\\]|\\[^\r\n])*' {
                        int oldColumn = lexer->yyColumn; // in order to point out the exact position, old col needs to be saved.
                        setupdatepos;
                        StringBuffer msg;
                        bool utf8 = false;
                        unsigned start = 2;
                        if (CUR_TOKEN_TEXT[1] == '8')
                        {
                            utf8 = true;
                            start++;
                        }
                        unsigned ep;
                        if(!lexer->checkUnicodeLiteral(CUR_TOKEN_TEXT + start, CUR_TOKEN_LENGTH - (start+1), ep, msg))
                        {
                            lexer->yyColumn = oldColumn + ep + start;
                            returnToken.setPosition(lexer->yyLineNo, lexer->yyColumn, lexer->yyPosition, lexer->querySourcePath());
                            lexer->reportError(returnToken, ERR_ESCAPE_UNKNOWN, "%s", msg.str());
                            returnToken.setPosition(lexer->yyLineNo, oldColumn, lexer->yyPosition, lexer->querySourcePath());
                        }

                        size32_t size = CUR_TOKEN_LENGTH - (start+1);
                        const char * value = CUR_TOKEN_TEXT + start;
                        Owned<IValue> unicodeValue;
                        if (utf8)
                        {
                            size32_t length = rtlUtf8Length(size, value);
                            unicodeValue.setown(createUtf8Value(length, value, "", true));
                        }
                        else
                            unicodeValue.setown(createUnicodeValue(value, size, "", true, true));
                        
                        returnToken.setExpr(createConstant(unicodeValue.getClear()));
                        return (UNICODE_CONST);
                    }
                        

(d|D|q|Q|v|V)?\'([^'\r\n\\]|\\[^\r\n])*\' { 
                        int oldColumn = lexer->yyColumn; // in order to point out the exact position, old col needs to be saved.
                        setupdatepos; 
                        MemoryAttr tempBuff;
                        char *b = (char *)tempBuff.allocate(CUR_TOKEN_LENGTH); // Escape sequence can only make is shorter...
                        char *bf = b;
                        const char *finger = CUR_TOKEN_TEXT;
                        type_t tc = type_string;
                        if (*finger != '\'')
                        {
                            if ((*finger == 'd') || (*finger == 'D'))
                                tc = type_data;
                            else if((*finger == 'q') || (*finger == 'Q'))
                                tc = type_qstring;
                            else if((*finger == 'v') || (*finger == 'V'))
                                tc = type_varstring;
                            finger++;
                        }
                        for (finger++; finger[1]; finger++)
                        {
                            unsigned char next = *finger;
                            if (next == '\\')
                            {
                                next = finger[1];
                                if (finger[2]==0)  // finger[1] must be '.
                                {
                                    assertex(false);

                                    lexer->yyColumn = oldColumn + (size32_t)(finger-CUR_TOKEN_TEXT);
                                    returnToken.setPosition(lexer->yyLineNo, lexer->yyColumn, lexer->yyPosition, lexer->querySourcePath());
                                    StringBuffer msg("Can not terminate a string with escape char '\\': ");
                                    msg.append(CUR_TOKEN_TEXT);
                                    lexer->reportError(returnToken, RRR_ESCAPE_ENDWITHSLASH, "%s", msg.str());
                                    if (lexer->isAborting())
                                        return EOF;
                                    returnToken.setPosition(lexer->yyLineNo, oldColumn, lexer->yyPosition, lexer->querySourcePath());
                                }
                                else if (next == '\'' || next == '\\' || next == '?' || next == '"') 
                                {
                                    finger++;
                                } 
                                else if (next == 'a') 
                                {
                                    next = '\a';
                                    finger++;
                                } 
                                else if (next == 'b') 
                                {
                                    next = '\b';
                                    finger++;
                                } 
                                else if (next == 'f') 
                                {
                                    next = '\f';
                                    finger++;
                                } 
                                else if (next == 'n') 
                                {
                                    next = '\n';
                                    finger++;
                                } 
                                else if (next == 'r') 
                                {
                                    next = '\r';
                                    finger++;
                                } 
                                else if (next == 't') 
                                {
                                    next = '\t';
                                    finger++;
                                } 
                                else if (next == 'v') 
                                {
                                    next = '\v';
                                    finger++;
                                } 
                                else if (isdigit(next) && next < '8')
                                {
                                    //Allow octal constants for ^Z etc.
                                    unsigned value = 0;
                                    unsigned count;
                                    for (count=0; count < 3; count++)
                                    {
                                        next = finger[count+1];
                                        if (!isdigit(next) || next >= '8')
                                            break;
                                        value = value * 8 + (next - '0');
                                    }
                                    if(count != 3)
                                    {
                                        lexer->yyColumn = oldColumn + (size32_t)(finger+count+1-CUR_TOKEN_TEXT);
                                        returnToken.setPosition(lexer->yyLineNo, lexer->yyColumn, lexer->yyPosition, lexer->querySourcePath());
                                        StringBuffer msg;
                                        msg.append("3-digit numeric escape sequence contained non-octal digit: ").append(next);
                                        lexer->reportError(returnToken, ERR_ESCAPE_UNKNOWN, "%s", msg.str());
                                        if (lexer->isAborting())
                                            return EOF;
                                        returnToken.setPosition(lexer->yyLineNo, oldColumn, lexer->yyPosition, lexer->querySourcePath());
                                    }
                                    *bf++ = value;
                                    if(!(isValidAsciiLikeCharacter(value) || (tc == type_data)))
                                    {
                                        lexer->reportWarning(returnToken, ERR_STRING_NON_ASCII, "Character in string literal is not defined in encoding " ASCII_LIKE_CODEPAGE);
                                        if (lexer->isAborting())
                                            return EOF;
                                    }
                                    finger += count;
                                    continue;
                                }
                                else
                                {
                                    lexer->yyColumn = oldColumn + (size32_t)(finger-CUR_TOKEN_TEXT);
                                    returnToken.setPosition(lexer->yyLineNo, lexer->yyColumn, lexer->yyPosition, lexer->querySourcePath());
                                    StringBuffer msg;
                                    msg.append("Unrecognized escape sequence: ");
                                    msg.append("\\").append(finger[1]);
                                    lexer->reportError(returnToken, ERR_ESCAPE_UNKNOWN, "%s", msg.str());
                                    if (lexer->isAborting())
                                        return EOF;

                                    returnToken.setPosition(lexer->yyLineNo, oldColumn, lexer->yyPosition, lexer->querySourcePath());
                                }
                                *bf++ = next;
                            }
                            else if (next == '\'')
                            {
                                lexer->yyColumn = oldColumn + (size32_t)(finger-CUR_TOKEN_TEXT);
                                returnToken.setPosition(lexer->yyLineNo, lexer->yyColumn, lexer->yyPosition, lexer->querySourcePath());
                                lexer->reportError(returnToken, ERR_STRING_NEEDESCAPE,"' needs to be escaped by \\ inside string");
                                if (lexer->isAborting())
                                    return EOF;

                                returnToken.setPosition(lexer->yyLineNo, oldColumn, lexer->yyPosition, lexer->querySourcePath());
                            }
                            else if (next >= 128)
                            {
                                const byte * temp = (byte *)finger;
                                unsigned lenLeft = CUR_TOKEN_LENGTH - (size32_t)(finger - CUR_TOKEN_TEXT);
                                int extraCharsRead = rtlSingleUtf8ToCodepage(bf, lenLeft, finger, ASCII_LIKE_CODEPAGE);
                                if (extraCharsRead == -1)
                                {
                                    //This really has to be an error, otherwise it will work most of the time, but will then sometimes fail
                                    //because two characters > 128 are next to each other.
                                    lexer->reportError(returnToken, ERR_STRING_NON_ASCII, "Character in string literal is not legal UTF-8");
                                    if (lexer->isAborting())
                                        return EOF;
                                    *bf = next;
                                }
                                else
                                {
                                    finger += extraCharsRead;
                                    if (*bf == ASCII_LIKE_SUBS_CHAR)
                                        lexer->reportWarning(returnToken, ERR_STRING_NON_ASCII, "Character in string literal is not defined in encoding " ASCII_LIKE_CODEPAGE ", try using a unicode constant");
                                }
                                bf++;
                            }
                            else
                            {
                                *bf++ = next;
                                if(!(isValidAsciiLikeCharacter(next) || (tc == type_data)))
                                {
                                    lexer->reportError(returnToken, ERR_STRING_NON_ASCII, "Character in string literal is not defined in encoding " ASCII_LIKE_CODEPAGE);
                                    if (lexer->isAborting())
                                        return EOF;
                                }
                            }
                        }
                        switch (tc)
                        {
                        case type_qstring:
                            {
                                Owned<ITypeInfo> qStrType = makeQStringType(UNKNOWN_LENGTH); 
                                returnToken.setExpr(createConstant(qStrType->castFrom((size32_t)(bf-b), b)));
                                return (DATA_CONST);
                            }
                        case type_data:
                            {
                                returnToken.setExpr(createConstant(createDataValue(b, (size32_t)(bf-b))));
                                return (DATA_CONST);
                            }
                        case type_varstring:
                            {
                                returnToken.setExpr(createConstant(createVarStringValue((size32_t)(bf-b), b, makeVarStringType(UNKNOWN_LENGTH))));
                                return (DATA_CONST);
                            }
                        case type_string:
                            returnToken.setExpr(createConstant(createStringValue(b, (size32_t)(bf-b))));
                            return (STRING_CONST);
                        }
                    }

(d|D|q|Q|u|U|v|V)?\'([^'\r\n\\]|\\[^\r\n])*(\\)? {  
                        /* error pattern */
                        setupdatepos;
                        StringBuffer msg("String constant is not terminated: \"");
                        unsigned len = CUR_TOKEN_LENGTH;
                        if (len > 50)
                            msg.append(50, CUR_TOKEN_TEXT).append("...");
                        else
                            msg.append(CUR_TOKEN_TEXT);
                        msg.append("\"");
                        lexer->reportError(returnToken, ERR_STRING_UNENDED, "%s", msg.str());
                        // recovery 
                        returnToken.setExpr(createConstant(""));
                        return STRING_CONST;                        
                    }

\"([^"\r\n]|\\[\"])*(\")? {
                        /* error pattern */
                        setupdatepos;
                        lexer->reportError(returnToken, ERR_STRING_ILLDELIMITER, "\" is not legal string delimiter; use ' instead");
                        returnToken.setExpr(createConstant(""));
                        return STRING_CONST;
                    }
[xX]\'{hexpairs}\'  { 
                        setupdatepos;
                        unsigned len = (CUR_TOKEN_LENGTH-3)/2;    // round sign
                        char *data =(char *)malloc(len+1);
                        hex2str(data, (char *)CUR_TOKEN_TEXT+2, CUR_TOKEN_LENGTH-3);                        
                        returnToken.setExpr(createConstant(createDataValue(data, len)));
                        free(data);
                        return (DATA_CONST);
                    }

[xX]\'{err_hexpairs}\'  { 
                        /* error pattern: odd hex data */
                        setupdatepos;
                        lexer->reportWarning(returnToken, ERR_HEXDATA_ODDDIGITS,"hex data must have even number of hex digits");
                        char* str = (char*)malloc(CUR_TOKEN_LENGTH-1);
                        str[0] = '0'; // add a leading zero
                        strncpy(str+1,(char*)CUR_TOKEN_TEXT+2,CUR_TOKEN_LENGTH-3);
                        str[CUR_TOKEN_LENGTH-2] = 0;
                        unsigned len = (CUR_TOKEN_LENGTH-2)/2;    // round sign
                        char *data =(char *)malloc(len+1);
                        
                        hex2str(data, str, CUR_TOKEN_LENGTH-2);                     
                        returnToken.setExpr(createConstant(createDataValue(data, len)));
                        free(data);
                        free(str);
                        return (DATA_CONST);
                    }

[xX]\'\'            { 
                        setupdatepos;
                        returnToken.setExpr(createConstant(createDataValue(NULL, 0U)));
                        return (DATA_CONST);
                    }
 
[xX]\'([^'\r\n]|\\['])*\' { 
                        /* error pattern: illegal char in hex data */
                        setupdatepos;
                        StringBuffer msg;
                        msg.append("hex data can only contains 0-9a-fA-F: ");
                        msg.append(CUR_TOKEN_TEXT);
                        lexer->reportError(returnToken, ERR_HEXDATA_ILL, "%s", msg.str());

                        // recovering
                        char data[2], str[3] = "00";
                        
                        hex2str((char*)data, (char*)str, 2);                        
                        returnToken.setExpr(createConstant(createDataValue(data, 2)));
                        
                        return (DATA_CONST);
                    }
    
(\$|{letter}){alphanum}* {
                        setupdatepos;
                        return lookupIdentifierToken(returnToken, lexer, lookup, activeState, CUR_TOKEN_TEXT);
                    }
\357\273\277        {
                        setupdatepos;
                        if (lexer->yyPosition != 3)
                            lexer->reportWarning(returnToken, ERR_STRING_ILLDELIMITER, "Misplaced BOM - should be at the start of the file");
                    }

"("                 {
                        setupdatepos;
                        lexer->onOpenBra();
                        return '(';
                    }
")"                 {
                        setupdatepos;
                        lexer->onCloseBra();
                        return ')';
                    }
"["                 {
                        setupdatepos;
                        lexer->onOpenBra();
                        return '[';
                    }
"]"                 {
                        setupdatepos;
                        lexer->onCloseBra();
                        return ']';
                    }
{lcurly}            { 
                        setupdatepos;
                        lexer->onOpenBra();
                        return '{';
                    }
{rcurly}            {   
                        setupdatepos;
                        lexer->onCloseBra();
                        return '}';
                    }


.                   { setupdatepos; return (CUR_TOKEN_TEXT[0]); }
%%
void HqlLex::doEnterEmbeddedMode(yyscan_t yyscanner)
{
   struct yyguts_t * yyg = (struct yyguts_t*)yyscanner;
   BEGIN(CPP);
}