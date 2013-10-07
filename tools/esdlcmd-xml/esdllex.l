%option nounistd
%option case-insensitive
%{
#include "platform.h"
#include <stdlib.h>
#include <string.h>
#include "esdlcomp.h"
#include "esdlgram.h"

extern YYSTYPE yylval;
extern unsigned linenum;

static enum { LSskip,LSclarion,LSsectionheader,LSsectionbody,LSsectionfooter} state=LSskip;
static int   opencurly=0;

static bool check_return(int);
static void output_text();
%}

digit [0-9]
hexdigit [0-9_a-f_A-F]
letter [a-z_A-Z]
esc \\([abfnrtv"'?\\]|[0-7]{1,3}|x[0-9a-fA-F]+)
alphanum [a-z_A-Z_0-9]
blank [ \t\r]
star [*]
dollar[$]

%x COMMENT

%%

\n  { linenum++; /*output_text();*/ }

{blank}+    { output_text(); }

module      { if (check_return( MODULE )) return MODULE; }
SCMexportdef    { if (check_return( SCMEXPORTDEF )) return SCMEXPORTDEF; }
SCMapi  { if (check_return( SCMAPI )) return SCMAPI; }
SCMinterface    { if (check_return( SCMINTERFACE )) return SCMINTERFACE; }
SCMenum { if (check_return( SCMENUM )) return SCMENUM; }
SCMclarion      { if (check_return( SCMCLARION )) return SCMCLARION; }
SCMend  { if (check_return( SCMEND )) return SCMEND; }
ESPdefexport    { if (check_return( ESPDEFEXPORT )) return ESPDEFEXPORT;}
ESPstruct   { if (check_return( ESPSTRUCT )) return ESPSTRUCTREF; else return ESPSTRUCT; }
ESPenum     { if (check_return( ESPENUM )) return ESPENUMREF; else return ESPENUM;       }
ESPrequest  { if (check_return( ESPREQUEST )) return ESPREQUEST; }
ESPresponse { if (check_return( ESPRESPONSE )) return ESPRESPONSE; }
ESPservice  { if (check_return( ESPSERVICE )) return ESPSERVICE; }
ESPmethod   { if (check_return( ESPMETHOD )) return ESPMETHOD; }
ESPmount    { if (check_return( ESPMOUNT )) return ESPMOUNT; }
ESParray    { yylval.setName((const char *) yytext); if (check_return( ESPTEMPLATE )) return ESPTEMPLATE; }
ESPuses     { if (check_return( ESPUSES )) return ESPUSES; }
ESPinclude  { if (check_return( ESPINCLUDE )) return ESPINCLUDE; }
ESPVersionDef   { if (check_return( ESPVERSIONDEF )) return ESPVERSIONDEF; }
const       { if (check_return( ESDL_CONST )) return ESDL_CONST; }
void        { if (check_return( _VOID )) return _VOID; }
char        { if (check_return( _CHAR )) return _CHAR; }
bool        { if (check_return( _BOOL )) return _BOOL; }
int     { if (check_return( _INT )) return _INT; }
unsigned    { if (check_return( _UNSIGNED )) return _UNSIGNED; }
short       { if (check_return( _SHORT )) return _SHORT; }
long        { if (check_return( _LONG )) return _LONG; }
float       { if (check_return( _FLOAT )) return _FLOAT; }
double      { if (check_return( _DOUBLE )) return _DOUBLE; }
in          { if (check_return( ESDL_IN )) return ESDL_IN; }
out         { if (check_return( ESDL_OUT )) return ESDL_OUT; }
inout       { if (check_return( INOUT )) return INOUT; }
size        { if (check_return( _SIZE )) return _SIZE; }
layout      { if (check_return( LAYOUT )) return LAYOUT; }
async       { if (check_return( ASYNC )) return ASYNC; }
callback    { if (check_return( ESDL_CALLBACK )) return ESDL_CALLBACK; }
virtual     { if (check_return( VIRTUAL )) return VIRTUAL; }
timeout     { if (check_return( TIMEOUT )) return TIMEOUT; }

string                  {yylval.setName((const char *) yytext); if (check_return( STRING )) return STRING; }

xsdNonPositiveInteger   {yylval.setName((const char *) yytext); if (check_return(XSDTYPE)) return XSDTYPE;}
xsdNegativeInteger      {yylval.setName((const char *) yytext); if (check_return(XSDTYPE)) return XSDTYPE;}
xsdNonNegativeInteger   {yylval.setName((const char *) yytext); if (check_return(XSDTYPE)) return XSDTYPE;}
xsdUnsignedLong         {yylval.setName((const char *) yytext); if (check_return(XSDTYPE)) return XSDTYPE;}
xsdUnsignedInt              {yylval.setName((const char *) yytext); if (check_return(XSDTYPE)) return XSDTYPE;}
xsdUnsignedShort            {yylval.setName((const char *) yytext); if (check_return(XSDTYPE)) return XSDTYPE;}
xsdUnsignedByte         {yylval.setName((const char *) yytext); if (check_return(XSDTYPE)) return XSDTYPE;}
xsdPositiveInteger      {yylval.setName((const char *) yytext); if (check_return(XSDTYPE)) return XSDTYPE;}
xsdNormalizedString     {yylval.setName((const char *) yytext); if (check_return(XSDTYPE)) return XSDTYPE;}
xsdBase64Binary         {yylval.setName((const char *) yytext); if (check_return(XSDTYPE)) return XSDTYPE;}
xsdBoolean                  {yylval.setName((const char *) yytext); if (check_return(XSDTYPE)) return XSDTYPE;}
xsdDecimal                  {yylval.setName((const char *) yytext); if (check_return(XSDTYPE)) return XSDTYPE;}
xsdInteger                  {yylval.setName((const char *) yytext); if (check_return(XSDTYPE)) return XSDTYPE;}
xsdString                   {yylval.setName((const char *) yytext); if (check_return(XSDTYPE)) return XSDTYPE; }
xsdByte                     {yylval.setName((const char *) yytext); if (check_return(XSDTYPE)) return XSDTYPE;}
xsdDuration                 {yylval.setName((const char *) yytext); if (check_return(XSDTYPE)) return XSDTYPE;}
xsdDateTime                 {yylval.setName((const char *) yytext); if (check_return(XSDTYPE)) return XSDTYPE;}
xsdTime                     {yylval.setName((const char *) yytext); if (check_return(XSDTYPE)) return XSDTYPE;}
xsdDate                     {yylval.setName((const char *) yytext); if (check_return(XSDTYPE)) return XSDTYPE;}
xsdYearMonth                {yylval.setName((const char *) yytext); if (check_return(XSDTYPE)) return XSDTYPE;}
xsdYear                     {yylval.setName((const char *) yytext); if (check_return(XSDTYPE)) return XSDTYPE;}
xsdMonthDay                 {yylval.setName((const char *) yytext); if (check_return(XSDTYPE)) return XSDTYPE;}
xsdDay                      {yylval.setName((const char *) yytext); if (check_return(XSDTYPE)) return XSDTYPE;}
xsdMonth                        {yylval.setName((const char *) yytext); if (check_return(XSDTYPE)) return XSDTYPE;}
xsdAnyURI                   {yylval.setName((const char *) yytext); if (check_return(XSDTYPE)) return XSDTYPE;}
xsdHexBinary                {yylval.setName((const char *) yytext); if (check_return(XSDTYPE)) return XSDTYPE;}
xsdBinary                   {yylval.setName((const char *) yytext); if (check_return(XSDTYPE)) return XSDTYPE;}
xsdQName                        {yylval.setName((const char *) yytext); if (check_return(XSDTYPE)) return XSDTYPE;}
xsdNOTATION                 {yylval.setName((const char *) yytext); if (check_return(XSDTYPE)) return XSDTYPE;}
xsdToken                        {yylval.setName((const char *) yytext); if (check_return(XSDTYPE)) return XSDTYPE;}
xsdLanguage                 {yylval.setName((const char *) yytext); if (check_return(XSDTYPE)) return XSDTYPE;}
xsdNMTOKEN                  {yylval.setName((const char *) yytext); if (check_return(XSDTYPE)) return XSDTYPE;}
xsdNMTOKENS                 {yylval.setName((const char *) yytext); if (check_return(XSDTYPE)) return XSDTYPE;}
xsdName                     {yylval.setName((const char *) yytext); if (check_return(XSDTYPE)) return XSDTYPE;}
xsdNCName                   {yylval.setName((const char *) yytext); if (check_return(XSDTYPE)) return XSDTYPE;}
xsdID                           {yylval.setName((const char *) yytext); if (check_return(XSDTYPE)) return XSDTYPE;}
xsdIDREF                        {yylval.setName((const char *) yytext); if (check_return(XSDTYPE)) return XSDTYPE;}
xsdIDREFS                   {yylval.setName((const char *) yytext); if (check_return(XSDTYPE)) return XSDTYPE;}
xsdENTITY                   {yylval.setName((const char *) yytext); if (check_return(XSDTYPE)) return XSDTYPE;}
xsdENTITIES                 {yylval.setName((const char *) yytext); if (check_return(XSDTYPE)) return XSDTYPE;}

(true|TRUE|Yes) {
    if (check_return( BOOL_CONST ))
    {
        yylval.setVal(1);
        return BOOL_CONST;
    }
}

(false|FALSE|No) {
    if (check_return( BOOL_CONST ))
    {
        yylval.setVal(0);
        return BOOL_CONST;
    }
}

L?\"([^"\\\n]|{esc})*\" {
    if (check_return( STRING_CONST ))
    {
        char *sval = (char *) malloc(yyleng+1);

        char *thestr=(char*)yytext;
        strncpy(sval, (char *)yytext, yyleng);
        sval[yyleng]=0;
        yylval.setVal(sval);

        return STRING_CONST;
    }
}

0x{hexdigit}+   {
                yylval.setVal((int)strtol((const char *)yytext, NULL, 16));
                if (check_return(INTEGER_CONST)) return INTEGER_CONST;
                }

{digit}+    { yylval.setVal(atoi ((const char *)yytext));
              if (check_return(INTEGER_CONST)) return INTEGER_CONST;
            }

{digit}+\.{digit}+((e|E)("+"|"-")?{digit}+)? { yylval.setVal(atof((const char *)yytext));
              if (check_return(DOUBLE_CONST)) return DOUBLE_CONST;
            }

\.{digit}+((e|E)("+"|"-")?{digit}+)? { yylval.setVal(atof((const char *)yytext));
              if (check_return(DOUBLE_CONST)) return DOUBLE_CONST;
            }


{letter}{alphanum}* {  yylval.setName((const char *) yytext);
                       if (check_return(ID)) return ID;
                    }

{dollar}{letter}{alphanum}* {  yylval.setName((const char *) yytext+1);
                       if (check_return(CONST_ID)) return CONST_ID;
                    }

{star}      { if (check_return(STAR)) return STAR; }
&           { if (check_return(UMBERSAND)) return UMBERSAND; }
"//".*\n    { /*if (state==LSskip) outs((const char*)yytext); */
              linenum++;
            }
"/*"                { BEGIN(COMMENT); nCommentStartLine = linenum; }
<COMMENT>"*/"       { BEGIN(0); nCommentStartLine = -1; }
<COMMENT>[^*\n]+    { }
<COMMENT>\n         { linenum++;  }
<COMMENT>"*"        { }

"*/"                { yyerror("No comment has started; \"*/\" is illegal"); }


.           { if (check_return(yytext[0])) return yytext[0];
            }

%%

#line 196 "esdllex.l"

bool check_return(int a)
{
    if (state==LSskip)
   {
      if (true)
      {
         switch(a)
         {
                case SCMINTERFACE:
                case SCMENUM:
                    state = LSsectionheader;
                    return true;

                case ESPSTRUCT:
                case ESPENUM:
                    state = LSsectionheader;
                    return false;

                case ESPREQUEST:
                case ESPRESPONSE:
                case ESPSERVICE:
                    state = LSsectionheader;
                    return true;
                case SCMAPI:
                case SCMEXPORTDEF:
                case ESPDEFEXPORT:
                case ESPINCLUDE:
                case ESPVERSIONDEF:
                    state = LSsectionfooter;
                    return true;
                case SCMCLARION:
                    state = LSclarion;
                    return false;
                default:
                    output_text();
                    return false;
            }
        }
        else
        {
         switch(a)
         {
                case MODULE:
                case SCMINTERFACE:
                    state = LSsectionheader;
                    return true;
                default:
                    output_text();
                    return false;
            }
        }
    }
    else if (state==LSclarion)
   {
        if (a == SCMEND)
        {
            state = LSskip;
        }
        else
            output_text();
        return false;
    }
    if (a == '{')
    {
        opencurly++;
        if (state==LSsectionheader)
            state = LSsectionbody;
    }
    else if (a == '}')
    {
        opencurly--;
        if (!opencurly && (state==LSsectionbody))
        {
            state = LSsectionfooter;
        }
    }
    else if ((a == ';')&&(state == LSsectionfooter))
        state = LSskip;

    return true;
}

#ifdef FLEX_SCANNER
int yywrap()
{
    return -1;
}
#endif

void output_text()
{
    if (state==LSclarion) {
    }
    else if (state==LSskip)
        outs((const char*)yytext);
}
