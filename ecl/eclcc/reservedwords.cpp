/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2014 HPCC Systems®.

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
#include "jfile.hpp"
#include "reservedwords.hpp"

struct Keywords
{
    unsigned int group;
    const char ** keywords;
};

static const char * eclReserved1[] = { //Grouped by syntax structure
    "beginc++",
    "elif", //ELSEIF
    "else",
    "elseif",
    "elsif", //ELSEIF
    "embed",
    "endembed",
    "end",
    "endc++",
    "endmacro",
    "function",
    "functionmacro",
    "if",
    "ifblock",
    "iff",
    "interface",
    "macro",
    "module",
    "record",
    "service",
    "then",
    "transform",
    "type",
    NULL
};

static const char * eclReserved2[] = { //HPCC and OS environment settings
    "__debug__",
    "__ecl_legacy_mode__",
    "__ecl_version__",
    "__ecl_version_major__",
    "__ecl_version_minor__",
    "__ecl_version_subminor__",
    "__line__",
    "__os__",
    "__platform__",
    "__set_debug_option__",
    "__stand_alone__",

    "clustersize",
    "getenv",
    NULL
};

static const char * eclReserved3[] = { //Template language
    "#append",
    "#apply",
    "#break",
    "#constant",
    "#debug",
    "#declare",
    "#demangle",
    "#else",
    "#elseif",
    "#end",
    "#endregion",
    "#error",
    "#expand",
    "#export",
    "#exportxml",
    "#for",
    "#forall",
    "#getdatatype",
    "#if",
    "#ifdefined",
    "#inmodule",
    "#isdefined",
    "#isvalid",
    "#line",
    "#link",
    "#loop",
    "#mangle",
    "#onwarning",
    "#option",
    "#region",
    "#set",
    "#stored",
    "#text",
    "#trace",
    "#uniquename",
    "#warning",
    "#workunit",
    "loadxml",
    NULL
};

static const char * eclReserved4[] = { //String functions
    "asstring",
    "encoding",
    "fromunicode",
    "intformat",
    "keyunicode",
    "length",
    "realformat",
    "regexfind",
    "regexreplace",
    "tounicode",
    "toxml",
    "trim",
    "unicodeorder",
    NULL
};

static const char * eclReserved5[] = { //Math
    "abs",
    "acos",
    "asin",
    "atan",
    "atan2",
    "ave",
    "cos",
    "cosh",
    "count",
    "correlation",
    "covariance",
    "div",
    "exists",
    "exp",
    "log",
    "ln",
    "max",
    "min",
    "power",
    "random",
    "round",
    "roundup",
    "sin",
    "sinh",
    "sqrt",
    "sum",
    "tan",
    "tanh",
    "truncate",
    "variance",
    NULL
};

static const char * eclReserved6[] = {
    "&",
    "|",
    //"^", ambiguous group location
    "bnot",
    "<<",
    ">>",
    NULL
};

static const char * eclReserved7[] = { //Comparison ops
    "=",
    "<",
    "<=",
    ">",
    //">=",     //x.<y>= n  really wants to be processed as x.<y> = n, not x.<y >=
    "!=",
    "<>",
    "between",
    "in",

    "isnull",
    NULL
};

static const char * eclReserved8[] = { //Binary ops
    "false",
    "true",

    "and",
    "not",
    "or",
    NULL
};

static const char * eclReserved9[] = {//ECL fundamental types and associates
    "ascii",
    "big_endian",
    "bitfield",
    "boolean",
    "data",
    "decimal",
    "enum",
    "integer",
    "little_endian",
    "qstring",
    "real",
    "recordof",
    "recordset",
    "set",
    "set of",
    "size_t",
    "string",
    "typeof",
    "udecimal",
    "unsigned",
    "utf8",
    "varstring",
    "varunicode",
    NULL
};

static const char * eclReserved10[] = { //File IO
    "backup",
    "cluster",
    "encrypt",
    "expire",
    "heading",
    "multiple",
    "nooverwrite",
    "overwrite",
    "preload",
    "single",

    "csv",
    "quote",
    "separator",
    "terminator",
    "noxpath",
    NULL
 };

static const char * eclReserved11[] = {//Activities
    "aggregate",
    "allnodes",
    "case",
    "choose",
    "choosen",
    "choosesets",
    "combine",
    "dataset",
    "dedup",
    "denormalize",
    "dictionary",
    "distribute",
    "distribution",
    "_empty_",
    "enth",
    "fetch",
    "fromxml",
    "graph",
    "group",
    "having",
    "httpcall",
    "index",
    "iterate",
    "join",
    "loop",
    "map",
    "merge",
    "mergejoin",
    "nonempty",
    "normalize",
    "parse",
    "process",
    "project",
    "quantile",
    "range",
    "rank",
    "ranked",
    "regroup",
    "rejected",
    "rollup",
    "row",
    "sample",
    "sort",
    "stepped",
    "subsort",
    "table",
    "topn",
    "ungroup",
    "which",
    "within",
    "workunit",
    "xmldecode",
    "xmlencode",
    NULL
};

static const char * eclReserved12[] = {//Attributes
    "__alias__",
    "_array_",
    "cardinality",
    "__compound__",
    "__compressed__",
    "__grouped__",
    "_linkcounted_",
    "__nameof__",
    "__nostreaming__",
    "__owned__",
    "after",
    "all",
    "any",
    "best",
    "bitmap",
    "blob",
    "choosen:all",
    "const",
    "counter",
    "descend",
    "desc", //short hand for descend
    "ebcdic",
    "embedded",
    "except",
    "exclusive",
    "extend",
    "few",
    "fileposition",
    "filtered",
    "first",
    "fixed",
    "flat",
    "full",
    "grouped",
    "inner",
    "last",
    "left",
    "linkcounted",
    "literal",
    "local",
    "locale",
    "localfileposition",
    "logicalfilename",
    "lookup",
    "lzw",
    "many",
    "noroot",
    "noscan",
    "notrim",
    "only",
    "opt",
    "out",
    "outer",
    "packed",
    "pulled",
    "remote",
    "return",
    "right",
    "rows",
    "rule",
    "scan",
    "self",
    "smart",
    "sql",
    "streamed",
    "thor",
    "unordered",
    "unsorted",
    "whole",
    NULL
};

static const char * eclReserved13[] = { //Scalar functions
    "eclcrc",
    "hash",
    "hash32",
    "hash64",
    "hashcrc",
    "hashmd5",
    "matchlength",
    "matchposition",
    "matchrow",
    "rowdiff",
    "sizeof",
    "transfer",
    NULL
};


static const char * eclReserved14[] = { //Attribute functions (some might actually be activities though predominantly used as an attribute.)
    "atmost",
    "before",
    "cogroup",
    "compressed",
    "default",
    "escape",
    "global",
    "groupby",
    "guard",
    "httpheader",
    "internal",
    "joined",
    "keep",
    "keyed",
    "limit",
    "matched",
    "matchtext",
    "matchunicode",
    "matchutf8",
    "mofn",
    "maxcount",
    "maxlength",
    "maxsize",
    "named",
    "namespace",
    "nocase",
    "nolocal",
    "nosort",
    "onfail",
    "partition",
    "penalty",
    "prefetch",
    "proxyaddress",
    "repeat",
    "response",
    "retry",
    "rowset",
    "skew",
    "skip",
    "soapaction",
    "stable",
    "thisnode",
    "threshold",
    "timelimit",
    "timeout",
    "token",
    "unstable",
    "update",
    "use",
    "validate",
    "virtual",
    "width",
    "wild",
    "xml",
    "xmldefault",
    "xpath",
    "xmlproject",
    "xmltext",
    "xmlunicode",
    NULL
};

static const char * eclReserved15[] = { //Actions and statements
    "apply",
    "as",
    "build",
    "buildindex",
    "checkpoint",
    "cron",
    "define",
    "deprecated",
    "dynamic",
    "event",
    "eventextra",
    "eventname",
    "export",
    "from",
    "import",
    "independent",
    "keydiff",
    "keypatch",
    "labeled",
    "labelled",
    "library",
    "notify",
    "once",
    "onwarning",
    "ordered",
    "output",
    "parallel",
    "persist",
    "pipe",
    "priority",
    "private",
    "section",
    "sequential",
    "shared",
    "soapcall",
    "stored",
    "wait",
    "when",
    NULL
};

static const char * eclReserved16[] = { //Compiler directive/hints
    "__common__",
    "distributed",
    "evaluate",
    "forward",
    "hint",
    "noboundcheck",
    "nofold",
    "nohoist",
    "nothor",
    "pull",
    "sorted",
    NULL
};

static const char * eclReserved17[] = { //Error management
    "assert",
    "catch",
    "encrypted",
    "error",
    "fail",
    "failcode",
    "failmessage",
    "failure",
    "ignore",
    "isvalid",
    "onfail",
    "success",
    "recovery",
    "warning",
    NULL
};

static const char * eclReserved18[] = { //Probably not used and/or correctly implemented
    "__sequence__",
    "feature",
    "omitted",
    NULL
};

static const Keywords keywordList[] = {
   {1, eclReserved1},
   {2, eclReserved2},
   {3, eclReserved3},
   {4, eclReserved4},
   {5, eclReserved5},
   {6, eclReserved6},
   {7, eclReserved7},
   {8, eclReserved8},
   {9, eclReserved9},
   {10, eclReserved10},
   {11, eclReserved11},
   {12, eclReserved12},
   {13, eclReserved13},
   {14, eclReserved14},
   {15, eclReserved15},
   {16, eclReserved16},
   {17, eclReserved17},
   {18, eclReserved18}
};

void printKeywordsToXml()
{
     StringBuffer buffer;
     unsigned int nGroups = sizeof(keywordList)/sizeof(keywordList[0]);

     buffer.append("<xml>\n");
     for (unsigned i = 0; i < nGroups; ++i)
     {
         buffer.append("  <cat group=\"").append(keywordList[i].group).append("\">\n");
         unsigned int j = 0;
         while(keywordList[i].keywords[j])
         {
             buffer.append("    <keyword name=\"").append(keywordList[i].keywords[j]).append("\"/>\n");
             ++j;
         }
         buffer.append("  </cat>\n");
     }
     buffer.append("</xml>");
     fprintf(stdout, "%s\n", buffer.str());
}
