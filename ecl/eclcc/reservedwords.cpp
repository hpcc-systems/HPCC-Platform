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
#include "jfile.hpp"
#include "reservedwords.hpp"

struct Keywords
{
    unsigned int group;
    const char ** keywords;
};

static const char * eclReserved1[] = {//ECL fundamental types and associates
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
    "size_t",
    "string",
    "typeof",
    "udecimal",
    "unsigned",
    "utf8",
    "varstring",
    "varunicode",

    "any",
    "const",
    "out",
    "sizeof",

    "transfer",
     NULL
};

static const char * eclReserved2[] = { //Types - dataset/recodset
    "dataset",
    "dictionary",
    "index",
    "record",
    "recordof",
    "recordset",
    "row",
    "set",
    "set of",
     NULL
};

static const char * eclReserved3[] = { //Grouped by syntax structure
    "beginc++",
    "embed",
    "endembed",
    "end",
    "endc++",
    "endmacro",
    "function",
    "functionmacro",
    "ifblock",
    "interface",
    "macro",
    "module",
    "service",
    "transform",
    "type",

    //attributes/options
    "__nostreaming__",
    "return",
    NULL
};

static const char * eclReserved4[] = { //Dataset math
    "correlation",
    "covariance",
    "distribution",
    "variance",

    "exists",
    "within",
    NULL
};

static const char * eclReserved5[] = { //Dataset/recordset functions
    "_empty_",
    "aggregate",
    "choosen",
    "choosen:all",
    "choosesets",
    "combine",
    "dedup",
    "denormalize",
    "distribute",
    "enth",
    "fetch",
    "group",
    "having",
    "iterate",
    "join",
    "merge",
    "mergejoin",
    "nonempty",
    "normalize",
    "process",
    "project",
    "range",
    "rank",
    "ranked",
    "regroup",
    "rollup",
    "sample",
    "sort",
    "subsort",
    "topn",
    "ungroup",
    NULL
};

static const char * eclReserved6[] = { //Dataset/recordset attribute/options
    "__compressed__",
    "__grouped__",
    "all",
    "atmost",
    "bitmap",
    "cogroup",
    "counter",
    "desc", //short hand for descend
    "descend",
    "except",
    "exclusive",
    "few",
    "flat",
    "full",
    "groupby",
    "grouped",
    "inner",
    "joined",
    "keep",
    "keyed",
    "last",
    "left",
    "limit",
    "local",
    "lookup",
    "many",
    "mofn",
    "nosort",
    "only",
    "opt",
    "outer",
    "partition",
    "pulled",
    "right",
    "rowdiff",
    "rows",
    "rowset",
    "self",
    "skew",
    "skip",
    "smart",
    "stable",
    "sql",
    "thor",
    "threshold",
    "unordered",
    "unsorted",
    "unstable",
    "wild",
    NULL
};

static const char * eclReserved7[] = { //Dataset/recordset  action list related
    "apply",
    "ordered",
    "parallel",
    "sequential",
    NULL
};

static const char * eclReserved8[] = { //Dataset/recordset  action list related attributes/options
    "after",
    "before",
    NULL
};

static const char * eclReserved9[] = { //Internal dataset format specifiers
    "_array_",
    "_linkcounted_",
    "embedded",
    "linkcounted",
    "streamed",
    NULL
};

static const char * eclReserved10[] = { //Field modifiers
    "blob",
    "cardinality",
    "default",
    "maxcount",
    "maxlength",
    "packed",
    "xmldefault",
    "xpath",
    "virtual",

    //attributes/options
    "fileposition",
    "localfileposition",
    "logicalfilename",
    NULL
};

static const char * eclReserved11[] = { //ROXIE only - perhaps incomplete
    "allnodes",
    "dynamic",
    "library",
    "nolocal",
    "stepped",
    "thisnode",

    //attributes/options
    "filtered",
    "internal",
    "prefetch",
    NULL
};

static const char * eclReserved12[] = { //Compiler directive/hints
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

static const char * eclReserved13[] = { //Hashing functions
    "eclcrc",
    "hash",
    "hash32",
    "hash64",
    "hashcrc",
    "hashmd5",
    NULL
};

static const char * eclReserved14[] = { //NAMED related
    "__alias__",
    "__nameof__",
    "named",
    "workunit",
    NULL
};

static const char * eclReserved15[] = { //HTTP related
    "httpcall",
    "onfail",
    "proxyaddress",
    "response",
    "retry",
    "timelimit",
    "timeout",
    NULL
};

static const char * eclReserved16[] = { //SOAP related
	"soapaction",
    "soapcall",

    //attributes/options
    "httpheader",
    "literal",
    "namespace",
    NULL
};

static const char * eclReserved17[] = { //Dataset/expression-list iterators/filters
    "case",
    "choose",
    "elif", //ELSEIF
    "else",
    "elseif",
    "elsif", //ELSEIF
    "graph",
    "if",
    "iff",
    "loop",
    "map",
    "rejected",
    "then",
    "which",
    NULL
};

static const char * eclReserved18[] = { //Workflow services
    "__compound__",
    "__owned__",
    "checkpoint",
    "cron",
    "deprecated",
    "event",
    "eventextra",
    "eventname",
    "global",
    "independent",
    "labeled",
    "labelled",
    "notify",
    "once",
    "onwarning",
    "persist",
    "pipe",
    "priority",
    "private",
    "recovery",
    "section",
    "stored",
    "success",
    "wait",
    "when",
    NULL
};

static const char * eclReserved19[] = { //Error management
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
    "warning",
    NULL
};

static const char * eclReserved20[] = { //Template language
    "__set_debug_option__",
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
    NULL
};

static const char * eclReserved21[] = { //HPCC and OS environment settings
    "__debug__",
    "__ecl_legacy_mode__",
    "__ecl_version__",
    "__ecl_version_major__",
    "__ecl_version_minor__",
    "__ecl_version_subminor__",
    "__line__",
    "__os__",
    "__platform__",
    "__stand_alone__",

    "clustersize",
    "getenv",
    NULL
};

static const char * eclReserved22[] = { //XML related
    "fromxml",
    "loadxml",
    "noxpath",
    "toxml",
    "xml",
    "xmldecode",
    "xmlencode",
    "xmlproject",
    "xmltext",
    "xmlunicode",
    NULL
};

static const char * eclReserved23[] = { //ECL PARSE related
    "define",
    "parse",

    //attributes / options
    "best",
    "first", //ambiguous group - also flag to build, perhaps others
    "guard",
    "matched",
    "matchlength",
    "matchposition",
    "matchrow",
    "matchtext",
    "matchunicode",
    "matchutf8",
    "nocase",
    "noscan",
    "penalty",
    "repeat",
    "rule",
    "scan",
    "token",
    "use",
    "validate",
    "whole",

    "escape",
    NULL
};

static const char * eclReserved24[] = { //String functions
    "asstring",
    "ebcdic",
    "encoding",
    "fromunicode",
    "intformat",
    "keyunicode",
    "length",
    "locale",
    "realformat",
    "regexfind",
    "regexreplace",
    "tounicode",
    "trim",
    "unicodeorder",

    "notrim",
    NULL
};

static const char * eclReserved25[] = { //Math
    "acos",
    "asin",
    "atan",
    "atan2",
    "cos",
    "cosh",
    "div",
    "exp",
    "log",
    "ln",
    "power",
    "sin",
    "sinh",
    "sqrt",
    "tan",
    "tanh",
    NULL
};

static const char * eclReserved26[] = { //Math functions
    "abs",
    "ave",
    "count",
    "max",
    "min",
    "random",
    "round",
    "roundup",
    "sum",
    "truncate",
    NULL
};

static const char * eclReserved27[] = {
    "&",
    "|",
    //"^", ambiguous group location
    "bnot",
    "<<",
    ">>",
    NULL
};

static const char * eclReserved28[] = { //Comparison ops
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

static const char * eclReserved29[] = { //Binary ops
    "false",
    "true",

    "and",
    "not",
    "or",
    NULL
};

static const char * eclReserved30[] = { //Probably not used and/or correctly implemented
    "__sequence__",
    "feature",
    "omitted",
    NULL
};

static const char * eclReserved31[] = { //IMPORT/EXPORT related
    "as",
    "from",
    "export",
    "import",
    "shared",
    NULL
};

static const char * eclReserved32[] = { //BUILD/INDEX related
    "build",
    "buildindex",
    "keydiff",
    "keypatch",

    "compressed",
    "fixed",
    "lsw",
    "noroot",
    "remote",
    "width",
    NULL
};

static const char * eclReserved33[] = { //OUTPUT related
    "output",
    "table",

    //attributes
    "extend",
    "update",
    NULL
};

static const char * eclReserved34[] = { //File IO
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
    NULL
 };

static const char * eclReserved35[] = { //Heavily overloaded
    "maxsize",
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
        {18, eclReserved18},
        {19, eclReserved19},
        {20, eclReserved20},
        {21, eclReserved21},
        {22, eclReserved22},
        {23, eclReserved23},
        {24, eclReserved24},
        {25, eclReserved25},
        {26, eclReserved26},
        {27, eclReserved27},
        {28, eclReserved28},
        {29, eclReserved29},
        {30, eclReserved30},
        {31, eclReserved31},
        {32, eclReserved32},
        {33, eclReserved33},
        {34, eclReserved34},
        {35, eclReserved35}
};

void printKeywordsToXml()
{
     StringBuffer buffer;
     unsigned int nGroups = sizeof(keywordList)/sizeof(keywordList[0]);

     buffer.append("<xml>\n");
     for (int i = 0; i < nGroups; ++i)
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
     buffer.append("</xml>\n");

     Owned<IFile> treeFile = createIFile("ECLKeywords.xml");
     Owned<IFileIO> io = treeFile->open(IFOcreaterw);
     Owned<IIOStream> out = createIOStream(io);

     out->write(buffer.length(), buffer.str());
}
