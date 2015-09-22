/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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
#ifndef _HQLERRORS_HPP_
#define _HQLERRORS_HPP_


///////////////////////////////////////////////////////////////////////////////

#define WRN_MACROEXPANSION          1001 /* error in macro (see error following this warning) */
#define WRN_LOCALNONDIST            1004 /* LOCAL specified on dataset that is not distributed */
#define WRN_LOCALONEXPLICITDIST     1005 /* Input is explicitly DISTRIBUTEd but LOCAL not specified */
#define WRN_CASENOCONDITION         1006 /* CASE does not have any conditions */
#define WRN_MODULE_DUPLICATED       1007 /* Module occurs twice in scope */

#define WRN_UNSUPPORTED_FEATURE     1010 /* Unsupported language feature */
#define WRN_EMPTY_STATEMENT         1012 /* ';' is not legal here (empty statement is not allowed) */
#define WRN_TRANX_HASASSIGNEDVALUE  1013 /* A field in transform already assigned a value */
#define WRN_USRTYPE_EXTRAPHYLEN     1014 /* physicalLength is not needed, ignored */
#define WRN_MODULE_IMPORTED         1015 /* Module is already imported */
#define WRN_OBSOLETED_SYNTAX        1016 /* Syntax obsoleted: use alternative */
#define WRN_TRANX_EMPTYASSIGNALL    1017 /* Empty assignall has no effort */
#define WRN_SVC_UNSUPPORTED_ATTR    1018 /* Unsupported service attribute */
#define WRN_SVC_ATTRNEEDNOVALUE     1019 /* An service attribute does not need a value */
#define WRN_MODULE_NAMEMISMATCH     1020 /* Module name mismatch */
#define WRN_OUTPUT_ALL_CONSTANT     1021 /* OUTPUT record contains no variables - probably an error */
#define WRN_COND_ALWAYS_TRUE        1022 /* Condition is always true */
#define WRN_COND_ALWAYS_FALSE       1023 /* Condition is always false */
#define WRN_HASHWARNING             1024 /* #WARNING statement */
#define WRN_DUPLICATECASE           1025 /* Duplicate case values */
#define WRN_LOCALIGNORED            1026 /* Local will be ignored for a grouped dataset */
#define WRN_LOCALEXPECTED           1027 /* Local expected */
#define WRN_GROUPINGIGNORED         1028 /* Grouping will be ignored. */
#define WRN_STRING_NON_ASCII        1029 /* No longer used (this is now an error, not a warning) */
#define WRN_COUNTER_NOT_USED        1030 /* Counter not used in transform */
#define WRN_MERGE_NOT_SORTED        1031 /* Merge inputs don't appear to be sorted */
#define WRN_MERGE_BAD_SORT          1032 /* Merge inputs don't appear to match */
#define WRN_DEFINITION_SANDBOXED    1033 /* Definition is sandboxed */
#define WRN_REFERENCE_SANDBOX       1034 /* Definition reference sandbox */
#define WRN_FEATURE_NOT_REPEAT      1035 /* regular expression syntax */
#define WRN_CHOOSEN_ALL             1036 /* deprecated syntax */
#define WRN_NOT_SORTED              1037 /* group input does not appear to be sorted */
#define WRN_SORT_INVARIANT          1038
#define WRN_POSSIBLE_FILTER         1039
#define WRN_BAD_LOOPFLAG            1040
#define WRN_COND_ASSIGN_NO_PREV     1042
#define WRN_NOT_INTERFACE           1043
#define WRN_PACKED_MAY_CHANGE       1044
#define WRN_MERGE_RECOMMEND_SORTED  1045
#define WRN_AMBIGUOUSRECORDMAXLENGTH 1046
#define WRN_FILENAMEIGNORED         1047
#define WRN_EXPORT_IGNORED          1048
#define WRN_RECORDMANYFIELDS        1049
#define WRN_RESERVED_FUTURE         1050 /* Identifier likely to be reserved in future versions */
#define WRN_SILLY_EXISTS            1051
#define WRN_INT_OR_RANGE_EXPECTED   1052 /* Integer or integer range (i.e. 2..3) expected when real detected */
#define WRN_UNRESOLVED_SYMBOL       1053
#define WRN_REQUIRES_SIGNED         1054
#define WRN_DISALLOWED              1055

//Do not define any warnings > 1099 - use the range below instead

///////////////////////////////////////////////////////////////////////////////
/* Error numbers */

/* type error */
#define ERR_ABORT_PARSING           2000
#define ERR_TYPEMISMATCH_REAL       2001 /* Type mismatch - Real value expected */
#define ERR_TYPEMISMATCH_INT        2002 /* Type mismatch - Integer value expected */
#define ERR_TYPEMISMATCH_STRING     2003 /* Type mismatch - String value expected */
#define ERR_TYPEMISMATCH_INTREAL    2004 /* Type mismatch - Integer or real value expected */
#define ERR_TYPEMISMATCH_TABLE      2005 /* Table type mismatch */
#define ERR_TYPEMISMATCH_RECORD     2006 /* Record type mismatch */
#define ERR_TYPE_INCOMPATIBLE       2007 /* Incompatible types */
#define ERR_TYPE_NOPARAMNEEDED      2008 /* Type does not require parameters */
#define ERR_TYPE_EXPECTED           2009 /* Boolean/Set of/Integer/Real/String/Table/Record/Unknown value expected */
#define ERR_TYPEOF_ILLOPERAND       2010 /* Illegal operand for TYPEOF */
#define ERR_USRTYPE_NESTEDDECL      2011 /* Cannot nest user TYPE declarations */
#define ERR_TYPEMISMATCH_DATASET    2012 /* Datasets must be the same type */
#define ERR_TYPE_DIFFER             2013 /* The types must be the same */ 
#define ERR_ELEMENT_NO_TYPE         2014 /* List element has unknown type */

#define ERR_TYPEERR_INT             2015 /* Integer type expected */
#define ERR_TYPEERR_INTDECIMAL      2016 /* Integer or decimal type expected */
#define ERR_TYPEERR_STRING          2017 /* String type expected */
#define ERR_LIST_INDEXINGALL        2018 /* Indexing ALL is undefined */
#define ERR_TYPEMISMATCH_INTSTRING  2019 /* Type mismatch - Integer or string value expected */

/* Illegal in context */
#define ERR_COUNT_ILL_HERE          2020 /* COUNT not valid in this context */
#define ERR_COUNTER_ILL_HERE        2021 /* COUNTER not valid in this context */
#define ERR_LEFT_ILL_HERE           2022 /* LEFT not legal here */
#define ERR_RIGHT_ILL_HERE          2023 /* RIGHT not legal here */
#define ERR_JOIN_ILL_HERE           2024 /* JOIN is not valid here */
#define ERR_SELF_ILL_HERE           2025 /* SELF not legal here */
#define ERR_ALL_ILL_HERE            2026 /* ALL not allowed here */
#define ERR_JOINED_ILL_HERE         2027 /* JOINED can only be specified inside SORT or SORTED */
#define ERR_NOVALUE                 2028 /* A value must be supplied for this attribute */
#define ERR_CONSTANT_DAFT           2029 /* Constant doesn't make any sense here */
#define ERR_BADKIND_LOOKUPJOIN      2030 /* Illegal join flags for a lookup join */
#define ERR_DEDUP_FILE_NOT_SUPPORTED 2031 /* DEDUP(file) not supported (or meaningful) on hole files */
#define ERR_SCHEDULING_DEFINITION   2032 /* Scheduling not allowed on an attribute (scheduling a definition makes no sense) */
#define ERR_MULTIPLE_WORKFLOW       2033 /* Multiple workflow clauses */
#define ERR_EXPECTED_INDEX          2034 /* Expected INDEX()... */
#define ERR_TRANSFORM_TYPE_MISMATCH 2035 /* Type returned from transform must match the source dataset type */
#define ERR_KEYEDNOTMATCHDATASET    2036 /* Keyed parameter does not match the dataset passed as a second parameter */
#define ERR_BUILD_INDEX_NAME        2037 /* Name of base file was not supplied and cannot be deduced */
#define ERR_BUILD_INDEX_BIAS        2038 /* BIAS not supplied and can't be calculated */
#define ERR_STRING_NON_ASCII        2039 /* Character in string literal is not defined in encoding */

/* Illegal size */
#define ERR_ILLSIZE_INT             2040 /* Invalid size for INTEGER type */
#define ERR_ILLSIZE_REAL            2041 /* Invalid size for REAL type */
#define ERR_ILLSIZE_DATA            2042 /* Invalid size for DATA type */
#define ERR_ILLSIZE_STRING          2043 /* Invalid size for STRING type */
#define ERR_ILLSIZE_VARSTRING       2044 /* Invalid size for VARSTRING type */
#define ERR_ILLSIZE_DECIMAL         2045 /* Invalid size for DECIMAL type */
#define ERR_ILLSIZE_UDECIMAL        2046 /* Invalid size for UDECIMAL type */
#define ERR_ILLSIZE_BITFIELD        2047 /* Invalid size for BITFIELD type */
#define ERR_ILLSIZE_UNSIGNED        2048 /* Invalid size for UNSIGNED type */
#define ERR_ILLSIZE_QSTRING         2049 /* Invalid size for QSTRING type */

/* macro */
#define ERR_MACRO_RECURSIVE         2050 /* Recursive macro call */
#define ERR_MACRO_NOENDMACRO        2051 /* No matching ENDMACRO found */
#define ERR_MACRO_EOFINPARAM        2052 /* EOF encountered while gathering macro parameters */
#define ERR_MACRO_NOPARAMTYPE       2053 /* No type is allowed for macro parameter */
#define ERR_MACRO_CONSTDEFPARAM     2054 /* Default value for macro parameter must be constant */
#define ERR_MACRO_UNKNOWNID         2055
#define ERR_TEXT_ONLY_IN_MACRO      2056

/* param */
#define ERR_PARAM_TOOMANY           2061 /* Too many parameters: XXX parameters expected */
#define ERR_PARAM_NODEFVALUE        2062 /* Omitted parameter has no default value */
#define ERR_PARAM_TOOFEW            2063 /* Too few parameters supplied */
#define ERR_PARAM_TYPEMISMATCH      2064 /* param type mismatch */
#define ERR_PARAM_WRONGNUMBER       2065 /* Wrong number of parameters: either 2061 or 2062 */
#define ERR_PARAM_NOTYPEORVOID      2066 /* non-typed or void expression can not used as parameter */

/* Expected ... */
#define ERR_EXPECTED_CONST          2071 /* Constant expression expected */
#define ERR_EXPECTED_NUMERIC        2072 /* Expected numeric expression */
#define ERR_EXPECTED_BOOLEANEXP     2073 /* Expected boolean expression */
#define ERR_EXPECTED_LEFTCURLY      2074 /* ( expected */
#define ERR_EXPECTED_RIGHTCURLY     2075 /* ) expected */
#define ERR_EXPECTED_COMMA          2076 /* , expected */
#define ERR_EXPECTED_IDENTIFIER     2077 /* identifier expected */
#define ERR_EXPECTED_SEMICOLON      2078 /* ; expected */
#define ERR_INDEX_FILEPOS_UNEXPECTED_SIZE 2079 /* Expected an 8 byte file position */
#define ERR_INDEX_FILEPOS_EXPECTED_LAST 2080 /* Fileposition should be the last field in an index */

/* module */
#define ERR_MODULE_UNKNOWN          2081 /* Import names unknown module XXX */

/* joined */
#define ERR_JOINED_NOSORTED         2091 /* JOINED must specify a sorted dataset */
#define ERR_JOINED_DIFFNOFIELDS     2092 /* JOINED data set has different number of sort fields */
#define ERR_JOINED_DIFFTYPE         2093 /* Component of JOINED has different type to this sort */
#define ERR_JOINED_TOOMANY          2094 /* Too many joined clauses */
#define ERR_DATASET_ILL_JOIN        2095 /* Implicit joins are only supported in HOLE */

/* sort */
#define ERR_SORT_EMPTYLIST          2096 /* The list to be sorted on can not be empty */

#define ERR_ILLSIZE_UNICODE         2097 /* Invalid size for UNICODE or VARUNICODE type */
#define ERR_LOCALES_INCOMPATIBLE    2098 /* Incompatible locales in unicode arguments of binary operation */
#define ERR_BAD_LOCALE              2099 /* Bad locale name */

#define ERR_EXPORT_OR_SHARE         2100 /* Definition must define EXPORTed or SHAREed value for XXX. */
#define ERR_ERR_REFATTR             2101 /* Error in referenced attribute */

/* transform */
#define ERR_VALUEDEFINED            2110 /* A value for XXX has already been specified */
#define ERR_TRANS_NOVALUE4FIELD     2111 /* Transform does not supply a value for field XXX */
#define ERR_REC_DUPFIELD            2112 /* A field called XXX is already defined in this record */
#define ERR_TRANS_RECORDTYPE        2113 /* TRANSFORM required a record return type */
#define ERR_TRANS_ILLASSIGN2SELF    2114 /* Can not assign type XXX to self */
#define ERR_TRANS_NOARGUMENT        2115 /* Transform must have at least 1 parameter */
#define ERR_TRANS_USEDATASET        2116 /* Dataset can not be used in transform */
#define ERR_TRANS_DSINPARAM         2117 /* Can not use dataset directly in transform parameters */

#define ERR_SUBSTR_INVALIDRANGE     2121 /* Invalid substring range */
#define ERR_ZEROLENSTORED           2124 /* STORED() has zero length */

#define ERR_SCOPE_NOTCOMPTUABLE     2130 /* Value for field XXX cannot be computed in this scope */
#define ERR_ASSERT_WRONGSCOPING     2131 /* Incorrect assertion scoping */
#define ERR_ASSERT_BOOLEXPECTED     2132 /* Assertion must be boolean */
#define ERR_SCOPE_USEDATASETINEXPR  2133 /* Use dataset in expression without proper context */
#define ERR_FETCH_NON_DATASET       2134 /* Parameter to fetch isn't a dataset */

#define ERR_MODIFIER_ILLCOMB        2141 /* Illegal combination of modifiers */
#define ERR_EXPORTSHARECONFLICT     2142 /* EXPORT and SHARED cannot be specified together */
#define ERR_ID_REDEFINE             2143 /* Identifier XXX is already defined */
#define ERR_STR_UNKNOWNLEN          2144 /* Expression produces string field of unknown length */
#define ERR_CHARSET_CONFLICT        2145 /* Different character sets in concatenation */
#define ERR_TYPETRANS_LARGERTYPE    2146 /* Type transfer: target type in is larger than source type */
#define ERR_ILLIDENTIFIER           2147 /* Illegal identifier XXX: $ is not allowed */

#define ERR_SIZEOF_WRONGPARAM       2160 /* SIZEOF requires a field or dataset parameter */
#define ERR_SORT_NESTED             2161 /* Nested SORTs may behave incorrectly. Split the attribute into two */
#define ERR_GROUP_NESTED            2162 /* Nested GROUPs may behave incorrectly. Split the attribute into two */
#define ERR_OPERANDS_TOOMANY        2163 /* too many operands */
#define ERR_XML_NOSCOPE             2164 /* No XML scope active */
#define ERR_FILEEXTENTION_ILL       2165 /* File extension must be ".HQL" */
#define ERR_FILENOFOUND             2166 /* File XXX not found */
#define ERR_UNKNOWN_IDENTIFIER      2167 /* Unknown identifier */
#define ERR_GROUP_BADSELECT         2168 /* Select a field other than these grouped upon */
#define ERR_AGG_FIELD_AFTER_VAR     2169 /* Aggregate field following a field that has a variable length */
#define ERR_REC_FIELDNODEFVALUE     2170 /* No default value for a field */
#define ERR_OBJ_NOSUCHFIELD         2171 /* Object has no such a field */

/* lex error */
#define ERR_ESCAPE_UNKNOWN          2180  /* Unknown escape sequence */
#define RRR_ESCAPE_ENDWITHSLASH     2181  /* A string end with escape char \, eg., x := 'abc\'; */
#define ERR_HEXDATA_ILL             2182  /* Illegal hex data, it can only have 0-9a-fA-F */
#define ERR_HEXDATA_ODDDIGITS       2183  /* Odd number of digits in hex data, e.g., x'ABC'.  */
#define ERR_HEXDATA_EMPTY           2184  /* Empty hex data, e.g., x''. */
#define ERR_COMMENT_NOTSTARTED      2185  /* Comment is not started: ending is not allowed */
#define ERR_STRING_NEEDESCAPE       2186  /* Char XXX needs to be escaped */
#define ERR_HASHERROR               2187  /* #ERROR statement */

#define ERR_RECORD_EMPTYDEF         2190  /* Empty record definition: no field defined */
#define ERR_SUBSTR_EMPTYRANGE       2191  /* Empty substring range */
#define ERR_USRTYPE_EMPTYDEF        2192  /* Empty user type definition */
#define ERR_TRANSFORM_EMPTYDEF      2193  /* Empty transform definition */
#define ERR_COMMENT_UNENDED         2194  /* Unended comment */         
#define ERR_STRING_UNENDED          2195  /* Unended string constant: string must end in one line */
#define ERR_STRING_ILLDELIMITER     2196  /* " is illegal string delimiter: use ' instead */
#define ERR_IFBLOCK_EMPTYDEF        2197  /* Empty ifblock definition */

/* hash commands */
#define ERR_TMPLT_EOFINFOR          2200 /* EOF encountered inside #FOR */
#define ERR_TMPLT_EOFINPARAM        2201 /* EOF inside parameter gathering */
#define ERR_TMPLT_SYMBOLREDECLARE   2202 /* Template symbol has already been declared */
#define ERR_TMPLT_SYMBOLNOTDECLARED 2203 /* Template symbol has not been declared */
#define ERR_TMPLT_HASHENDEXPECTED   2204 /* Can not find #end for XXX */
#define ERR_TMPLT_EXTRAELSE         2205 /* #ELSE does not match a #IF */
#define ERR_TMPLT_EXTRAEND          2206 /* #END does not match a # command */
#define ERR_TMPLT_LOADXMLFAILED     2207 /* LoadXML failed */
#define ERR_TMPLT_EXTRABREAK        2208 /* #BREAK is only allowed with a #FOR or #LOOP */
#define ERR_TMPLT_NOBREAKINLOOP     2209 /* #LOOP does not have a #BREAK: an infinite loop will occur */
#define ERR_TMPLT_LOOPEXCESSMAX     2210 /* Loops more than max times */
#define ERR_TMPLT_UNKNOWNCOMMAND    2211 /* Unknown # command */
#define ERR_TMPLT_NONPUREFUNC       2212 /* Can not fold non pure function */
#define ERR_TMPLT_NONEXTERNCFUNC    2213 /* Can not fold non c function */
#define ERR_TMPLT_EXTRAELIF         2214 /* #ELIF does not match a #IF */
#define ERR_TMPLT_NOFOLDFUNC        2215 /* Can not fold function not marked as foldable */

/* security */
#define ERR_SECURITY_BADFORMAT      2216  /* Bad format for access token */
#define WRN_SECURITY_SIGNERROR      2217  /* Errors validating signature */

/* User type */
/* Error 2192, 2011, 2015, 2065, warning 1014  are also possible */
#define ERR_USRTYPE_NOLOAD          2220 /* No load function is defined */
#define ERR_USRTYPE_NOSTORE         2221 /* No store function is defined */
#define ERR_USRTYPE_NOPHYLEN        2222 /* No physical length function is defined */
#define ERR_USRTYPE_BADLOGTYPE      2223 /* Inconsistent logical type */
#define ERR_USRTYPE_BADPHYTYPE      2224 /* Inconsistent physical type */
#define ERR_USRTYPE_NOTDEFASFUNC    2225 /* Store/Load/physicalLength(?) is not defined as func */
#define ERR_USRTYPE_BADPHYLEN       2226 /* physicalLength must have physical type */

/* service */
#define ERR_SVC_FUNCDEFINED         2230 /* Function is already defined */
#define ERR_SVC_ATTRDEFINED         2231 /* Attribute is already defined */
#define ERR_SVC_INVALIDLIBRARY      2232 /* Invalid library entry */
#define ERR_SVC_INVALIDENTRYPOINT   2233 /* Invalid entrypoint: must be valid C identifier. */
#define ERR_SVC_NOSCOPEMODIFIER     2234 /* Function in service can not specify EXPORT or SHARED */
#define ERR_SVC_NOENTRYPOINT        2235 /* Entrypoint is not defined, default to XXX */
#define ERR_SVC_INVALIDINCLUDE      2236 /* Invalid include entry */
#define ERR_SVC_NOYPENEEDED         2237 /* Service does need a type */
#define ERR_SVC_ATTRCONFLICTS       2238 /* Conflicted attributes defined */
#define ERR_SVC_INVALIDINITFUNC     2239 /* Invalid initfunction: must be valid C identifier */

#define ERR_SVC_LOADLIBFAILED       2240 /* Load library failed */
#define ERR_SVC_LOADFUNCFAILED      2241 /* Load procedure in library failed */
#define ERR_SVC_NOLIBRARY           2242 /* library is not defined (it is an error in template) */
#define ERR_SVC_EXCPTIONEXEFUNC     2243 /* Exception occurs when executing service function */
#define ERR_SVC_NOPARAMALLOWED      2244 /* Service can not have any parameter */

/* misc */
#define ERR_LOADXML_NOANATTRIBUTE   2250 /* LOADXML is not an attribute: it can only used alone for test purpose */
#define ERR_ITERATE_INVALIDTRANX    2251 /* Invalid transform for ITERATE */
#define ERR_JOIN_INVALIDTRANX       2252 /* Invalid transform for JOIN */
#define ERR_EXPECTED_DATASET        2255 /* expected a DATASET(...) */
#define ERR_INDEX_COMPONENTS        2256 /* Only one arg supplied in record for INDEX */
#define ERR_NAME_NOT_VALID_ID       2257 /* Parameter to STORED() should be a valid id */

/* dataset parameter */
#define ERR_DSPARM_MAPDUPLICATE     2260 /* A field is mapped more than once */
#define ERR_DSPARM_MISSINGFIELD     2261 /* Dataset has no field as required or mapped */
#define ERR_DSPARM_MAPNOTUSED       2262 /* A map is not used */
#define ERR_DSPARAM_TYPEMISMATCH    2263 /* Mapping fields type mismatch */
#define ERR_DSPARAM_INVALIDOPTCOMB  2264 /* Dataset options DISTRIBUTED, LOCAL, and NOLOCAL cannot be used in combination */

/* pattern/regex */
#define ERR_TOKEN_IN_TOKEN          2280 /* token used inside a token definition */
#define ERR_TOKEN_IN_PATTERN        2281 /* Can't refer to a rule inside a pattern */
#define ERR_BAD_PATTERN             2282 /* Bad syntax inside a PATTERN() definition */
#define ERR_EXPECTED_PATTERN        2283 /* expected  pattern */
#define ERR_PATTEN_SUBPATTERN       2284 /* Invalid format for an assertion subpattern */
#define ERR_EXPR_IN_PATTERN         2285 /* This expression can't be included in a pattern */
#define ERR_PATTERNFUNCREF          2286 /* Can't reference a pattern function in a MATCHED() arg */
#define ERR_SELFOUTSIDERULE         2287 /* Self used outside a rule definition */
#define ERR_USEONLYINRULE           2288 /* Can only use use() inside a rule definition */
#define ERR_INVALIDPATTERNCLAUSE    2289 /* :stored etc. used in a pattern attribute */
#define ERR_EXPECTED_RULE           2290
#define ERR_BADGUARD                2291 /* Could not deduce feature from the guard condition */
#define ERR_EXPECTED_CHARLIST       2292 /* Expected a list of characters */
#define ERR_EXPECTED_REPEAT         2293 /* Expected a repeat... */
#define ERR_PATTERN_TYPE_MATCH      2294
#define ERR_BAD_RULE_QUALIFIER      2295
#define ERR_AMBIGUOUS_PRODUCTION    2296
#define ERR_NOT_ROW_RULE            2297

//                                  2299 

// Any old errors..
#define ERR_NO_MULTI_ARRAY          2300  /* Multi dimension array indexing not supported. */
#define ERR_DISTRIBUTED_MISSING     2301  /* Could not find distributed fields */
#define ERR_MERGE_ONLY_LOCAL        2302  /* Only LOCAL merge is currently supported */
#define ERR_FIELD_NOT_FOUND         2303  /* Field not found */
#define ERR_DEPRECATED              2304  /* Deprecated */
#define ERR_KEYEDINDEXINVALID       2305  /* KEYED index is invalid */
#define ERR_EXPECT_SINGLE_FIELD     2306
#define ERR_RECORD_NOT_MATCH_SET    2307
#define ERR_INVALIDKEYEDJOIN        2308
#define ERR_BAD_JOINFLAG            2309
#define ERR_GROUPING_MISMATCH       2310
#define ERR_NOLONGER_SUPPORTED      2311  /* No longer supported language feature */
#define ERR_INDEX_BADTYPE           2312
#define ERR_ONFAIL_MISMATCH         2313
#define ERR_NOSCOPEMODIFIER         2314
#define ERR_INVALID_XPATH           2315
#define ERR_BAD_FIELD_ATTR          2316
#define ERR_BAD_FIELD_TYPE          2317
#define ERR_ZEROSIZE_RECORD         2318
#define ERR_INVALIDALLJOIN          2319
#define ERR_OUTPUT_TO_INPUT         2320
#define ERR_EXTEND_NOT_VALID        2321
#define ERR_INVALID_CSV_RECORD      2322
#define ERR_INVALID_XML_RECORD      2323
#define ERR_UNKNOWN_TYPE            2324
#define ERR_RESULT_IGNORED          2325
#define ERR_SKIP_NOT_KEYED          2326
#define ERR_NEGATIVE_WIDTH          2327
#define ERR_BAD_FIELD_SIZE          2328
#define ERR_RECURSIVE_DEPENDENCY    2329
#define ERR_NAMED_PARAM_NOT_FOUND   2331
#define ERR_NAMED_ALREADY_HAS_VALUE 2332
#define ERR_NON_NAMED_AFTER_NAMED   2333
#define ERR_EXPECTED_ID             2334
#define ERR_MATCH_KEY_EXACTLY       2335
#define ERR_ROLLUP_NOT_GROUPED      2336
#define ERR_MAX_MISMATCH            2337
#define ERR_BAD_VIRTUAL             2338
#define ERR_PLATFORM_TBD            2339  /* Feature not yet enabled on this platform */
#define ERR_REMOTE_GROUPED          2340
#define ERR_MISMATCH_PROTO          2341
#define ERR_EXPECTED_MODULE         2342
#define ERR_ABSTRACT_MODULE         2343
#define ERR_AMBIGUOUS_DEF           2344
#define ERR_SHOULD_BE_EXPORTED      2345
#define ERR_SAME_TYPE_REQUIRED      2346
#define ERR_CANNOT_REDEFINE         2347
#define ERR_NOT_BASE_MODULE         2348
#define ERR_NOT_INTERFACE           2349
#define ERR_BAD_LIBRARY_SYMBOL      2350
#define ERR_BUILD_FEW               2351
#define ERR_EXPECTED_ATTRIBUTE      2352
#define ERR_CANNOT_DEDUCE_TYPE      2353
#define ERR_NO_FORWARD_VIRTUAL      2354
#define ERR_FORWARD_TOO_LATE        2355
#define ERR_INTERNAL_NOEXPR         2356
#define ERR_USER_FUNC_NO_WORKFLOW   2357
#define ERR_SKIP_IN_NESTEDCHILD     2358
#define ERR_DEDUP_ALL_KEEP          2359
#define ERR_EXPECTED_LIBRARY        2360
#define ERR_EXPECTED_LIBRARY_NAME   2361
#define ERR_PROTOTYPE_MISMATCH      2362
#define ERR_NOT_IMPLEMENTED         2363
#define ERR_DEPRECATED_ATTR         2364
#define ERR_EXPECTED_STORED_VARIABLE 2365
#define ERR_PLUSEQ_NOT_SUPPORTED    2366
#define ERR_COGROUP_NO_GROUP        2367
#define ERR_ASSIGN_MEMBER_DATASET   2368
#define ERR_DATASET_NOT_CONTAIN_X   2369
#define ERR_DATASET_NOT_CONTAIN_SAME_X  2370
#define ERR_OBJ_NOACTIVEDATASET     2371
#define WRN_RECORDCANNOTDERIVE      2372
#define ERR_EXPECTED_FIELD          2373
#define HQLERR_TERM_NOT_RELEASED    2374
#define ERR_NO_ATTRIBUTE_TEXT       2375
#define ERR_EXPECTED_LIST           2376
#define ERR_EXPECTED_SCALAR         2377
#define ERR_UNEXPECTED_ATTRX        2378
#define ERR_EXPECTED_ROW            2379
#define ERR_UNEXPECTED_PUBLIC_ID    2380
#define ERR_NO_GLOBAL_MODULE        2381
#define ERR_NO_IFBLOCKS             2382
#define ERR_WORKFLOW_ILLEGAL        2383
#define ERR_BAD_IMPORT              2384
#define HQLERR_CannotSubmitFunction 2385
#define HQLERR_CannotSubmitModule   2386
#define ERR_COUNTER_NOT_COUNT       2387
#define HQLERR_CannotSubmitMacroX   2388
#define HQLERR_CannotBeGrouped      2389
#define HQLERR_CannotAccessShared   2390
#define ERR_PluginNoScripting       2391
#define ERR_ZERO_SIZE_VIRTUAL       2392
#define ERR_BAD_JOINGROUP_FIELD     2393
#define ERR_CANNOT_ACCESS_CONTAINER 2394
#define ERR_RESULT_IGNORED_SCOPE    2395
#define ERR_INDEX_DEPEND_DATASET    2396
#define ERR_DUBIOUS_NAME            2397
#define ERR_DUPLICATE_FILENAME      2398
#define ERR_DUPLICATE_SOURCE        2399

#define ERR_CPP_COMPILE_ERROR       2999

#define ERR_ASSERTION_FAILS         100000

/* general error types */
#define ERR_INTERNALEXCEPTION       3000  /* Internal exception */
#define ERR_ERROR_TOOMANY           3001  /* Too many errors */
#define ERR_EXPECTED                3002  /* Expected ... */
#define ERR_ILL_HERE                3003  /* XXX is not valid here */
#define ERR_EOF_UNEXPECTED          3004  /* Unexpected end of file encountered */
#define ERR_NO_WHOLE_RECORD         3005  /* Whole record not valid here */
#define ERR_EXCEPT_NOT_FOUND        3006  /* Except not found in the incoming list */
#define ERR_ROWPIPE_AND_PROJECT     3007  /* PIPE(name) and project not supported */

// Errors with text
#define HQLERR_DedupFieldNotFound               3100
#define HQLERR_CycleWithModuleDefinition        3101
#define HQLERR_BadProjectOfStepping             3102
#define HQLERR_DatasetNotExpected               3103
#define HQLERR_AtmostSubstringNotMatch          3104
#define HQLERR_AtmostSubstringSingleInstance    3105
#define HQLERR_SerializeExtractTooComplex       3106
#define HQLERR_FieldInMapNotDataset             3107
#define HQLERR_FieldAlreadyMapped               3108
#define HQLERR_FileNotInDataset                 3109
#define HQLERR_FileXNotFound                    3110
#define HQLERR_NoBrowseGroupChild               3111
#define HQLERR_SelectedFieldXNotInDataset       3112
#define HQLERR_IllegalRegexPattern              3113
#define HQLERR_UnknownCharClassX                3114
#define HQLERR_CollectionNotYetSupported        3115
#define HQLERR_EquivalenceNotYetSupported       3116
#define HQLERR_CouldNotConnectEclServer         3117
#define HQLERR_VersionMismatch                  3118
#define HQLERR_VirtualFieldInTempTable          3119
#define HQLERR_IncompatiableInitailiser         3120
#define HQLERR_NoDefaultProvided                3121
#define HQLERR_TooManyInitializers              3122
#define HQLERR_IncompatibleTypesForField        3123
#define HQLWRN_CouldNotConstantFoldIf           3124
#define HQLERR_UnexpectedOperator               3125
#define HQLERR_UnexpectedType                   3126
#define HQLERR_PayloadMismatch                  3127
#define HQLERR_MemberXContainsVirtualRef        3128
#define HQLERR_FieldHasNoDefaultValue           3129
#define HQLERR_AtmostFailMatchCondition         3130
#define HQLERR_AtmostCannotImplementOpt         3131
#define HQLERR_PrefixJoinRequiresEquality       3132
#define HQLERR_AtmostFollowUnknownSubstr        3133
#define HQLERR_AtmostLegacyMismatch             3134
#define HQLERR_PropertyArgumentNotConstant      3135
#define HQLERR_InvalidErrorCategory             3136
#define HQLERR_MultipleHashWebserviceCalls      3137

#define HQLERR_DedupFieldNotFound_Text          "Field removed from dedup could not be found"
#define HQLERR_CycleWithModuleDefinition_Text   "Module definition contains an illegal cycle/recursive definition %s"
#define HQLERR_BadProjectOfStepping_Text        "Cannot calculate inversion of PROJECT on STEPPED index%s"
#define HQLERR_DatasetNotExpected_Text          "Dataset not expected in this context"
#define HQLERR_AtmostSubstringNotMatch_Text     "Equality on field[n..*] must match a similar equality"
#define HQLERR_AtmostSubstringSingleInstance_Text "Join only supports a single x[n..*] comparison"
#define HQLERR_SerializeExtractTooComplex_Text  "INTERNAL: Serializing extract too complex"
#define HQLERR_UnexpectedOperator_Text          "INTERNAL: Unexpected '%s' at %s(%d)"
#define HQLERR_UnexpectedType_Text              "INTERNAL: Unexpected type '%s' at %s(%d)"
#define HQLERR_FieldInMapNotDataset_Text        "Field '%s' in the map was not found in the dataset parameter"
#define HQLERR_FieldAlreadyMapped_Text          "Field '%s' has already been mapped"
#define HQLERR_FileNotInDataset_Text            "A field named '%s' was not found in the dataset parameter"
#define HQLERR_FileXNotFound_Text               "File %s not found"
#define HQLERR_NoBrowseGroupChild_Text          "Browsing child grouped tables not handled at the moment"
#define HQLERR_SelectedFieldXNotInDataset_Text  "INTERNAL: Selected field '%s' does not appear in the dataset"
#define HQLERR_IllegalRegexPattern_Text         "Illegal pattern"
#define HQLERR_UnknownCharClassX_Text           "Unknown character class [:%s:]"
#define HQLERR_CollectionNotYetSupported_Text   "Collation symbols not yet supported"
#define HQLERR_EquivalenceNotYetSupported_Text  "Equivalence class symbols not yet supported"
#define HQLERR_CouldNotConnectEclServer_Text    "Could not connect to any ECL server"
#define HQLERR_VersionMismatch_Text             "Mismatch in major version number (%s v %s)"
#define HQLERR_VirtualFieldInTempTable_Text     "Virtual field %s not supported in constant table - please provide a value"
#define HQLERR_IncompatiableInitailiser_Text    "Inline DATASET field '%s' cannot be initialized with a list of values"
#define HQLERR_NoDefaultProvided_Text           "No value or default provided for field %s in inline table"
#define HQLERR_TooManyInitializers_Text         "Too many initializers (value %s) for inline dataset definition"
#define HQLERR_IncompatibleTypesForField_Text   "Initializer for field %s has the wrong type"
#define HQLWRN_CouldNotConstantFoldIf_Text      "Could not constant fold the condition on an IFBLOCK for an inline table"
#define HQLERR_PayloadMismatch_Text             "Mismatched => in inline dictionary definition"
#define HQLERR_MemberXContainsVirtualRef_Text   "Member %s contains virtual references but not supported as virtual"
#define HQLERR_FieldHasNoDefaultValue_Text      "Field '%s' doesn't have a defined value"
#define HQLERR_AtmostFailMatchCondition_Text    "ATMOST(%s) failed to match part of the join condition"
#define HQLERR_AtmostCannotImplementOpt_Text    "ATMOST() optional condition is too complex"
#define HQLERR_PrefixJoinRequiresEquality_Text  "Global JOIN with no required equalities requires ALL"
#define HQLERR_AtmostFollowUnknownSubstr_Text   "ATMOST [1..*] on an unknown length string must be last in the optional list"
#define HQLERR_AtmostLegacyMismatch_Text        "Legacy JOIN condition on field[1..*] should be included in the optional fields"
#define HQLERR_PropertyArgumentNotConstant_Text "The argument to attribute '%s' must be a constant"
#define HQLERR_InvalidErrorCategory_Text        "Unrecognised ONWARNING category '%s'"
#define HQLERR_MultipleHashWebserviceCalls_Text "#webservice can only be called once"

/* parser error */
#define ERR_PARSER_CANNOTRECOVER    3005  /* The parser can not recover from previous error(s) */

/////////////////////////////////////////////////////////////////////////////
/* Code Generation errors - defined in hqlcerrors.hpp */
#define ERR_CODEGEN_FIRST           4000
#define ERR_CODEGEN_LAST            4999

#define ECODETEXT(x)                (x), (x##_Text)

#define WARNING(cat, x)                  reportWarning(cat, x, x##_Text)
#define WARNING1(cat, x, a)              reportWarning(cat, x, x##_Text, a)
#define WARNING2(cat, x, a, b)           reportWarning(cat, x, x##_Text, a, b)
#define WARNING3(cat, x, a, b, c)        reportWarning(cat, x, x##_Text, a, b, c)

#define ERRORAT(e, x)               reportError(e, x, x##_Text)
#define ERRORAT1(e, x, a)           reportError(e, x, x##_Text, a)
#define ERRORAT2(e, x, a, b)        reportError(e, x, x##_Text, a, b)
#define ERRORAT3(e, x, a, b, c)     reportError(e, x, x##_Text, a, b, c)

#define throwUnexpectedOp(op)       throw MakeStringException(ECODETEXT(HQLERR_UnexpectedOperator), getOpString(op), __FILE__, __LINE__)
#define throwUnexpectedType(type)   throw MakeStringException(ECODETEXT(HQLERR_UnexpectedType), type->queryTypeName(), __FILE__, __LINE__)

#endif // _HQLERRORS_HPP_
