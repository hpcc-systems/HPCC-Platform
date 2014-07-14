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

#ifndef HQLEXPR_INCL
#define HQLEXPR_INCL

#ifdef _DEBUG
   #define ENABLE_ENUM_TYPE
#endif

#ifndef _DEBUG
//  #define USE_TBB
#endif

#define USE_SELSEQ_UID
//It is impossible to ensure that LEFT/RIGHT are unique, and cannot be nested.  For instance
//x := PROJECT(ds, t(LEFT));
//y := x(field not in SET(x, field2));
//Once filters are moved in child queries then it can occur even when the common project cannot be hoisted.

//Currently nearly all functional attributes are expanded when the call is parsed.
//This is potentially inefficient, and makes it hard to dynamically control whether functions are expanded in line or generated out of line.
//Expansion in place probably occupies 50% of the parse time processing, but parsing is typically 20% of over-all time - significant but not disastrous.
//There may be situations (syntax check, gather dependencies) where the savings are significant enough to allow it to work for parse - even if it doesn't work for generation.
//
//The following constant changes that functionality so expansion is delayed until before code generation.
//It nearly works - but there are still some examples which have problems - primarily libraries, old parameter syntax, enums and other issues.
//There may also problems with queryRecord() which needs to really be replaced with recordof(x), especially if "templates" are delayed expanded.
//To work properly it may require many of the transformations in hqlgram2.cpp to be moved to after the expansion.  (E.g., BUILD)
//#define DELAY_CALL_EXPANSION

#ifdef DELAY_CALL_EXPANSION
#define DEFAULT_EXPAND_CALL false
#else
#define DEFAULT_EXPAND_CALL true
#endif


#define GATHER_HIDDEN_SELECTORS

#define XPATH_CONTENTS_TEXT     "<>"

//This should be defined, but HOLe does not cope with variable length stored strings.  Need to wait until HOLe is integrated into thor.
//#define STORED_CAN_CHANGE_LENGTH

#include "hql.hpp"
#include "jhash.hpp"
#include "jprop.hpp"
#include "jptree.hpp"
#include "defvalue.hpp"
#include "hqlerror.hpp"

interface IXmlScope;
interface IHqlScope;
interface IHqlSimpleScope;
interface IHqlDataset;
interface IHqlExpression;

extern HQL_API ISourcePath * createSourcePath(const char * name);
extern HQL_API ISourcePath * createSourcePath(unsigned len, const char * name);

class HqlGramCtx;       // opaque, but still not very nice
class HQL_API HqlExprArray : public IArrayOf<IHqlExpression>
{
public:
    bool containsBody(IHqlExpression & expr);
};


class HQL_API UniqueSequenceCounter
{
public:
    inline UniqueSequenceCounter() { value = 0; }

    inline unique_id_t next() 
    { 
        SpinBlock block(lock);
        return ++value; 
    }

protected:
    unique_id_t value;
    SpinLock lock;
};


typedef ICopyArrayOf<IHqlExpression> HqlExprCopyArray;
typedef class MapXToMyClass<IAtom *, IAtom *, IAtom *> KeywordMap;
typedef ICopyArrayOf<IAtom> AtomArray;

const unsigned UnadornedParameterIndex = (unsigned)-1;

#define CHOOSEN_ALL_LIMIT          I64C(0x7FFFFFFFFFFFFFFF)

// Allow of the following are private, and should only be accessed by the associated helper functions.
//NB: If this is changed, remember to look at whether no_select an noinherit need modifying.
enum
{
// House keeping information on a per node basis
  HEF____unused0____          = 0x00000001,
    HEFobserved                 = 0x00000002,
    HEFgatheredNew              = 0x00000004,
    HEFhasunadorned             = 0x00000008,

    HEFhousekeeping             = (HEFhasunadorned|HEFobserved|HEFgatheredNew),

// generally applicable start from the top down
    HEFunbound                  = 0x00000010,
    HEFinternalSelect          = 0x00000020,
    HEFcontainsDatasetAliasLocally= 0x00000040,
  HEF____unused2____          = 0x00000080,
  HEF____unused3____          = 0x00000100,
    HEFfunctionOfGroupAggregate = 0x00000200,
    HEFvolatile                 = 0x00000400,           // value changes each time called - e.g., random()
    HEFaction                   = 0x00000800,           // an action, or something that can have a side-effect
    HEFaccessRuntimeContext     = 0x00000100,
    HEFthrowscalar              = 0x00002000,           // scalar/action that can throw an exception
    HEFthrowds                  = 0x00004000,           // dataset that can throw an exception
    HEFoldthrows                = 0x00008000,           // old throws flag, which I should remove asap

// code generator specific start from the bottom up.
//NB: update the select 
    HEFcontainsThisNode         = 0x00010000,
    HEFonFailDependent          = 0x00020000,
    HEFcontainsActiveDataset    = 0x00040000,
    HEFcontainsActiveNonSelector= 0x00080000,
    HEFcontainsDataset          = 0x00100000,

    HEFtranslated               = 0x00200000,
    HEFgraphDependent           = 0x00400000,
    HEFcontainsNlpText          = 0x00800000,
    HEFcontainsXmlText          = 0x01000000,
    HEFtransformDependent       = 0x02000000,               // depends in some way on the containing transform
                                                            // self, fail and counter.  Latter should be treated like a dataset I suspect., could have separate flags for others.
    HEFcontainsSkip             = 0x04000000,
    HEFcontainsCounter          = 0x08000000,
    HEFassertkeyed              = 0x10000000,
    HEFcontextDependentException= 0x20000000,               // A context dependent item that doesn't fit into any other category - for use as a last resort!
    HEFcontainsAlias            = 0x40000000,
    HEFcontainsAliasLocally     = 0x80000000,

//Combinations used for processing elsewhere:
//Don't ever inherit the following...
    HEFalwaysInherit            = HEFunbound|HEFinternalSelect,
    HEFassigninheritFlags       = ~(HEFhousekeeping|HEFalwaysInherit),          // An assign inherits all but this list from the rhs value 

//  HEFcontextDependent         = (HEFgraphDependent|HEFcontainsNlpText|HEFcontainsXmlText|HEFcontainsSkip|HEFcontainsCounter|HEFtransformDependent|HEFtranslated|HEFonFailDependent|HEFcontextDependentException|HEFthrowscalar|HEFthrowds),
    HEFcontextDependent         = (HEFgraphDependent|HEFcontainsNlpText|HEFcontainsXmlText|HEFcontainsSkip|HEFcontainsCounter|HEFtransformDependent|HEFtranslated|HEFonFailDependent|HEFcontextDependentException|HEFoldthrows),
    HEFretainedByActiveSelect   = (HEFhousekeeping|HEFalwaysInherit),

    HEFintersectionFlags        = (0),
    HEFunionFlags               = (HEFunbound|HEFfunctionOfGroupAggregate|HEFvolatile|HEFaction|HEFthrowscalar|HEFthrowds|HEFoldthrows|
                                   HEFonFailDependent|HEFcontainsActiveDataset|HEFcontainsActiveNonSelector|HEFcontainsDataset|
                                   HEFtranslated|HEFgraphDependent|HEFcontainsNlpText|HEFcontainsXmlText|HEFtransformDependent|
                                   HEFcontainsSkip|HEFcontainsCounter|HEFassertkeyed|HEFcontextDependentException|HEFcontainsAlias|HEFcontainsAliasLocally|
                                   HEFinternalSelect|HEFcontainsThisNode|HEFcontainsDatasetAliasLocally|HEFaccessRuntimeContext),

    HEFcontextDependentNoThrow  = (HEFcontextDependent & ~(HEFthrowscalar|HEFthrowds|HEFoldthrows)),
    HEFcontextDependentDataset  = (HEFcontextDependent & ~(HEFthrowscalar)),
    HEFimpure                   = (HEFvolatile|HEFaction|HEFthrowds|HEFthrowscalar|HEFcontainsSkip),
};

//NB: increase the member variable if it grows 
enum
{
    HEF2workflow                = 0x00000001,
    HEF2mustHoist               = 0x00000002,
    HEF2assertstepped           = 0x00000004,
    HEF2containsNonGlobalAlias  = 0x00000008,
    HEF2containsSelf            = 0x00000010,
    HEF2containsNewDataset      = 0x00000020,
    HEF2constant                = 0x00000040,
    HEF2____unused1____         = 0x00000080,
    HEF2globalAction            = 0x00000100,               // Can only be evaluated globally
    HEF2containsCall            = 0x00000200,
    HEF2containsDelayedCall     = 0x00000400,

    //NB: infoFlags2 is currently 2 bytes
    HEF2alwaysInherit           = (HEF2workflow|HEF2containsCall|HEF2containsDelayedCall),
    HEF2intersectionFlags       = (HEF2constant),
    HEF2unionFlags              = (HEF2alwaysInherit)|(HEF2mustHoist|HEF2assertstepped|HEF2containsNonGlobalAlias|HEF2containsNewDataset|HEF2globalAction|HEF2containsSelf),
    HEF2assigninheritFlags      = ~(HEF2alwaysInherit),         // An assign inherits all but this list from the rhs value 
};

//Removed spaces so it is easier to translate from a number to a node when debugging.
enum _node_operator {
        no_none,
        no_scope,
        no_list,
        no_mul,
        no_div,
        no_modulus,  
        no_negate,   
        no_add,      
        no_sub,      
        no_eq,       
        no_ne,       
        no_lt,       
        no_le,       
        no_gt,       
        no_ge,       
        no_not, 
        no_notnot,
        no_and, 
        no_or, 
        no_xor,     
        no_concat,
        no_notin,
        no_in,
        no_notbetween,
        no_between,
        no_comma,
        no_count,
        no_countgroup,
        no_selectmap,
        no_exists,
        no_within,
        no_notwithin,
        no_param,
        no_constant,
        no_field,
        no_map,
        no_if,
        no_max,
        no_min,
        no_sum,
        no_ave,
        no_maxgroup,
        no_mingroup,
        no_sumgroup,
        no_avegroup,
        no_exp,
        no_power,
        no_round,
        no_roundup,
        no_range,
        no_rangeto,
        no_rangefrom,
        no_substring,
        no_transform,
        no_rollup,
        no_iterate,
        no_hqlproject,
        no_assign,
        no_assignall,
        no_asstring,
        no_group,  
        no_cogroup,
        no_cosort,
        no_truncate,
        no_ln,
        no_log10,
        no_sin,
        no_cos,
        no_tan,
        no_asin,
        no_acos,
        no_atan,
        no_atan2,
        no_sinh,
        no_cosh,
        no_tanh,
        no_sqrt,
        no_evaluate,
        no_choose,
        no_which,
        no_rejected,
        no_mapto,
        no_record,
        no_service,
        no_index,
        no_all,
        no_left,
        no_right,
        no_outofline,
        no_cast,
        no_implicitcast, 
        no_once,
        no_csv,
        no_sql,
        no_thor,
        no_flat,
        no_pipe,
        no_mix,
        no_selectnth,
        no_stored,
        no_failure,   
        no_success,
        no_recovery,
        no_external,
        no_funcdef,
        no_externalcall,
        no_wait,
        no_event,
        no_persist,
        no_buildindex,
        no_output,
        no_omitted,   
        no_when,
        no_setconditioncode,
        no_priority,
        no_intformat,
        no_realformat,
        no_abs,
        no_nofold,
        no_table,
        no_keyindex,
        no_temptable,
        no_usertable,
        no_choosen,
        no_filter, 
        no_fetch,
        no_join,
        no_joined,
        no_sort,
        no_sorted,
        no_sortlist,
        no_dedup,
        no_enth,
        no_sample,
        no_selectfields,
        no_persist_check,
        no_create_initializer,
        no_owned_ds,
        no_complex,
        no_assign_addfiles,
        no_debug_option_value,
        no_hash,
        no_hash32,
        no_hash64,
        no_crc,
        no_return_stmt,
        no_update,    
        no_subsort,
        no_chooseds,
        no_alias,
        no_datasetfromdictionary,
        no_delayedscope,
        no_assertconcrete,
        no_unboundselect,
        no_id,
        no_orderedactionlist,
        no_dataset_from_transform,
        no_childquery,
        no_unknown,
        no_createdictionary,
        no_indict,
        no_countdict,
        no_any,
        no_existsdict,
    no_unused101,
    no_unused25,
    no_unused28,  
    no_unused29,
    no_unused30,
    no_unused31,
    no_unused32,
    no_unused33,
    no_unused34,
    no_unused35,
    no_unused36,
    no_unused37,
    no_unused38,
        no_is_null,
        no_dataset_alias,
    no_unused40,
    no_unused41,
    no_unused52,
        no_trim,
        no_position,
        no_charlen,
    no_unused42,
    no_unused43,
    no_unused44,
    no_unused45,
    no_unused46,
    no_unused47,
    no_unused48,
    no_unused49,
    no_unused50,
        no_nullptr,
        no_sizeof,
        no_offsetof, 
        no_current_date,
        no_current_time,
        no_current_timestamp,
        no_variable,
        no_libraryselect,
        no_case,
        no_band,
        no_bor,
        no_bxor,
        no_bnot, 
        no_postinc,
        no_postdec,
        no_preinc,
        no_predec,
        no_pselect,
        no_address,
        no_deref,
        no_order,
        no_hint,
        no_attr, 
        no_self,
        no_rank,
        no_ranked,
        no_mergedscope,
        no_ordered,                 // internal, used in processing of rank/ranked
        no_typetransfer,
        no_decimalstack,
        no_type,
        no_apply,
        no_ifblock,
        no_translated,              // contents below this have already been translated
        no_addfiles,
        no_distribute,
        no_macro,
        no_cloned,
        no_cachealias,
        no_lshift, 
        no_rshift,
        no_colon,
        no_setworkflow_cond,
        no_unused102,
        no_unused15,
        no_random,
        no_select,
        no_normalize,
        no_counter,
        no_distributed,
        no_grouped, 
        no_denormalize,
        no_transformebcdic,
        no_transformascii,
        no_childdataset,
        no_envsymbol,
        no_null,
        no_quoted,                      // similar to no_variable, but not a simple variable
        no_bound_func,                  // used in caching
        no_bound_type,
        no_metaactivity,
        no_fail,
        no_filepos,
        no_aggregate,
        no_distribution,
        no_newusertable,                // replacements for no_usertable that have a separate newtransform.
        no_newaggregate,                // otherwise it becomes impossible to transform the trees.
        no_newtransform,                // same as transform, but no LEFT. in assignments.  Allows easy reference to hole child tables.
        no_fromunicode,
        no_tounicode,
        no_keyunicode,
        no_loadxml,                     // used for error report
        no_isomitted,
        no_fieldmap,                    // dataset with a map
        no_template_context,            // dataset func 
        no_ensureresult,                // (value, seq, [name])
        no_getresult,                   // (seq, [name])
        no_setresult,                   // (value, seq, [name])
        no_is_valid,
        no_alias_project,               // add aliases to projected values
        no_alias_scope,                 // define aliases for context of child expressions.
        no_global,                      // force expression to be evaluated at global scope
        no_eventname,
        no_sequential,
        no_parallel,
        no_writespill,
        no_readspill,
        no_nolink,
        no_workflow,
        no_workflow_action,
        no_commonspill,
        no_choosesets,
        no_regex_find,
        no_regex_replace,
        no_workunit_dataset,
        no_failcode,
        no_failmessage,
        no_independent,
        no_keyed,
        no_compound,
        no_checkpoint,
        no_split,                       // only used in graph resourcing.
        no_spill,                       // passthrough activity that also spills to disk.
        no_subgraph,
        no_dependenton,
        no_setmeta,
        no_throughaggregate,
        no_joincount,
        no_merge_nomatch,
        no_countcompare,
        no_limit,
        no_evaluate_stmt,
        no_notify,
        no_parse,
        no_newparse,                    // parse where record is expanded to (record,newtransform)
        no_skip,
        no_matched,
        no_matchtext,
        no_matchlength,
        no_matchposition,
        no_pat_select,                  // parameter to MATCHED
        no_pat_const,                   // literal string
        no_pat_pattern,                 // pattern
        no_pat_follow,                  // following tokens
        no_pat_first,                   // first in buffer
        no_pat_last,                    // last in buffer
        no_pat_repeat,                  // x{n[,m]}, no second parameter means unlimited.
        no_pat_instance,                // instance of a named symbol
        no_pat_anychar,                 // .
        no_pat_token,                   // make a token from a pattern
        no_pat_imptoken,                // An implicit token (to help regeneration)
        no_pat_set,                     // created from [a-z0-9] etc.
        no_pat_checkin,                 // Does the pattern exist in the arguments?
        no_pat_x_before_y,              // look ahead assertion
        no_pat_x_after_y,               // look behind assertion
        no_xml,
        no_compound_fetch,
        no_pat_index,
        no_pat_beginpattern,            // marks the start of a global/child pattern.
        no_pat_endpattern,              // marks the end of a global/child pattern.
        no_pat_checklength,             // has symbol x got length y?
        no_topn,
        no_outputscalar,
        no_matchunicode,
        no_pat_validate,
   no_unused83,
        no_existsgroup,
        no_pat_use,
        no_unused13,
        no_penalty,
        no_rowdiff,
        no_wuid,
        no_featuretype,
        no_pat_guard,
        no_xmltext,
        no_xmlunicode,
        no_newxmlparse,
        no_xmlparse,
        no_xmldecode,
        no_xmlencode,
        no_pat_featureparam,
        no_pat_featureactual,
        no_pat_featuredef,
        no_evalonce,
        no_unused14,
        no_merge,
        no_keyeddistribute,
        no_distributer,
        no_impure,
        no_attr_link,                   // a link to another expression - parameters should be mapped by transformations.
        no_attr_expr,                   // an attribute with parameters that should be transformed (not just mapped)
        no_addsets,
        no_rowvalue,
        no_newkeyindex,
        no_pat_case,
        no_pat_nocase,
        no_activetable,                 // used in the type information to represent the active table
        no_preload,
        no_createset,
        no_assertkeyed,
        no_assertwild,
        no_recordlist,
        no_hashmd5,
        no_soapcall,
        no_soapcall_ds,                 // as above, but has a dataset parameter
        no_newsoapcall,
        no_newsoapcall_ds,
        no_soapaction_ds,
        no_newsoapaction_ds,
        no_temprow,
        no_activerow,                   // no_activerow(dataset) - used to disambiguate rows that are in scope.
        no_catch,
    no_unused80,
        no_reference,
        no_callback,                    // only used by code generator to backpatch the source
        no_keyedlimit,
        no_keydiff,
        no_keypatch,
        no_returnresult,
        no_id2blob,
        no_blob2id,
        no_anon,
        no_projectrow,
        no_embedbody,
        no_sortpartition,
        no_define,
        no_globalscope,
        no_forcelocal,
        no_typedef,
        no_matchattr,
        no_pat_production,
        no_guard,
        no_datasetfromrow,
        no_createrow,
        no_selfref,                     // reference to self inside record - not lhs of a transform.  Because inside a record it can't have a type, since type isn't know at that point.
        no_unicodeorder,
        no_assertconstant,
        no_compound_selectnew,
        no_nothor,
        no_newrow,                      // only used when changing selector on an expression
        no_clustersize,
        no_call,
        no_compound_diskread,           //NB: All these compound activities occur in the same order for each source.
        no_compound_disknormalize,
        no_compound_diskaggregate,
        no_compound_diskcount,
        no_compound_diskgroupaggregate,
        no_compound_indexread,
        no_compound_indexnormalize,
        no_compound_indexaggregate,
        no_compound_indexcount,
        no_compound_indexgroupaggregate,
        no_compound_childread,
        no_compound_childnormalize,
        no_compound_childaggregate,
        no_compound_childcount,
        no_compound_childgroupaggregate,
        no_compound_inline,
        no_getgraphresult,
        no_setgraphresult,
        no_assert,
        no_assert_ds,
        no_namedactual,
        no_combine,
        no_rows,
        no_rollupgroup,
        no_regroup,
        no_combinegroup,
        no_inlinetable,
        no_transformlist,
        no_variance,
        no_covariance,
        no_correlation,
        no_vargroup,
        no_covargroup,
        no_corrgroup,
        no_denormalizegroup,
        no_xmlproject,
        no_spillgraphresult,
        no_enum,
        no_pat_or,
        no_loop,
        no_loopbody,
        no_cluster,
        no_forcenolocal,
        no_allnodes,
        no_unused6,
        no_matchrow,
        no_sequence,
        no_selfjoin,
        no_remotescope,
        no_privatescope,
        no_virtualscope,
        no_concretescope,
        no_purevirtual,
        no_internalselect,
        no_delayedselect,
        no_pure,
        no_libraryscope,
        no_libraryscopeinstance,
        no_libraryinput,
        no_pseudods,
        no_process,
        no_matchutf8,
        no_thisnode,
        no_graphloop,
        no_rowset,
        no_loopcounter,
        no_getgraphloopresult,
        no_setgraphloopresult,
        no_rowsetindex,
        no_rowsetrange,
        no_assertstepped,
        no_assertsorted,
        no_assertgrouped,
        no_assertdistributed,
        no_mergejoin,
        no_datasetlist,
        no_nwayjoin,
        no_nwaymerge,
        no_stepped,
        no_existslist,
        no_countlist,
        no_maxlist,
        no_minlist,
        no_sumlist,
        no_getgraphloopresultset,
        no_forwardscope,
        no_pat_before_y,                    // look ahead assertion
        no_pat_after_y,                     // look behind assertion
        no_extractresult,
        no_attrname,                        // used to save names, but not an "attribute" so names don't clash
        no_nonempty,
        no_processing,                      // currently expanding this as a definition.
        no_filtergroup,
        no_rangecommon,
        no_section,
        no_nobody,
        no_deserialize,
        no_serialize,
        no_eclcrc,
        no_top,
        no_uncommoned_comma,
        no_nameof,
        no_catchds,
        no_file_logicalname,
        no_toxml,
        no_sectioninput,
        no_forcegraph,
        no_eventextra,
    no_unused81,
        no_related,
        no_executewhen,
        no_definesideeffect,
        no_callsideeffect,
        no_fromxml,
        no_actionlist,                          // Similar to no_sequential, but does not require independence
        no_preservemeta,
        no_normalizegroup,
        no_indirect,
        no_selectindirect,
        no_nohoist,                             //purely for debugging/test generation
        no_merge_pending,
        no_httpcall,
        no_getenv,
        no_last_op,

//These never get created as IHqlExpressions....
        no_pat_compound,
        no_pat_begintoken,
        no_pat_endtoken,
        no_pat_begincheck,
        no_pat_endcheckin,
        no_pat_endchecklength,
        no_pat_beginseparator,
        no_pat_endseparator,
        no_pat_separator,
        no_pat_beginvalidate,
        no_pat_endvalidate,
        no_pat_dfa,
        no_pat_singlechar,
        no_pat_beginrecursive,
        no_pat_endrecursive,
        no_pat_utf8single,
        no_pat_utf8lead,
        no_pat_utf8follow,

        no_last_pseudoop
         };

enum
{
    compound_readdelta = 0,
    compound_normalizedelta,
    compound_aggregatedelta,
    compound_countdelta,
    compound_groupaggregatedelta,
    compound_maxdelta
};

enum annotate_kind
{
    annotate_none,
    annotate_symbol,
    annotate_meta,
    annotate_warning,
    annotate_javadoc,
    annotate_location,
    annotate_parsemeta,
    annotate_max
};

//An enumeration of all the possible derived properties implemented by functions in hqlattr.hpp
enum ExprPropKind
{
    EPnone,
    EPrecordCount,
    EPdiskserializedForm,
    EPinternalserializedForm,
    EPsize,
    EPaligned,
    EPunadorned,
    EPlocationIndependent,
    EPmeta,
    EPmax
};


#ifdef ENABLE_ENUM_TYPE
    typedef enum _node_operator node_operator;
#else
    typedef unsigned short node_operator;
#endif

interface IHqlSimpleScope : public IInterface
{
    virtual IHqlExpression *lookupSymbol(IIdAtom * name) = 0;
};

interface IHqlAlienTypeInfo : public IInterface
{
    virtual ITypeInfo * getLogicalType() = 0;
    virtual ITypeInfo * getPhysicalType() = 0;
    virtual ITypeInfo * queryLogicalType() = 0;
    virtual ITypeInfo * queryPhysicalType() = 0;
    virtual int getLogicalTypeSize() = 0;
    virtual int getPhysicalTypeSize() = 0;
    virtual unsigned getMaxSize() = 0;
    virtual IHqlExpression * queryLoadFunction() = 0;
    virtual IHqlExpression * queryLengthFunction() = 0;
    virtual IHqlExpression * queryStoreFunction() = 0;
    virtual IHqlExpression * queryFunction(IIdAtom * name) = 0;
};

interface IFileContents : public IInterface
{
public:
    virtual IFile * queryFile() = 0;
    virtual ISourcePath * querySourcePath() = 0;
    virtual const char * getText() = 0;
    virtual size32_t length() = 0;
};

//This class ensures that the pointer to the owner is cleared before both links are released, which allows
//you to safely have an unlinked owner pointer in a child while this class instance exists.
template <class OWNER, class CHILD>
class SafeOwnerReference : public CInterface
{
public:
    inline SafeOwnerReference(OWNER * _owner, CHILD * _child) : owner(_owner), child(_child) {}
    inline ~SafeOwnerReference() { child->clearOwner(); }
protected:
    Linked<OWNER> owner;
    Linked<CHILD> child;
};

interface IHasUnlinkedOwnerReference : public IInterface
{
    virtual void clearOwner() = 0;
};

typedef SafeOwnerReference<IHqlScope, IHasUnlinkedOwnerReference> ForwardScopeItem;

class HQL_API HqlParseContext
{
public:
    class MetaOptions
    {
    public:
        MetaOptions() { _clear(*this); }

        bool includePublicDefinitions;   // include details of SHARED and EXPORTED symbols
        bool includePrivateDefinitions;  // include details of local symbols/parameters etc.
        bool onlyGatherRoot;             // only gather information for the main definition processed.
        bool includeInternalUses;        // gather use of internal symbols
        bool includeExternalUses;        // gather use of external symbols
        bool includeImports;            // gather imports
        bool includeLocations;          // include information about source locations
        bool includeJavadoc;
    };

    HqlParseContext(IEclRepository * _eclRepository, IPropertyTree * _archive)
    : archive(_archive), eclRepository(_eclRepository)
    {
        expandCallsWhenBound = DEFAULT_EXPAND_CALL;
        ignoreUnknownImport = false;
        _clear(metaState);
    }

    void addForwardReference(IHqlScope * owner, IHasUnlinkedOwnerReference * child);
    void noteBeginAttribute(IHqlScope * scope, IFileContents * contents, IIdAtom * name);
    void noteBeginModule(IHqlScope * scope, IFileContents * contents);
    void noteBeginQuery(IHqlScope * scope, IFileContents * contents);
    void noteEndAttribute();
    void noteEndModule();
    void noteEndQuery();
    void noteFinishedParse(IHqlScope * scope);
    void notePrivateSymbols(IHqlScope * scope);
    IPropertyTree * queryEnsureArchiveModule(const char * name, IHqlScope * scope);

    void setGatherMeta(const MetaOptions & options);

    inline IPropertyTree * getMetaTree() { return LINK(metaTree); }
    inline IPropertyTree * queryArchive() const { return archive; }
    inline IEclRepository * queryRepository() const { return eclRepository; }

public:
    Linked<IPropertyTree> archive;
    Linked<IEclRepository> eclRepository;
    Owned<IPropertyTree> nestedDependTree;
    Owned<IPropertyTree> globalDependTree;
    Owned<IPropertyTree> metaTree;
    IECLErrorArray orphanedWarnings;
    HqlExprArray defaultFunctionCache;
    CIArrayOf<ForwardScopeItem> forwardLinks;
    bool expandCallsWhenBound;
    bool ignoreUnknownImport;

private:
    bool checkBeginMeta();
    bool checkEndMeta();
    void finishMeta();

    MetaOptions metaOptions;

    struct {
        bool gatherNow;
        PointerArrayOf<IPropertyTree> nesting;
    } metaState;
};

class HqlDummyParseContext : public HqlParseContext
{
public:
    HqlDummyParseContext() : HqlParseContext(NULL, NULL) {}
};


class HqlLookupContext
{
public:
    explicit HqlLookupContext(const HqlLookupContext & other) : parseCtx(other.parseCtx)
    {
        errs.set(other.errs); 
        functionCache = other.functionCache; 
        curAttrTree.set(other.curAttrTree); 
    }
    HqlLookupContext(HqlParseContext & _parseCtx, IErrorReceiver * _errs)
    : parseCtx(_parseCtx), errs(_errs)
    {
        functionCache = &parseCtx.defaultFunctionCache;
    }

    void createDependencyEntry(IHqlScope * scope, IIdAtom * name);
    void noteBeginAttribute(IHqlScope * scope, IFileContents * contents, IIdAtom * name);
    void noteBeginModule(IHqlScope * scope, IFileContents * contents);
    void noteBeginQuery(IHqlScope * scope, IFileContents * contents);
    inline void noteEndAttribute() { parseCtx.noteEndAttribute(); }
    inline void noteEndModule() { parseCtx.noteEndAttribute(); }
    inline void noteEndQuery() { parseCtx.noteEndQuery(); }
    inline void noteFinishedParse(IHqlScope * scope) { parseCtx.noteFinishedParse(scope); }
    void noteExternalLookup(IHqlScope * parentScope, IHqlExpression * expr);
    inline void notePrivateSymbols(IHqlScope * scope) { parseCtx.notePrivateSymbols(scope); }

    inline IEclRepository * queryRepository() const { return parseCtx.eclRepository; }
    inline bool queryExpandCallsWhenBound() const { return parseCtx.expandCallsWhenBound; }
    inline HqlParseContext & queryParseContext() const { return parseCtx; }
    inline unsigned numErrors() const { return errs ? errs->errCount() : 0; }

protected:

    inline IPropertyTree * queryArchive() const { return parseCtx.archive; }
    inline IPropertyTree * queryNestedDependTree() const { return parseCtx.nestedDependTree; }

private:
    HqlParseContext & parseCtx;

public:
    Linked<IErrorReceiver> errs;
    HqlExprArray * functionCache;
    Owned<IPropertyTree> curAttrTree;
    HqlExprCopyArray dependents;
};

class HqlDummyLookupContext : public HqlLookupContext
{
public:
    //Potentially problematic - dummyCtx not created at the point the constructor is called.
    HqlDummyLookupContext(IErrorReceiver * _errs) : HqlLookupContext(dummyCtx, _errs) {}

private:
    HqlDummyParseContext dummyCtx;
};

enum
{
    LSFpublic       = 0x0000,
    LSFsharedOK     = 0x0001,
    LSFignoreBase   = 0x0002,
    LSFrequired     = 0x0004,
    LSFimport       = 0x0008,
    LSFfromderived  = 0x0010,
};
inline unsigned makeLookupFlags(bool sharedOK, bool ignoreBase, bool required)
{
    return (sharedOK ? LSFsharedOK : 0) |
           (ignoreBase ? LSFignoreBase : 0) |
           (required ? LSFrequired : 0);
}


//MORE: This should be split into a constant and creator interfaces
interface IHqlScope : public IInterface
{
    virtual IHqlExpression * queryExpression() = 0;
    virtual IHqlExpression *lookupSymbol(IIdAtom * searchName, unsigned lookupFlags, HqlLookupContext & ctx) = 0;

    virtual void getSymbols(HqlExprArray& exprs) const= 0;
    virtual IAtom * queryName() const = 0;
    virtual IIdAtom * queryId() const = 0;
    virtual const char * queryFullName() const = 0;
    virtual ISourcePath * querySourcePath() const = 0;
    virtual bool hasBaseClass(IHqlExpression * searchBase) = 0;
    virtual bool allBasesFullyBound() const = 0;

    virtual IHqlScope * clone(HqlExprArray & children, HqlExprArray & symbols) = 0;
    virtual IHqlScope * queryConcreteScope() = 0;
    virtual IHqlScope * queryResolvedScope(HqlLookupContext * context) = 0;
    virtual void ensureSymbolsDefined(HqlLookupContext & ctx) = 0;

    virtual bool isImplicit() const = 0;
    virtual bool isPlugin() const = 0;
    virtual int getPropInt(IAtom *, int) const = 0;
    virtual bool getProp(IAtom *, StringBuffer &) const = 0;

    //IHqlCreateScope
    virtual void defineSymbol(IIdAtom * name, IIdAtom * moduleName, IHqlExpression *value, bool isExported, bool isShared, unsigned flags, IFileContents *fc, int lineno, int column, int _startpos, int _bodypos, int _endpos) = 0;
    virtual void defineSymbol(IIdAtom * name, IIdAtom * moduleName, IHqlExpression *value, bool isExported, bool isShared, unsigned flags) = 0;
    virtual void defineSymbol(IHqlExpression * expr) = 0;       // use with great care, expr must be a named symbol.
    virtual void removeSymbol(IIdAtom * name) = 0;      // use with great care
};

interface IEclSource;
interface IHqlRemoteScope : public IInterface
{
    virtual IHqlScope * queryScope() = 0;
    virtual void setProp(IAtom *, const char *) = 0;
    virtual void setProp(IAtom *, int) = 0;
    virtual IEclSource * queryEclSource() const = 0;
};


interface IHqlDataset : public IInterface
{
    virtual IHqlExpression * queryExpression() = 0;
    virtual IHqlDataset* queryTable() = 0;
    virtual IHqlDataset * queryRootTable() = 0;         // get DATASET definition.
    virtual IHqlExpression * queryContainer() = 0;          // this shouldn't really be used - won't extent to more arbitrary relation trees.
};

extern HQL_API int getPrecedence(node_operator op);

struct BindParameterContext;
interface IHqlAnnotation;
class HQL_API CUsedTablesBuilder;
interface IHqlExpression : public IInterface
{
    virtual IAtom * queryName() const = 0;
    virtual IIdAtom * queryId() const = 0;
    virtual node_operator getOperator() const = 0;
    virtual bool isBoolean() = 0;
    virtual bool isDataset() = 0;
    virtual bool isDatarow() = 0;
    virtual bool isDictionary() = 0;
    virtual bool isScope() = 0;
    virtual bool isType() = 0;
    virtual bool isAction() = 0;
    virtual bool isConstant() = 0;
    virtual bool isTransform() = 0;
    virtual bool isList() = 0;
    virtual bool isField() = 0;
    virtual bool isMacro() = 0;
    virtual bool isRecord() = 0;
    virtual bool isFunction() = 0;
    virtual bool isAggregate() = 0;
    virtual bool isGroupAggregateFunction() = 0;
    virtual bool isPure() = 0;
    virtual bool isAttribute() const = 0;
    virtual annotate_kind getAnnotationKind() const = 0;
    virtual IHqlAnnotation * queryAnnotation() = 0;

    virtual unsigned getInfoFlags() const = 0;
    virtual unsigned getInfoFlags2() const = 0;

    virtual ISourcePath * querySourcePath() const = 0;
    virtual IIdAtom * queryFullContainerId() const = 0;              // only defined for a named symbol and global module.
    virtual int  getStartLine() const = 0;
    virtual int  getStartColumn() const = 0;
    virtual IPropertyTree * getDocumentation() const = 0;

    virtual ITypeInfo *queryType() const = 0;
    virtual ITypeInfo *getType() = 0;
    virtual StringBuffer &toString(StringBuffer &ret) = 0;
    virtual IHqlExpression *queryChild(unsigned idx) const = 0;
    virtual IHqlExpression *queryBody(bool singleLevel = false) = 0;
    virtual unsigned numChildren() const = 0;
    virtual bool isIndependentOfScope() = 0;
    virtual bool usesSelector(IHqlExpression * selector) = 0;
    virtual bool isIndependentOfScopeIgnoringInputs() = 0;
    virtual void gatherTablesUsed(HqlExprCopyArray * newScope, HqlExprCopyArray * inScope) = 0;
    virtual IValue *queryValue() const = 0;
    virtual IInterface *queryUnknownExtra() = 0;
    virtual unsigned __int64 querySequenceExtra() = 0;              // sequence, but also overloaded with parameter number

    virtual IHqlDataset *queryDataset() = 0;
    virtual IHqlScope *queryScope() = 0;
    virtual IHqlSimpleScope *querySimpleScope() = 0;
    virtual IHqlExpression *queryFunctionDefinition() const = 0;
    virtual IHqlExpression *queryExternalDefinition() const = 0;
    virtual IHqlExpression *queryNormalizedSelector(bool skipIndex=false) = 0;

    virtual IHqlExpression *queryAttribute(IAtom * propName) const = 0;
    virtual IHqlExpression *queryProperty(ExprPropKind kind) = 0;

    virtual ITypeInfo *queryRecordType() = 0;
    virtual IHqlExpression *queryRecord() = 0;

    virtual IHqlExpression *clone(HqlExprArray &) = 0;
    virtual IHqlExpression * cloneAnnotation(IHqlExpression * body) = 0;
    virtual IHqlExpression * cloneAllAnnotations(IHqlExpression * body) = 0;
    virtual void unwindList(HqlExprArray &dst, node_operator) = 0;

    virtual IInterface*         queryTransformExtra() = 0;
    virtual void                setTransformExtra(IInterface *) = 0;
    virtual void                setTransformExtraOwned(IInterface *) = 0;           // same as above, but argument is already linked
    virtual void                setTransformExtraUnlinked(IInterface *) = 0;        // same as above, but argument isn't linked

    virtual bool isFullyBound() const = 0;
    virtual IHqlExpression *closeExpr() = 0;                    // MORE - should be in expressionBuilder interface!
    virtual bool isExprClosed() const = 0;                            // aid to error recovery                 
    virtual IHqlExpression *addOperand(IHqlExpression *) = 0;   // MORE - should be in expressionBuilder interface!

    virtual StringBuffer& getTextBuf(StringBuffer& buf) = 0;
    virtual IFileContents * queryDefinitionText() const = 0;

    virtual unsigned getSymbolFlags() const = 0;              // only valid for a named symbol
    virtual bool isExported() const = 0;

    virtual IHqlExpression * queryAnnotationParameter(unsigned i) const = 0;

// The following are added to allow efficient storage/retreival in a hashtable.
    virtual void                addObserver(IObserver & observer) = 0;
    virtual void                removeObserver(IObserver & observer) = 0;
    virtual unsigned            getHash() const = 0;
    virtual bool                equals(const IHqlExpression & other) const = 0;

//purely used for internal processing.  Should not be called directly
    virtual void gatherTablesUsed(CUsedTablesBuilder & used) = 0;
    virtual unsigned            getCachedEclCRC() = 0;          // do not call directly - use getExpressionCRC()

// The following inline functions are purely derived from the functions in this interface
    inline int getPrecedence() const { return ::getPrecedence(getOperator()); }
    inline bool isAnnotation() const { return getAnnotationKind() != annotate_none; }
    inline bool isNamedSymbol() const { return getAnnotationKind() == annotate_symbol; }
    inline bool isFunctionDefinition() const { return getOperator() == no_funcdef; }
    inline bool hasAttribute(IAtom * propName) const { return queryAttribute(propName) != NULL; }
    inline bool hasText() const 
    { 
        IFileContents * contents = queryDefinitionText();
        return contents && contents->length() != 0;
    }

    inline IHqlExpression * queryCallParameter(unsigned i) { return queryChild(i); }
    inline IHqlExpression * queryDefinition() { return queryBody()->queryFunctionDefinition(); }
    inline unsigned numCallParameters() { return numChildren(); }
};

interface IHqlAnnotation : public IInterface
{
    virtual annotate_kind getAnnotationKind() const = 0;
    virtual IHqlExpression * queryExpression() = 0;
};

interface IHqlNamedAnnotation : public IHqlAnnotation
{
    virtual IFileContents * getBodyContents() = 0;
    virtual IHqlExpression * cloneSymbol(IIdAtom * optname, IHqlExpression * optnewbody, IHqlExpression * optnewfuncdef, HqlExprArray * optargs) = 0;
    virtual bool isExported() const = 0;
    virtual bool isShared() const = 0;
    virtual bool isPublic() const = 0;
    virtual bool isVirtual() const = 0;
    virtual int getStartLine() const = 0;
    virtual int getStartColumn() const = 0;
    virtual void setRepositoryFlags(unsigned _flags) = 0;  // To preserve flags like ob_locked, ob_sandbox, etc.
    virtual int getStartPos() const = 0;
    virtual int getBodyPos() const = 0;
    virtual int getEndPos() const = 0;
};


typedef Linked<IHqlExpression> HqlExprAttr;
typedef Shared<IHqlExpression> SharedHqlExpr;
typedef Linked<IHqlExpression> LinkedHqlExpr;
typedef Owned<IHqlExpression>  OwnedHqlExpr;

struct OwnedHqlExprItem : public CInterface
{
    OwnedHqlExpr value;
};

extern HQL_API const char *getOpString(node_operator op);
extern HQL_API IHqlExpression *createValue(node_operator op);
extern HQL_API IHqlExpression *createValue(node_operator op, IHqlExpression *p1, IHqlExpression *p2);
extern HQL_API IHqlExpression *createOpenValue(node_operator op, ITypeInfo *type);
extern HQL_API IHqlExpression *createOpenNamedValue(node_operator op, ITypeInfo *type, IIdAtom * id);
extern HQL_API IHqlExpression *createNamedValue(node_operator op, ITypeInfo *type, IIdAtom * id, HqlExprArray & args);
extern HQL_API IHqlExpression *createId(IIdAtom * id);

extern HQL_API IHqlExpression *createValue(node_operator op, ITypeInfo * type);
extern HQL_API IHqlExpression *createValue(node_operator op, ITypeInfo * type, IHqlExpression *p1);
extern HQL_API IHqlExpression *createValue(node_operator op, ITypeInfo * type, IHqlExpression *p1, IHqlExpression *p2);
extern HQL_API IHqlExpression *createValue(node_operator op, ITypeInfo * type, IHqlExpression *p1, IHqlExpression *p2, IHqlExpression *p3);
extern HQL_API IHqlExpression *createValue(node_operator op, ITypeInfo * type, IHqlExpression *p1, IHqlExpression *p2, IHqlExpression *p3, IHqlExpression *p4);
extern HQL_API IHqlExpression *createValueF(node_operator op, ITypeInfo * type, ...);
extern HQL_API IHqlExpression *createValue(node_operator op, ITypeInfo * type, unsigned num, IHqlExpression * * args);
extern HQL_API IHqlExpression *createValue(node_operator op, ITypeInfo * type, HqlExprArray & args);        //NB: This deletes the array that is passed
extern HQL_API IHqlExpression *createValueSafe(node_operator op, ITypeInfo * type, const HqlExprArray & args);
extern HQL_API IHqlExpression *createValueSafe(node_operator op, ITypeInfo * type, const HqlExprArray & args, unsigned from, unsigned max);
extern HQL_API IHqlExpression *createValueFromCommaList(node_operator op, ITypeInfo * type, IHqlExpression * argsExpr);

//These all consume their arguments
extern HQL_API IHqlExpression *createWrapper(node_operator op, IHqlExpression * expr);
extern HQL_API IHqlExpression *createWrapper(node_operator op, IHqlExpression * e, IHqlExpression * arg);
extern HQL_API IHqlExpression *createWrapper(node_operator op, HqlExprArray & args);
extern HQL_API IHqlExpression *createWrapper(node_operator op, ITypeInfo * type, HqlExprArray & args);

//This doesn't consume its argument!
extern HQL_API IHqlExpression *createWrappedExpr(IHqlExpression * expr, node_operator op, HqlExprArray & args);

extern HQL_API IHqlExpression *createAlienType(IIdAtom *, IHqlScope *);
extern HQL_API IHqlExpression *createAlienType(IIdAtom * name, IHqlScope * scope, HqlExprArray &newkids, IHqlExpression * funcdef=NULL);
extern HQL_API IHqlExpression *createEnumType(ITypeInfo * _type, IHqlScope *_scope);

extern HQL_API IHqlExpression *createBoolExpr(node_operator op, IHqlExpression *p1);
extern HQL_API IHqlExpression *createBoolExpr(node_operator op, IHqlExpression *p1, IHqlExpression *p2);
extern HQL_API IHqlExpression *createBoolExpr(node_operator op, IHqlExpression *p1, IHqlExpression *p2, IHqlExpression *p3);
extern HQL_API IHqlExpression *createBoolExpr(node_operator op, IHqlExpression *p1, IHqlExpression *p2, IHqlExpression *p3, IHqlExpression *p4);

extern HQL_API IHqlExpression *createList(node_operator op, ITypeInfo *type, IHqlExpression *p1);
extern HQL_API IHqlExpression *createBinaryList(node_operator op, HqlExprArray & args);
extern HQL_API IHqlExpression *createLeftBinaryList(node_operator op, HqlExprArray & args);

extern HQL_API IHqlExpression *createField(IIdAtom * name, ITypeInfo *type, IHqlExpression *defaultValue, IHqlExpression *attrs=NULL);
extern HQL_API IHqlExpression *createField(IIdAtom *name, ITypeInfo *type, HqlExprArray & args);
extern HQL_API IHqlExpression *createConstant(bool constant);
extern HQL_API IHqlExpression *createConstant(__int64 constant);
extern HQL_API IHqlExpression *createConstant(const char *constant);
extern HQL_API IHqlExpression *createConstant(double constant);
extern HQL_API IHqlExpression *createConstant(IValue * constant);
extern HQL_API IHqlExpression *createConstant(__int64 constant, ITypeInfo * ownedType);
extern HQL_API IHqlExpression *createDataset(node_operator op, IHqlExpression *dataset);
extern HQL_API IHqlExpression *createDataset(node_operator op, IHqlExpression *dataset, IHqlExpression *elist);
extern HQL_API IHqlExpression *createDataset(node_operator op, HqlExprArray & parms);       // inScope should only be set internally.
extern HQL_API IHqlExpression *createDatasetF(node_operator op, ...);
extern HQL_API IHqlExpression *createDictionary(node_operator op, IHqlExpression *initializer, IHqlExpression *recordDef);
extern HQL_API IHqlExpression *createDictionary(node_operator op, IHqlExpression *dictionary);
extern HQL_API IHqlExpression *createDictionary(node_operator op, HqlExprArray & parms);
extern HQL_API IHqlExpression *createNewDataset(IHqlExpression *name, IHqlExpression *recorddef, IHqlExpression *mode, IHqlExpression *parent, IHqlExpression *joinCondition, IHqlExpression * options = NULL);
extern HQL_API IHqlExpression *createRow(node_operator op, IHqlExpression *Dataset, IHqlExpression *element = NULL);
extern HQL_API IHqlExpression *createRow(node_operator op, HqlExprArray & args);
extern HQL_API IHqlExpression *createRowF(node_operator op, ...);
extern HQL_API IHqlExpression *createRecord();
extern HQL_API IHqlExpression *createRecord(const HqlExprArray & fields);
extern HQL_API IHqlExpression *createExternalReference(IIdAtom * name, ITypeInfo *_type, IHqlExpression *props);
extern HQL_API IHqlExpression *createExternalReference(IIdAtom * name, ITypeInfo *_type, HqlExprArray & attributes);
HQL_API IHqlExpression * createExternalFuncdefFromInternal(IHqlExpression * funcdef);
extern HQL_API IHqlExpression *createBoundFunction(IErrorReceiver * errors, IHqlExpression *func, HqlExprArray & ownedActuals, HqlExprArray * functionCache, bool forceExpansion);
extern HQL_API IHqlExpression *createReboundFunction(IHqlExpression *func, HqlExprArray & ownedActuals);
extern HQL_API IHqlExpression *createTranslatedExternalCall(IErrorReceiver * errors, IHqlExpression *func, HqlExprArray &actuals);
extern HQL_API IHqlExpression *createParameter(IIdAtom * name, unsigned idx, ITypeInfo *type, HqlExprArray & args);
extern HQL_API IHqlExpression *createDatasetFromRow(IHqlExpression * ownedRow);
extern HQL_API IHqlExpression * createTypedValue(node_operator op, ITypeInfo * type, HqlExprArray & args);
extern HQL_API IHqlExpression * createTypeTransfer(IHqlExpression * expr, ITypeInfo * newType);     //arguments must be linked

extern HQL_API IHqlExpression * cloneFieldMangleName(IHqlExpression * expr);
extern HQL_API IHqlExpression * expandOutOfLineFunctionCall(IHqlExpression * expr);
extern HQL_API void expandDelayedFunctionCalls(IErrorReceiver * errors, HqlExprArray & exprs);

extern HQL_API IHqlExpression *createQuoted(const char * name, ITypeInfo *type);
extern HQL_API IHqlExpression *createVariable(const char * name, ITypeInfo *type);
extern HQL_API IHqlExpression * createSymbol(IIdAtom * name, IHqlExpression *expr, unsigned exportFlags);
extern HQL_API IHqlExpression * createSymbol(IIdAtom * _name, IIdAtom * _module, IHqlExpression *_expr, bool _exported, bool _shared, unsigned _flags);
extern HQL_API IHqlExpression * createSymbol(IIdAtom * _name, IIdAtom * moduleName, IHqlExpression *expr, IHqlExpression * funcdef,
                                             bool exported, bool shared, unsigned symbolFlags,
                                             IFileContents *fc, int lineno, int column, int _startpos, int _bodypos, int _endpos);
extern HQL_API IHqlExpression *createAttribute(IAtom * name, IHqlExpression * value = NULL, IHqlExpression * value2 = NULL, IHqlExpression * value3 = NULL);
extern HQL_API IHqlExpression *createAttribute(IAtom * name, HqlExprArray & args);
extern HQL_API IHqlExpression *createExprAttribute(IAtom * name, IHqlExpression * value = NULL, IHqlExpression * value2 = NULL, IHqlExpression * value3 = NULL);
extern HQL_API IHqlExpression *createExprAttribute(IAtom * name, HqlExprArray & args);
extern HQL_API IHqlExpression *createLinkAttribute(IAtom * name, IHqlExpression * value = NULL, IHqlExpression * value2 = NULL, IHqlExpression * value3 = NULL);
extern HQL_API IHqlExpression *createLinkAttribute(IAtom * name, HqlExprArray & args);
extern HQL_API IHqlExpression *createUnknown(node_operator op, ITypeInfo * type, IAtom * name, IInterface * value);
extern HQL_API IHqlExpression *createSequence(node_operator op, ITypeInfo * type, IAtom * name, unsigned __int64 value);
extern HQL_API IHqlExpression *createCompareExpr(node_operator op, IHqlExpression * l, IHqlExpression * r);
extern HQL_API IHqlExpression * createAliasOwn(IHqlExpression * expr, IHqlExpression * attr);
inline IHqlExpression * createAlias(IHqlExpression * expr, IHqlExpression * attr) { return createAliasOwn(LINK(expr), LINK(attr)); }
extern HQL_API IHqlExpression * createLocationAnnotation(IHqlExpression * _ownedBody, const ECLlocation & _location);
extern HQL_API IHqlExpression * createLocationAnnotation(IHqlExpression * ownedBody, ISourcePath * sourcePath, int lineno, int column);
extern HQL_API IHqlExpression * createMetaAnnotation(IHqlExpression * _ownedBody, HqlExprArray & _args);
extern HQL_API IHqlExpression * createParseMetaAnnotation(IHqlExpression * _ownedBody, HqlExprArray & _args);
extern HQL_API IHqlExpression * createWarningAnnotation(IHqlExpression * _ownedBody, IECLError * _ownedWarning);
extern HQL_API IHqlExpression * createJavadocAnnotation(IHqlExpression * _ownedBody, IPropertyTree * _doc);

extern HQL_API IHqlExpression * createCompound(IHqlExpression * expr1, IHqlExpression * expr2);
extern HQL_API IHqlExpression * createCompound(const HqlExprArray & actions);
extern HQL_API IHqlExpression * createCompound(node_operator op, const HqlExprArray & actions);
extern HQL_API IHqlExpression * createActionList(const HqlExprArray & actions);
extern HQL_API IHqlExpression * createActionList(node_operator op, const HqlExprArray & actions);
extern HQL_API IHqlExpression * createActionList(const HqlExprArray & actions, unsigned from, unsigned to);
extern HQL_API IHqlExpression * createActionList(node_operator op, const HqlExprArray & actions, unsigned from, unsigned to);
extern HQL_API IHqlExpression * createComma(IHqlExpression * expr1, IHqlExpression * expr2);
extern HQL_API IHqlExpression * createComma(IHqlExpression * expr1, IHqlExpression * expr2, IHqlExpression * expr3);
extern HQL_API IHqlExpression * createComma(IHqlExpression * expr1, IHqlExpression * expr2, IHqlExpression * expr3, IHqlExpression * expr4);
extern HQL_API IHqlExpression * createComma(const HqlExprArray & exprs);
extern HQL_API IHqlExpression * createBalanced(node_operator op, ITypeInfo * type, const HqlExprArray & exprs);
extern HQL_API IHqlExpression * createBalanced(node_operator op, ITypeInfo * type, const HqlExprArray & exprs, unsigned first, unsigned last);
extern HQL_API IHqlExpression * createUnbalanced(node_operator op, ITypeInfo * type, const HqlExprArray & exprs);
extern HQL_API IHqlExpression * createAssign(IHqlExpression * expr1, IHqlExpression * expr2);
extern HQL_API void ensureActions(HqlExprArray & actions);
extern HQL_API void ensureActions(HqlExprArray & actions, unsigned first, unsigned last);
extern HQL_API IHqlExpression * extendConditionOwn(node_operator op, IHqlExpression * l, IHqlExpression * r);
inline IHqlExpression * extendCondition(node_operator op, IHqlExpression * l, IHqlExpression * r)
{
    return extendConditionOwn(op, LINK(l), LINK(r));
}

IHqlExpression * attachWorkflowOwn(HqlExprArray & meta, IHqlExpression * value, IHqlExpression * workflow, const HqlExprCopyArray * allActiveParameters);

extern HQL_API IHqlExpression * createNullExpr(ITypeInfo * type);
extern HQL_API IHqlExpression * createNullExpr(IHqlExpression * expr);
extern HQL_API IValue *         createNullValue(ITypeInfo * type);
extern HQL_API IHqlExpression * createNullScope();
extern HQL_API IHqlExpression * createPureVirtual(ITypeInfo * type);
extern HQL_API IHqlExpression * cloneOrLink(IHqlExpression * expr, HqlExprArray & children);
extern HQL_API IHqlExpression * createConstantOne();
extern HQL_API IHqlExpression * createLocalAttribute();
extern HQL_API bool isNullExpr(IHqlExpression * expr, IHqlExpression * field);
inline bool isNull(IHqlExpression * expr)       { return expr->getOperator() == no_null; }
inline bool isNullAction(IHqlExpression * expr) { return isNull(expr) && expr->isAction(); }
inline bool isFail(IHqlExpression * expr)       { return expr->getOperator() == no_fail; }

extern HQL_API IHqlExpression * createDelayedReference(node_operator op, IHqlExpression * moduleMarker, IHqlExpression * attr, unsigned lookupFlags, HqlLookupContext & ctx);
extern HQL_API IHqlExpression * createLibraryInstance(IHqlExpression * scopeFunction, HqlExprArray &operands);

extern HQL_API IHqlExpression* createValue(node_operator op, ITypeInfo *type, HqlExprArray& operands);
extern HQL_API IHqlExpression* createValue(node_operator op, HqlExprArray& operands);
extern HQL_API IHqlExpression *createValue(node_operator op, IHqlExpression *p1);
extern HQL_API IHqlExpression* createConstant(int ival);
extern HQL_API IHqlExpression* createBoolExpr(node_operator op, HqlExprArray& operands);
extern HQL_API IHqlExpression* createSelectExpr(IHqlExpression * lhs, IHqlExpression * rhs, bool isNew);
inline IHqlExpression* createSelectExpr(IHqlExpression * lhs, IHqlExpression * rhs) { return createSelectExpr(lhs, rhs, false); }
inline IHqlExpression* createNewSelectExpr(IHqlExpression * lhs, IHqlExpression * rhs) { return createSelectExpr(lhs, rhs, true); }
extern HQL_API IHqlExpression* createSelectExpr(HqlExprArray & args);

inline IHqlExpression* createAction(node_operator op, HqlExprArray& operands) { return createValue(op, makeVoidType(), operands); }

// Helper functions for retreiving meta information about a node.
extern HQL_API bool definesColumnList(IHqlExpression * dataset);
extern HQL_API unsigned getNumChildTables(IHqlExpression * dataset);
extern HQL_API bool isAggregateDataset(IHqlExpression * expr);
extern HQL_API bool isAggregatedDataset(IHqlExpression * expr);
extern HQL_API bool datasetHasGroupBy(IHqlExpression * expr);
extern HQL_API IHqlExpression *queryDatasetGroupBy(IHqlExpression * expr);
extern HQL_API bool hasSingleRow(IHqlExpression * expr);
extern HQL_API bool hasFewRows(IHqlExpression * expr);
extern HQL_API node_operator queryHasRows(IHqlExpression * expr);
extern HQL_API void queryRemoveRows(HqlExprCopyArray & tables, IHqlExpression * expr, IHqlExpression * left, IHqlExpression * right);

extern HQL_API bool isPureActivity(IHqlExpression * expr);
extern HQL_API bool isPureActivityIgnoringSkip(IHqlExpression * expr);
extern HQL_API bool assignsContainSkip(IHqlExpression * expr);

extern HQL_API bool isPureInlineDataset(IHqlExpression * expr);
extern HQL_API bool transformHasSkipAttr(IHqlExpression * transform);
extern HQL_API IHqlExpression * queryNewColumnProvider(IHqlExpression * expr);          // what is the transform/newtransform/record?
extern HQL_API unsigned queryTransformIndex(IHqlExpression * expr);
extern HQL_API bool isKnownTransform(IHqlExpression * transform);
extern HQL_API bool hasUnknownTransform(IHqlExpression * expr);

// Some Helper functions for processing the tree
extern HQL_API IHqlExpression * ensureExprType(IHqlExpression * expr, ITypeInfo * type);
extern HQL_API IHqlExpression * ensureExprType(IHqlExpression * expr, ITypeInfo * type, node_operator castOp);
extern HQL_API IHqlExpression * normalizeListCasts(IHqlExpression * expr);
extern HQL_API IHqlExpression * simplifyFixedLengthList(IHqlExpression * expr);
extern HQL_API IHqlExpression * getCastExpr(IHqlExpression * expr, ITypeInfo * type);

extern HQL_API void parseModule(IHqlScope *scope, IFileContents * contents, HqlLookupContext & ctx, IXmlScope *xmlScope, bool loadImplicit);
extern HQL_API IHqlExpression *parseQuery(IHqlScope *scope, IFileContents * contents, 
                                          HqlLookupContext & ctx, IXmlScope *xmlScope, IProperties * macroParams, bool loadImplicit);
extern HQL_API IHqlExpression *parseQuery(const char *in, IErrorReceiver * errs);

extern HQL_API IPropertyTree * gatherAttributeDependencies(IEclRepository * dataServer, const char * items = NULL);

extern HQL_API IHqlScope *createService();
extern HQL_API IHqlScope *createDatabase(IHqlExpression * name);
extern HQL_API IHqlScope *createScope();
extern HQL_API IHqlScope *createConcreteScope();
extern HQL_API IHqlScope *createForwardScope(IHqlScope * parentScope, HqlGramCtx * parentCtx, HqlParseContext & parseCtx);
extern HQL_API IHqlScope *createLibraryScope();
extern HQL_API IHqlScope *createVirtualScope();
extern HQL_API IHqlScope* createVirtualScope(IIdAtom * name, const char * fullName);
extern HQL_API IHqlScope *createScope(node_operator op);
extern HQL_API IHqlScope *createScope(IHqlScope * scope);
extern HQL_API IHqlScope *createPrivateScope();
extern HQL_API IHqlScope *createPrivateScope(IHqlScope * scope);
extern HQL_API IHqlExpression * createDelayedScope(IHqlExpression * expr);
extern HQL_API IHqlExpression * createDelayedScope(HqlExprArray &newkids);
extern HQL_API IHqlRemoteScope *createRemoteScope(IIdAtom * name, const char * fullName, IEclRepositoryCallback *ds, IProperties* props, IFileContents * _text, bool lazy, IEclSource * eclSource);
extern HQL_API IHqlExpression * populateScopeAndClose(IHqlScope * scope, const HqlExprArray & children, const HqlExprArray & symbols);

extern HQL_API IHqlExpression* createTemplateFunctionContext(IHqlExpression* body, IHqlScope* helperScope);
extern HQL_API IHqlExpression* createFieldMap(IHqlExpression*, IHqlExpression*);
extern HQL_API IHqlExpression * createNullDataset(IHqlExpression * ds);
extern HQL_API IHqlExpression * ensureDataset(IHqlExpression * expr);

extern HQL_API node_operator getInverseOp(node_operator op);
extern HQL_API node_operator getReverseOp(node_operator op);
extern HQL_API bool isKeyedJoin(IHqlExpression * expr);
extern HQL_API bool isSelfJoin(IHqlExpression * expr);
extern HQL_API bool isInnerJoin(IHqlExpression * expr);
extern HQL_API bool isFullJoin(IHqlExpression * expr);
extern HQL_API bool isLeftJoin(IHqlExpression * expr);
extern HQL_API bool isRightJoin(IHqlExpression * expr);
extern HQL_API bool isSimpleInnerJoin(IHqlExpression * expr);
extern HQL_API IAtom * queryJoinKind(IHqlExpression * expr);
inline bool isSpecificJoin(IHqlExpression * expr, IAtom * leftRightKind) { return queryJoinKind(expr) == leftRightKind; }

extern HQL_API bool isLimitedJoin(IHqlExpression * expr);
extern HQL_API bool isGroupedActivity(IHqlExpression * expr);
extern HQL_API bool isLocalActivity(IHqlExpression * expr);
extern HQL_API bool localChangesActivity(IHqlExpression * expr);
extern HQL_API bool localChangesActivityData(IHqlExpression * expr);
extern HQL_API bool localChangesActivityAction(IHqlExpression * expr);
extern HQL_API bool isInImplictScope(IHqlExpression * scope, IHqlExpression * dataset);
extern HQL_API bool isLimitedDataset(IHqlExpression * expr, bool onFailOnly=false);
extern HQL_API IHqlExpression * queryJoinRhs(IHqlExpression * expr);

extern HQL_API void lockTransformMutex();
extern HQL_API void unlockTransformMutex();
extern HQL_API void PrintLogExprTree(IHqlExpression *expr, const char *caption = NULL, bool full = false);

extern HQL_API IHqlExpression *doInstantEclTransformations(IHqlExpression *qquery, unsigned limit);
//extern HQL_API void loadImplicitScopes(IEclRepository &dataServer, HqlScopeArray &defualtScopes, int suppress, IIdAtom * suppressName);

extern HQL_API unsigned getExpressionCRC(IHqlExpression * expr);
extern HQL_API IHqlExpression * queryAttributeInList(IAtom * search, IHqlExpression * cur);
extern HQL_API IHqlExpression * queryAttribute(IAtom * search, const HqlExprArray & exprs);
extern HQL_API IHqlExpression * queryAnnotation(IHqlExpression * expr, annotate_kind search);       // return first match
extern HQL_API IHqlNamedAnnotation * queryNameAnnotation(IHqlExpression * expr);
extern HQL_API void gatherAttributes(HqlExprArray & matches, IAtom * search, IHqlExpression * expr);

inline bool hasAnnotation(IHqlExpression * expr, annotate_kind search){ return queryAnnotation(expr, search) != NULL; }
inline IHqlExpression * queryNamedSymbol(IHqlExpression * expr) { return queryAnnotation(expr, annotate_symbol); }
inline bool hasNamedSymbol(IHqlExpression * expr) { return hasAnnotation(expr, annotate_symbol); }
inline bool hasAttribute(IAtom * search, const HqlExprArray & exprs) { return queryAttribute(search, exprs) != NULL; }

extern HQL_API IHqlExpression * queryAnnotationAttribute(IAtom * search, IHqlExpression * annotation);
extern HQL_API IHqlExpression * queryMetaAttribute(IAtom * search, IHqlExpression * expr);
extern HQL_API void gatherMetaAttributes(HqlExprArray & matches, IAtom * search, IHqlExpression * expr);
extern HQL_API void gatherMetaProperties(HqlExprCopyArray & matches, IAtom * search, IHqlExpression * expr);

extern HQL_API IHqlExpression * queryLocation(IHqlExpression * expr);
extern HQL_API void gatherLocations(HqlExprCopyArray & matches, IHqlExpression * expr);
extern HQL_API bool okToAddAnnotation(IHqlExpression * expr);
extern HQL_API bool okToAddLocation(IHqlExpression *);

extern HQL_API IHqlExpression * cloneAnnotationKind(IHqlExpression * donor, IHqlExpression * expr, annotate_kind search);
extern HQL_API IHqlExpression * cloneInheritedAnnotations(IHqlExpression * donor, IHqlExpression * expr);
extern HQL_API IHqlExpression * cloneMissingAnnotations(IHqlExpression * donor, IHqlExpression * body);
extern HQL_API IHqlExpression * forceCloneSymbol(IHqlExpression * donor, IHqlExpression * expr);

// donor must be a symbol.  Any of the other arguments can be null to inherit the existing values
extern HQL_API IHqlExpression * cloneSymbol(IHqlExpression * donor, IIdAtom * optnewname, IHqlExpression * optnewbody, IHqlExpression * optnewfuncdef, HqlExprArray * optoperands);

extern HQL_API IHqlExpression * queryOperatorInList(node_operator search, IHqlExpression * cur);
extern HQL_API IHqlExpression * queryOperator(node_operator search, const HqlExprArray & args);
extern HQL_API IHqlExpression * queryRoot(IHqlExpression * dataset);
extern HQL_API IHqlExpression * queryTable(IHqlExpression * dataset);
extern HQL_API node_operator queryTableMode(IHqlExpression * expr);

// Code for producing simplified records that the file viewer can cope with
extern HQL_API ITypeInfo * getSimplifiedType(ITypeInfo * type, bool isConditional, bool isSerialized, IAtom * serialForm);
extern HQL_API IHqlExpression * getFileViewerRecord(IHqlExpression * record, bool isKey);
extern HQL_API IHqlExpression * getRecordMappingTransform(node_operator op, IHqlExpression * tgt, IHqlExpression * src, IHqlExpression * sourceSelector);
extern HQL_API IHqlExpression * getSimplifiedTransform(IHqlExpression * tgt, IHqlExpression * src, IHqlExpression * sourceSelector);
extern HQL_API IHqlExpression * removeVirtualAttributes(IHqlExpression * record);
extern HQL_API bool isSimplifiedRecord(IHqlExpression * expr, bool isKey);
extern HQL_API void unwindChildren(HqlExprArray & children, IHqlExpression * expr);
extern HQL_API void unwindChildren(HqlExprArray & children, const IHqlExpression * expr, unsigned first);
extern HQL_API void unwindChildren(HqlExprArray & children, const IHqlExpression * expr, unsigned from, unsigned to);
extern HQL_API void unwindChildren(HqlExprCopyArray & children, const IHqlExpression * expr, unsigned first=0);
extern HQL_API void unwindRealChildren(HqlExprArray & children, const IHqlExpression * expr, unsigned first);
extern HQL_API void unwindAttributes(HqlExprArray & children, const IHqlExpression * expr);
extern HQL_API void unwindList(HqlExprArray &dst, IHqlExpression * expr, node_operator op);
extern HQL_API void unwindCopyList(HqlExprCopyArray &dst, IHqlExpression * expr, node_operator op);
extern HQL_API void unwindCommaCompound(HqlExprArray & target, IHqlExpression * expr);
extern HQL_API void unwindRecordAsSelects(HqlExprArray & children, IHqlExpression * record, IHqlExpression * ds, unsigned max = (unsigned)-1);
extern HQL_API unsigned unwoundCount(IHqlExpression * expr, node_operator op);
extern HQL_API void unwindAttribute(HqlExprArray & args, IHqlExpression * expr, IAtom * name);
extern HQL_API IHqlExpression * queryChildOperator(node_operator op, IHqlExpression * expr);
extern HQL_API IHqlExpression * createSelector(node_operator op, IHqlExpression * ds, IHqlExpression * seq);
extern HQL_API IHqlExpression * createUniqueId();
extern HQL_API IHqlExpression * createUniqueRowsId();
extern HQL_API IHqlExpression * createCounter();
extern HQL_API IHqlExpression * createSelectorSequence();
extern HQL_API IHqlExpression * createSequenceExpr();
extern HQL_API IHqlExpression * createUniqueSelectorSequence();
extern HQL_API IHqlExpression * createSelectorSequence(unsigned __int64 seq);
extern HQL_API IHqlExpression * createDummySelectorSequence();
extern HQL_API IHqlExpression * expandBetween(IHqlExpression * expr);
extern HQL_API bool isAlwaysActiveRow(IHqlExpression * expr);
extern HQL_API bool isAlwaysNewRow(IHqlExpression * expr);
extern HQL_API IHqlExpression * ensureActiveRow(IHqlExpression * expr);
extern HQL_API bool isRedundantGlobalScope(IHqlExpression * expr);
extern HQL_API bool isIndependentOfScope(IHqlExpression * expr);
extern HQL_API bool isActivityIndependentOfScope(IHqlExpression * expr);
extern HQL_API bool exprReferencesDataset(IHqlExpression * expr, IHqlExpression * dataset);
extern HQL_API bool canEvaluateInScope(const HqlExprCopyArray & activeScopes, IHqlExpression * expr);
extern HQL_API bool canEvaluateInScope(const HqlExprCopyArray & activeScopes, const HqlExprCopyArray & required);
extern HQL_API IHqlExpression * ensureDeserialized(IHqlExpression * expr, ITypeInfo * type, IAtom * serialForm);
extern HQL_API IHqlExpression * ensureSerialized(IHqlExpression * expr, IAtom * serialForm);
extern HQL_API bool isDummySerializeDeserialize(IHqlExpression * expr);

extern HQL_API unsigned getRepeatMax(IHqlExpression * expr);
extern HQL_API unsigned getRepeatMin(IHqlExpression * expr);
extern HQL_API bool isStandardRepeat(IHqlExpression * expr);
extern HQL_API bool transformContainsCounter(IHqlExpression * transform, IHqlExpression * counter);
extern HQL_API bool preservesValue(ITypeInfo * after, IHqlExpression * expr);
extern HQL_API IHqlExpression * getActiveTableSelector();
extern HQL_API IHqlExpression * queryActiveTableSelector();
extern HQL_API IHqlExpression * getSelf(IHqlExpression * ds);
extern HQL_API IHqlExpression * querySelfReference();
extern HQL_API bool removeAttribute(HqlExprArray & args, IAtom * name);
extern HQL_API void removeAttributes(HqlExprArray & args);

extern HQL_API bool isChildRelationOf(IHqlExpression * child, IHqlExpression * other);
extern HQL_API IHqlExpression * queryRecord(ITypeInfo * type);
extern HQL_API unsigned numPayloadFields(IHqlExpression * index);
extern HQL_API unsigned firstPayloadField(IHqlExpression * index);
extern HQL_API unsigned firstPayloadField(IHqlExpression * record, unsigned numPayloadFields);
extern HQL_API unsigned numKeyedFields(IHqlExpression * index);
extern HQL_API bool isSortDistribution(IHqlExpression * distribution);
extern HQL_API bool isChooseNAllLimit(IHqlExpression * limit);

extern HQL_API bool isZero(IHqlExpression * expr);
extern HQL_API bool couldBeNegative(IHqlExpression * expr);
extern HQL_API bool isNegative(IHqlExpression * expr);

extern HQL_API bool isSelectFirstRow(IHqlExpression * expr);
extern HQL_API IHqlExpression * queryTransformSingleAssign(IHqlExpression * expr);
extern HQL_API IHqlExpression * convertToSimpleAggregate(IHqlExpression * expr);        // return NULL if doesn't make sense
extern HQL_API node_operator querySimpleAggregate(IHqlExpression * expr, bool canFilterArg, bool canCast);
extern HQL_API node_operator querySingleAggregate(IHqlExpression * expr, bool canFilterArg, bool canBeGrouped, bool canCast);
extern HQL_API bool isSimpleCountAggregate(IHqlExpression * aggregateExpr, bool canFilterArg);
extern HQL_API bool isSimpleCountExistsAggregate(IHqlExpression * aggregateExpr, bool canFilterArg, bool canCast);
extern HQL_API bool isKeyedCountAggregate(IHqlExpression * aggregate);
extern HQL_API IHqlExpression * createNullDataset();
extern HQL_API IHqlExpression * createNullDictionary();
extern HQL_API IHqlExpression * queryNullRecord();
extern HQL_API IHqlExpression * queryNullRowRecord();
extern HQL_API IHqlExpression * queryExpression(ITypeInfo * t);
extern HQL_API IHqlExpression * queryExpression(IHqlDataset * ds);
inline IHqlExpression * queryExpression(IHqlScope * scope) { return scope ? scope->queryExpression() : NULL; }
extern HQL_API IHqlScope * queryScope(ITypeInfo * t);
extern HQL_API IHqlRemoteScope * queryRemoteScope(IHqlScope * scope);
extern HQL_API IHqlAlienTypeInfo * queryAlienType(ITypeInfo * t);
extern HQL_API bool includeChildInDependents(IHqlExpression * expr, unsigned which);
extern HQL_API IHqlExpression * extractFieldAttrs(IHqlExpression * field);
extern HQL_API IHqlExpression * extractAttrsFromExpr(IHqlExpression * value);
extern HQL_API bool isUninheritedFieldAttribute(IHqlExpression * expr);
extern HQL_API bool hasUninheritedAttribute(IHqlExpression * field);
extern HQL_API IHqlExpression * extractChildren(IHqlExpression * value);
extern HQL_API IHqlExpression * queryOnlyField(IHqlExpression * record);
extern HQL_API bool recordTypesMatch(ITypeInfo * left, ITypeInfo * right);
extern HQL_API void assertRecordTypesMatch(ITypeInfo * left, ITypeInfo * right);
extern HQL_API void assertRecordTypesMatch(IHqlExpression * left, IHqlExpression * right);
extern HQL_API bool recordTypesMatch(IHqlExpression * left, IHqlExpression * right);
extern HQL_API bool recordTypesMatchIgnorePayload(IHqlExpression *left, IHqlExpression *right);
extern HQL_API IHqlExpression * queryOriginalRecord(IHqlExpression * expr);
extern HQL_API IHqlExpression * queryOriginalRecord(ITypeInfo * type);
extern HQL_API IHqlExpression * queryOriginalTypeExpression(ITypeInfo * type);
extern HQL_API ITypeInfo * createRecordType(IHqlExpression * record);
extern HQL_API bool isTargetSelector(IHqlExpression * expr);
extern HQL_API IHqlExpression * queryDatasetCursor(IHqlExpression * ds);
extern HQL_API IHqlExpression * querySelectorDataset(IHqlExpression * select, bool & isNew);
extern HQL_API IHqlExpression * replaceSelectorDataset(IHqlExpression * expr, IHqlExpression * newDataset);
extern HQL_API IHqlExpression * querySkipDatasetMeta(IHqlExpression * dataset);
extern HQL_API bool isNewSelector(IHqlExpression * expr);
extern HQL_API IHqlExpression * queryRecordAttribute(IHqlExpression * record, IAtom * name);
extern HQL_API bool isExported(IHqlExpression * expr);
extern HQL_API bool isShared(IHqlExpression * expr);
extern HQL_API bool isImport(IHqlExpression * expr);
extern HQL_API bool isVirtualSymbol(IHqlExpression * expr);

extern HQL_API IECLError * queryAnnotatedWarning(const IHqlExpression * expr);

extern HQL_API bool isPublicSymbol(IHqlExpression * expr);
extern HQL_API ITypeInfo * getSumAggType(IHqlExpression * arg);
extern HQL_API ITypeInfo * getSumAggType(ITypeInfo * argType);

extern HQL_API bool getAttribute(IHqlExpression * expr, IAtom * propName, StringBuffer & ret);

extern HQL_API bool filterIsKeyed(IHqlExpression * expr);
extern HQL_API bool filterIsUnkeyed(IHqlExpression * expr);
extern HQL_API bool canEvaluateGlobally(IHqlExpression * expr);
extern HQL_API bool isTrivialDataset(IHqlExpression * expr);
extern HQL_API bool isInlineTrivialDataset(IHqlExpression * expr);
extern HQL_API void gatherChildTablesUsed(HqlExprCopyArray * newScope, HqlExprCopyArray * inScope, IHqlExpression * expr, unsigned firstChild);
extern HQL_API IHqlScope * closeScope(IHqlScope * scope);
extern HQL_API IIdAtom * queryPatternName(IHqlExpression * expr);
extern HQL_API IHqlExpression * closeAndLink(IHqlExpression * expr);
extern HQL_API IHqlExpression * createAbstractRecord(IHqlExpression * record);
extern HQL_API IHqlExpression * createSortList(HqlExprArray & elements);

// Same as expr->queryChild() except it doesn't return attributes.
inline IHqlExpression * queryRealChild(IHqlExpression * expr, unsigned i)
{
    IHqlExpression * temp = expr->queryChild(i);
    if (temp && !temp->isAttribute())
        return temp;
    return NULL;
}
inline bool isCast(IHqlExpression * expr)
{
    node_operator op = expr->getOperator();
    return (op == no_cast) || (op == no_implicitcast);
}
extern HQL_API bool isKey(IHqlExpression * expr);

extern HQL_API IHqlExpression * queryGrouping(IHqlExpression * expr);
extern HQL_API IHqlExpression * queryDistribution(IHqlExpression * expr);
extern HQL_API IHqlExpression * queryGlobalSortOrder(IHqlExpression * expr);
extern HQL_API IHqlExpression * queryLocalUngroupedSortOrder(IHqlExpression * expr);
extern HQL_API IHqlExpression * queryGroupSortOrder(IHqlExpression * expr);

inline bool isGrouped(ITypeInfo * type)                     { return type && (type->getTypeCode() == type_groupedtable); }
inline bool isGrouped(IHqlExpression * expr)                { return isGrouped(expr->queryType()); }

inline IFunctionTypeExtra * queryFunctionTypeExtra(ITypeInfo * type)    { return static_cast<IFunctionTypeExtra *>(queryUnqualifiedType(type)->queryModifierExtra()); }
inline IHqlExpression * queryFunctionParameters(ITypeInfo * type)   { return static_cast<IHqlExpression *>(queryFunctionTypeExtra(type)->queryParameters()); }
inline IHqlExpression * queryFunctionParameterDefaults(ITypeInfo * type)    { return static_cast<IHqlExpression *>(queryFunctionTypeExtra(type)->queryDefaults()); }
inline IHqlExpression * queryFunctionParameters(IHqlExpression * expr)  { return queryFunctionParameters(expr->queryType()); }
extern HQL_API IHqlExpression * queryFunctionDefaults(IHqlExpression * expr);

inline IHqlExpression * querySelSeq(IHqlExpression * expr)
{
    return expr->queryAttribute(_selectorSequence_Atom);
}
extern HQL_API IHqlExpression * createGroupedAttribute(IHqlExpression * grouping);
extern HQL_API bool isSameUnqualifiedType(ITypeInfo * l, ITypeInfo * r);
extern HQL_API bool isSameFullyUnqualifiedType(ITypeInfo * l, ITypeInfo * r);
extern HQL_API IHqlExpression * queryNewSelectAttrExpr();

//The following are wrappers for the code generator specific getInfoFlags()
//inline bool isTableInvariant(IHqlExpression * expr)       { return (expr->getInfoFlags() & HEFtableInvariant) != 0; }
inline bool containsActiveDataset(IHqlExpression * expr){ return (expr->getInfoFlags() & HEFcontainsActiveDataset) != 0; }
inline bool containsActiveNonSelector(IHqlExpression * expr)
                                                        { return (expr->getInfoFlags() & HEFcontainsActiveNonSelector) != 0; }

inline bool containsNonActiveDataset(IHqlExpression * expr) { return (expr->getInfoFlags() & (HEFcontainsDataset)) != 0; }
inline bool containsAnyDataset(IHqlExpression * expr)   { return (expr->getInfoFlags() & (HEFcontainsDataset|HEFcontainsActiveDataset)) != 0; }
inline bool containsAlias(IHqlExpression * expr)        { return (expr->getInfoFlags() & HEFcontainsAlias) != 0; }
inline bool containsAliasLocally(IHqlExpression * expr) { return (expr->getInfoFlags() & HEFcontainsAliasLocally) != 0; }
inline bool containsDatasetAliasLocally(IHqlExpression * expr) { return (expr->getInfoFlags() & HEFcontainsDatasetAliasLocally) != 0; }
inline bool containsNonGlobalAlias(IHqlExpression * expr)   
                                                        { return (expr->getInfoFlags2() & HEF2containsNonGlobalAlias) != 0; }
inline bool containsAssertKeyed(IHqlExpression * expr)  { return (expr->getInfoFlags() & HEFassertkeyed) != 0; }
inline bool containsAssertStepped(IHqlExpression * expr){ return (expr->getInfoFlags2() & HEF2assertstepped) != 0; }
inline bool containsCounter(IHqlExpression * expr)      { return (expr->getInfoFlags() & HEFcontainsCounter) != 0; }
inline bool isCountProject(IHqlExpression * expr)       { return expr->hasAttribute(_countProject_Atom); }
inline bool containsSkip(IHqlExpression * expr)         { return (expr->getInfoFlags() & (HEFcontainsSkip)) != 0; }
inline bool containsSelf(IHqlExpression * expr)         { return (expr->getInfoFlags2() & (HEF2containsSelf)) != 0; }
inline bool isContextDependentExceptGraph(IHqlExpression * expr)    
                                                        { return (expr->getInfoFlags() & (HEFcontextDependent & ~HEFgraphDependent)) != 0; }
inline bool isGraphDependent(IHqlExpression * expr)     { return (expr->getInfoFlags() & HEFgraphDependent) != 0; }
inline bool containsTranslated(IHqlExpression * expr)   { return (expr->getInfoFlags() & (HEFtranslated)) != 0; }
inline bool containsSideEffects(IHqlExpression * expr)  { return (expr->getInfoFlags() & (HEFaction|HEFthrowscalar|HEFthrowds)) != 0; }
inline bool containsInternalSelect(IHqlExpression * expr)  { return (expr->getInfoFlags() & (HEFinternalSelect)) != 0; }
inline bool containsThisNode(IHqlExpression * expr)     { return (expr->getInfoFlags() & (HEFcontainsThisNode)) != 0; }
inline bool usesRuntimeContext(IHqlExpression * expr)   { return (expr->getInfoFlags() & (HEFaccessRuntimeContext)) != 0; }

inline bool containsWorkflow(IHqlExpression * expr)     { return (expr->getInfoFlags2() & (HEF2workflow)) != 0; }
inline bool containsMustHoist(IHqlExpression * expr)    { return (expr->getInfoFlags2() & (HEF2mustHoist)) != 0; }
inline bool containsNewDataset(IHqlExpression * expr)   { return (expr->getInfoFlags2() & HEF2containsNewDataset) != 0; }
inline bool containsCall(IHqlExpression * expr, bool includeOutOfLine)  
{ 
    unsigned mask = includeOutOfLine ? HEF2containsCall : HEF2containsDelayedCall;
    return (expr->getInfoFlags2() & mask) != 0;
}

inline bool hasDynamic(IHqlExpression * expr)           { return expr->hasAttribute(dynamicAtom); }
inline bool isAbstractDataset(IHqlExpression * expr)    
{ 
    IHqlExpression * record = expr->queryRecord();
    return record && record->hasAttribute(abstractAtom);
}
inline IHqlExpression * queryRecord(IHqlExpression * expr)
{
    if (!expr)
        return NULL;
    return expr->queryRecord();
}

extern HQL_API bool isPureVirtual(IHqlExpression * cur);
inline bool isForwardScope(IHqlScope * scope) { return scope && (queryExpression(scope)->getOperator() == no_forwardscope); }

extern HQL_API bool isContextDependent(IHqlExpression * expr, bool ignoreFailures = false, bool ignoreGraph = false);

extern HQL_API bool isPureCanSkip(IHqlExpression * expr);
extern HQL_API bool hasSideEffects(IHqlExpression * expr);
extern HQL_API bool containsAnyActions(IHqlExpression * expr);
extern bool canBeVirtual(IHqlExpression * expr);
extern bool areAllBasesFullyBound(IHqlExpression * module);
extern HQL_API bool isUpdatedConditionally(IHqlExpression * expr);
extern HQL_API bool activityMustBeCompound(IHqlExpression * expr);
extern HQL_API bool isSequentialActionList(IHqlExpression * expr);
extern HQL_API unsigned queryCurrentTransformDepth();                   // debugging - only valid inside a transform
extern HQL_API bool isExternalFunction(IHqlExpression * funcdef);
extern HQL_API bool isEmbedFunction(IHqlExpression * expr);
extern HQL_API bool isEmbedCall(IHqlExpression * expr);

typedef enum { 
    //Flags to indicate which datasets are available in scope
    childdataset_hasnone    = 0x0000,   // dataset->queryNormalizedSelector()
    childdataset_hasdataset = 0x0001,   // dataset->queryNormalizedSelector()
    childdataset_hasleft    = 0x0002,   // no_left
    childdataset_hasright   = 0x0004,   // no_right
    childdataset_hasactive  = 0x0008,   // no_activetable
    childdataset_hasevaluate= 0x0010,   // weird!

    //Flags that indicate the number/type of the dataset parameters to the operator
    childdataset_dsmask     = 0xFF00,
    childdataset_dsnone     = 0x0000,   // No datasets
    childdataset_ds         = 0x0100,   // A single dataset
    childdataset_dsds       = 0x0200,   // two datasets
    childdataset_dschild    = 0x0300,   // a dataset with second dependent on the first (normalize)
    childdataset_dsmany     = 0x0400,   // many datasetes
    childdataset_dsset      = 0x0500,   // [set-of-datasets]
    childdataset_dsif       = 0x0600,   // IF(<cond>, ds, ds)
    childdataset_dscase     = 0x0700,   // CASE
    childdataset_dsmap      = 0x0800,   // MAP

    //Combinations of the two sets of flags above for the cases which are currently used.
    childdataset_none               = childdataset_dsnone|childdataset_hasnone,
    childdataset_dataset_noscope    = childdataset_ds|childdataset_none,
    childdataset_dataset            = childdataset_ds|childdataset_hasdataset,
    childdataset_datasetleft        = childdataset_ds|childdataset_hasdataset|childdataset_hasleft,
    childdataset_left               = childdataset_ds|childdataset_hasleft,
    childdataset_leftright          = childdataset_dsds|childdataset_hasleft|childdataset_hasright,
    childdataset_same_left_right    = childdataset_ds|childdataset_hasleft|childdataset_hasright,
    childdataset_top_left_right     = childdataset_ds|childdataset_hasdataset|childdataset_hasleft|childdataset_hasright,
    childdataset_many_noscope       = childdataset_dsmany|childdataset_none,
    childdataset_many               = childdataset_dsmany|childdataset_hasactive,
    childdataset_nway_left_right    = childdataset_dsset|childdataset_hasleft|childdataset_hasright,

    //weird exceptions
    childdataset_evaluate           = childdataset_ds|childdataset_hasevaluate,
    childdataset_if                 = childdataset_dsif|childdataset_none,
    childdataset_case               = childdataset_dscase|childdataset_none,
    childdataset_map                = childdataset_dsmap|childdataset_none,
    childdataset_max
} childDatasetType;
extern HQL_API childDatasetType getChildDatasetType(IHqlExpression * expr);
inline bool hasLeft(childDatasetType value) { return (value & childdataset_hasleft) != 0; }
inline bool hasRight(childDatasetType value) { return (value & childdataset_hasright) != 0; }
inline bool hasLeftRight(childDatasetType value) { return (value & (childdataset_hasleft|childdataset_hasright)) == (childdataset_hasleft|childdataset_hasright); }
inline bool hasSameLeftRight(childDatasetType value)
{
    return hasLeftRight(value) &&
        (((value & childdataset_dsmask) == childdataset_ds) ||
         ((value & childdataset_dsmask) == childdataset_dsset));
}

// To improve error message.
extern HQL_API StringBuffer& getFriendlyTypeStr(IHqlExpression* e, StringBuffer& s);
extern HQL_API StringBuffer& getFriendlyTypeStr(ITypeInfo* type, StringBuffer& s);

#define ForEachChild(idx, expr)  unsigned numOfChildren##idx = (expr)->numChildren(); \
        for (unsigned idx = 0; idx < numOfChildren##idx; idx++) 

#define ForEachChildFrom(idx, expr, first)  unsigned numOfChildren##idx = (expr)->numChildren(); \
        for (unsigned idx = first; idx < numOfChildren##idx; idx++) 

extern HQL_API void exportData(IPropertyTree *data, IHqlExpression *table, bool flatten=false);

extern HQL_API void clearCacheCounts();
extern HQL_API void displayHqlCacheStats();


//only really makes sense on datasets, rows and selects.
inline bool isInScope(IHqlExpression * e)       { return e == e->queryNormalizedSelector(); }
inline int boolToInt(bool x)                    { return x ? 1 : 0; }

extern HQL_API IHqlExpression * createFunctionDefinition(IIdAtom * name, IHqlExpression * value, IHqlExpression * parms, IHqlExpression * defaults, IHqlExpression * attrs);
extern HQL_API IHqlExpression * createFunctionDefinition(IIdAtom * name, HqlExprArray & args);
extern HQL_API IHqlExpression * queryNonDelayedBaseAttribute(IHqlExpression * expr);

#define NO_AGGREGATE        \
         no_count:          \
    case no_sum:            \
    case no_variance:       \
    case no_covariance:     \
    case no_correlation:    \
    case no_max:            \
    case no_min:            \
    case no_ave:            \
    case no_exists

#define NO_AGGREGATEGROUP   \
         no_countgroup:         \
    case no_sumgroup:           \
    case no_vargroup:           \
    case no_covargroup:         \
    case no_corrgroup:          \
    case no_maxgroup:           \
    case no_mingroup:           \
    case no_avegroup:           \
    case no_existsgroup

extern HQL_API ITypeInfo * getTypedefType(IHqlExpression * expr);

extern HQL_API ITypeInfo * getPromotedECLType(ITypeInfo * lType, ITypeInfo * rType);
extern HQL_API ITypeInfo * getPromotedECLCompareType(ITypeInfo * lType, ITypeInfo * rType);
extern HQL_API void extendAdd(SharedHqlExpr & value, IHqlExpression * expr);
inline bool isOmitted(IHqlExpression * actual) { return !actual || actual->getOperator() == no_omitted; }

extern HQL_API IFileContents * createFileContentsFromText(unsigned len, const char * text, ISourcePath * sourcePath);
extern HQL_API IFileContents * createFileContentsFromText(const char * text, ISourcePath * sourcePath);
extern HQL_API IFileContents * createFileContentsFromFile(const char * filename, ISourcePath * sourcePath);
extern HQL_API IFileContents * createFileContentsSubset(IFileContents * contents, size32_t offset, size32_t len);
extern HQL_API IFileContents * createFileContents(IFile * file, ISourcePath * sourcePath);

void addForwardDefinition(IHqlScope * scope, IIdAtom * symbolName, IIdAtom * moduleName, IFileContents * contents,
                          unsigned symbolFlags, bool isExported, unsigned startLine, unsigned startColumn);

extern HQL_API IPropertyTree * createAttributeArchive();
extern HQL_API void ensureSymbolsDefined(IHqlExpression * scope, HqlLookupContext & ctx);
extern HQL_API void ensureSymbolsDefined(IHqlScope * scope, HqlLookupContext & ctx);
extern HQL_API bool getBoolAttribute(IHqlExpression * expr, IAtom * name, bool dft=false);
extern HQL_API bool getBoolAttributeInList(IHqlExpression * expr, IAtom * name, bool dft);

extern HQL_API void setLegacyEclSemantics(bool _legacyImport, bool _legacyWhen);
extern HQL_API bool queryLegacyImportSemantics();
extern HQL_API bool queryLegacyWhenSemantics();
void exportSymbols(IPropertyTree* data, IHqlScope * scope, HqlLookupContext & ctx);

//The following is only here to provide information about the source file being compiled when reporting leaks
extern HQL_API void setActiveSource(const char * filename);

#endif
