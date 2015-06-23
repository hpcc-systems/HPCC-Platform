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
#include "jmisc.hpp"
#include "jfile.hpp"
#include "jiter.ipp"
#include "jexcept.hpp"
#include "jmutex.hpp"
#include "jsort.hpp"
#include "jutil.hpp"
#include "hql.hpp"
#include "hqlexpr.ipp"
#include "hqlgram.hpp"
#include "hqlfold.hpp"
#include "hqlthql.hpp"
#include "hqlpmap.hpp"
#include <math.h>
#include "hqlerrors.hpp"
#include "hqlerror.hpp"
#include "hqlplugins.hpp"
#include "hqltrans.ipp"
#include "hqlutil.hpp"
#include "eclrtl.hpp"
#include "hqlattr.hpp"
#include "hqlmeta.hpp"

static SpinLock * propertyLock;

MODULE_INIT(INIT_PRIORITY_HQLINTERNAL)
{
    propertyLock = new SpinLock;
    return true;
}
MODULE_EXIT()
{
    delete propertyLock;
}

// This file should contain most of the derived property calculation for nodes in the expression tree,
// Other candidates are
// checkConstant, getChilddatasetType(), getNumChildTables
// queryHasRows, definesColumnList(), queryTransformIndex
// initFlagsBefore(), updatFlagsAfter()
// getCachedEclCRC(), cacheTablesUsed(), isIndependentOfScope()
// logic inside createDataset

//Originally the idea was to have a class instance for each kind of opcode, and to call opcode[op]->evalautePropXXXXXX(this);
//to evaluate the property.  However because there are so many opcodes I'm not convinced this is the best way.
//Better may be to model it more on the way queryRecordCount() is implemented.

//This switch statement provides an entry of each opcode grouped according to its function.
//within each group try and maintain alphabetical ordering
unsigned getOperatorMetaFlags(node_operator op)
{
    switch (op)
    {
    case no_none:
    case no_nobody:

//Records/types
    case no_field:
    case no_record:
    case no_type:
    case no_ifblock:
    case no_enum:
    case no_selfref:
    case no_typedef:

//Simple arithmetic expressions with no children:
    case no_constant:
    case no_variable:
    case no_quoted:                 // codegen only
    case no_getresult:
    case no_matched:
    case no_matchtext:
    case no_matchlength:
    case no_matchposition:
    case no_failcode:
    case no_failmessage:
    case no_id2blob:
    case no_blob2id:
    case no_clustersize:
    case no_loopcounter:
    case no_callback:
    case no_assertwild:
    case no_eventname:
    case no_eventextra:
    case no_debug_option_value: 

//Arithmetic operators
    case no_mul:
    case no_div:
    case no_modulus:
    case no_negate:
    case no_add:
    case no_sub:
    case no_exp:
    case no_power:
    case no_round:
    case no_roundup:
    case no_ln:
    case no_log10:
    case no_sin:
    case no_cos:
    case no_tan:
    case no_asin:
    case no_acos:
    case no_atan:
    case no_atan2:
    case no_sinh:
    case no_cosh:
    case no_tanh:
    case no_sqrt:
    case no_truncate:
    case no_cast:
    case no_implicitcast:
    case no_abs:
    case no_charlen:
    case no_sizeof:
    case no_offsetof:
    case no_nameof:
    case no_band:
    case no_bor:
    case no_bxor:
    case no_bnot:
    case no_order:          //?? also a comparison
    case no_rank:
    case no_ranked:
    case no_hash:
    case no_typetransfer:
    case no_lshift:
    case no_rshift:
    case no_crc:
    case no_random:
    case no_counter:
    case no_address:
    case no_hash32:
    case no_hash64:
    case no_wuid:
    case no_countdict:
    case no_existslist:
    case no_countlist:
    case no_maxlist:
    case no_minlist:
    case no_sumlist:
    case no_unicodeorder:
    case no_assertkeyed:
    case no_hashmd5:
    case no_pure:
    case no_sequence:
    case no_getenv:
    
//Selection operators  - could be arithmetic, string, dataset etc.
    case no_map:
    case no_if:
    case no_choose:
    case no_which:
    case no_rejected:
    case no_mapto:
    case no_case:

//String operators:
    case no_concat:
    case no_substring:
    case no_asstring:
    case no_intformat:
    case no_realformat:
    case no_trim:
    case no_fromunicode:
    case no_tounicode:
    case no_keyunicode:
    case no_rowdiff:
    case no_xmltext:
    case no_xmlunicode:
    case no_xmldecode:
    case no_xmlencode:
    case no_matchunicode:
    case no_matchutf8:
    case no_regex_find:
    case no_regex_replace:
    case no_toxml:
    case no_tojson:

//Boolean operators:
    case no_eq:
    case no_ne:
    case no_lt:
    case no_le:
    case no_gt:
    case no_ge:
    case no_not:
    case no_notnot:
    case no_and:
    case no_or:
    case no_xor:
    case no_notin:
    case no_in:
    case no_notbetween:
    case no_between:
    case no_is_valid:
    case no_indict:

//Lists/Sets etc.
    case no_list:
    case no_all:
    case no_addsets:
    case no_createset:

    case no_rowset:
    case no_rowsetindex:
    case no_rowsetrange:
    case no_sortlist:
    case no_recordlist:
    case no_datasetlist:
    case no_transformlist:

//Aggregate operators
    case no_count:
    case no_exists:
    case no_existsdict:
    case no_max:
    case no_min:
    case no_sum:
    case no_ave:
    case no_variance:
    case no_covariance:
    case no_correlation:
    case no_countgroup:
    case no_existsgroup:
    case no_maxgroup:
    case no_mingroup:
    case no_sumgroup:
    case no_avegroup:
    case no_vargroup:
    case no_covargroup:
    case no_corrgroup:

    case no_within:
    case no_notwithin:
    case no_countcompare:

//Selectors
    case no_left:
    case no_right:
    case no_self:
    case no_activetable:
    case no_activerow:
    case no_top:

//Transforms
    case no_transform:
    case no_assign:
    case no_assignall:
    case no_newtransform:

//Rows
    case no_selectmap:
    case no_selectnth:
    case no_matchrow:
    case no_matchattr:      // and scalar
    case no_projectrow:
    case no_createrow:
    case no_newrow:
    case no_temprow:

//Dictionaries
    case no_createdictionary:

//Datasets [see also selection operators]
    case no_rollup:
    case no_iterate:
    case no_hqlproject:
    case no_group:
    case no_cogroup:
    case no_cosort:
    case no_index:
    case no_table:
    case no_keyindex:
    case no_temptable:
    case no_usertable:
    case no_choosen:
    case no_filter:
    case no_fetch:
    case no_join:
    case no_sort:
    case no_subsort:
    case no_sorted:
    case no_dedup:
    case no_enth:
    case no_sample:
    case no_selectfields:
    case no_addfiles:
    case no_distribute:
    case no_normalize:
    case no_distributed:
    case no_preservemeta:
    case no_grouped:
    case no_denormalize:
    case no_newusertable:
    case no_newaggregate:
    case no_aggregate:
    case no_choosesets:
    case no_workunit_dataset:
    case no_split:
    case no_spill:
    case no_readspill:
    case no_writespill:
    case no_commonspill:
    case no_parse:
    case no_newparse:
    case no_throughaggregate:
    case no_compound_diskread:
    case no_compound_disknormalize:
    case no_compound_diskaggregate:
    case no_compound_diskcount:
    case no_compound_diskgroupaggregate:
    case no_compound_indexread:
    case no_compound_indexnormalize:
    case no_compound_indexaggregate:
    case no_compound_indexcount:
    case no_compound_indexgroupaggregate:
    case no_compound_childread:
    case no_compound_childnormalize:
    case no_compound_childaggregate:
    case no_compound_childcount:
    case no_compound_childgroupaggregate:
    case no_compound_inline:
    case no_getgraphresult:
    case no_compound_fetch:
    case no_topn:
    case no_newxmlparse:
    case no_httpcall:
    case no_soapcall:
    case no_soapcall_ds:
    case no_newsoapcall:
    case no_newsoapcall_ds:
    case no_quantile:
    case no_nonempty:
    case no_filtergroup:
    case no_limit:
    case no_catchds:
    case no_loop:
    case no_forcenolocal:
    case no_allnodes:
    case no_selfjoin:
    case no_process:
    case no_thisnode:
    case no_getgraphloopresult:
    case no_graphloop:
    case no_assertstepped:
    case no_assertsorted:
    case no_assertgrouped:
    case no_assertdistributed:
    case no_mergejoin:
    case no_nwayjoin:
    case no_nwaymerge:
    case no_stepped:
    case no_datasetfromrow:
    case no_datasetfromdictionary:
    case no_assert_ds:
    case no_combine:
    case no_rollupgroup:
    case no_regroup:
    case no_combinegroup:
    case no_inlinetable:
    case no_denormalizegroup:
    case no_xmlproject:
    case no_spillgraphresult:
    case no_rows:
    case no_keyedlimit:
    case no_compound_selectnew:
    case no_getgraphloopresultset:
    case no_preload:
    case no_merge:
    case no_keyeddistribute:
    case no_newkeyindex:
    case no_anon:
    case no_pseudods:
    case no_deserialize:
    case no_serialize:
    case no_forcegraph:
    case no_related:
    case no_executewhen:
    case no_callsideeffect:
    case no_fromxml:
    case no_fromjson:
    case no_xmlparse:
    case no_normalizegroup:
    case no_owned_ds:
    case no_dataset_alias:
    case no_chooseds:

//Multiple different kinds of values
    case no_select:
    case no_indirect:
    case no_selectindirect:
    case no_null:
    case no_globalscope:
    case no_nothor:
    case no_embedbody:
    case no_alias_scope:
    case no_evalonce:
    case no_forcelocal:
    case no_cluster:

//Parser only - not in normalized expression trees
    case no_evaluate:
    case no_macro:
    case no_transformebcdic:
    case no_transformascii:
    case no_metaactivity:
    case no_loadxml:
    case no_fieldmap:
    case no_template_context:
    case no_processing:
    case no_merge_pending:
    case no_merge_nomatch:
    case no_namedactual:
    case no_assertconstant:
    case no_assertconcrete:
    case no_delayedscope:

//Code generator only - only created once code is being generated.
    case no_postinc:
    case no_postdec:
    case no_preinc:
    case no_predec:
    case no_pselect:
    case no_deref:
    case no_ordered:
    case no_decimalstack:
    case no_translated:
    case no_filepos:
    case no_file_logicalname:
    case no_reference:
    case no_assign_addfiles:
    case no_nullptr:
    case no_childquery:

//Workflow
    case no_stored:
    case no_failure:
    case no_success:
    case no_recovery:
    case no_wait:
    case no_event:
    case no_persist:
    case no_when:
    case no_setconditioncode:
    case no_priority:
    case no_colon:
    case no_setworkflow_cond:
    case no_global:
    case no_workflow:
    case no_workflow_action:
    case no_checkpoint:
    case no_define:
    case no_independent:
    case no_catch:
    case no_once:

//Patterns 
    case no_pat_select:
    case no_pat_const:
    case no_pat_pattern:
    case no_pat_follow:
    case no_pat_first:
    case no_pat_last:
    case no_pat_repeat:
    case no_pat_instance:
    case no_pat_anychar:
    case no_pat_token:
    case no_pat_imptoken:
    case no_pat_set:
    case no_pat_checkin:
    case no_pat_x_before_y:
    case no_pat_x_after_y:
    case no_pat_index:
    case no_pat_beginpattern:
    case no_pat_endpattern:
    case no_pat_checklength:
    case no_pat_featureparam:
    case no_pat_featureactual:
    case no_pat_featuredef:
    case no_pat_validate:
    case no_pat_use:
    case no_featuretype:
    case no_pat_guard:
    case no_penalty:
    case no_pat_case:
    case no_pat_nocase:
    case no_pat_before_y:
    case no_pat_after_y:
    case no_pat_production:
    case no_pat_or:

//Pseudo-Attributes
    case no_csv:
    case no_sql:
    case no_thor:
    case no_flat:
    case no_pipe:
    case no_joined:
    case no_any:
    case no_xml:
    case no_json:
    case no_distributer:
    case no_keyed:
    case no_sortpartition:

//Multiple types
    case no_outofline:
    case no_create_initializer:

//Actions
    case no_buildindex:
    case no_output:
    case no_apply:
    case no_fail:
    case no_distribution:
    case no_ensureresult:
    case no_setresult:
    case no_sequential:
    case no_parallel:
    case no_actionlist:
    case no_orderedactionlist:
    case no_soapaction_ds:
    case no_newsoapaction_ds:
    case no_keydiff:
    case no_keypatch:
    case no_returnresult:
    case no_outputscalar:
    case no_evaluate_stmt:
    case no_return_stmt:
    case no_setgraphloopresult:
    case no_skip:
    case no_assert:
    case no_notify:
    case no_setgraphresult:
    case no_extractresult:
    case no_unused81:
    case no_definesideeffect:

// Scopes etc.
    case no_scope:
    case no_forwardscope:
    case no_remotescope:
    case no_privatescope:
    case no_virtualscope:
    case no_concretescope:
    case no_mergedscope:

//Used for representing functional attributes
    case no_service:
    case no_external:
    case no_funcdef:
    case no_externalcall:
    case no_libraryselect:
    case no_bound_func:
    case no_purevirtual:
    case no_internalselect:
    case no_delayedselect:
    case no_unboundselect:
    case no_libraryscope:
    case no_libraryscopeinstance:
    case no_libraryinput:
    case no_call:
    case no_attrname:

// Other
    case no_comma:
    case no_uncommoned_comma:
    case no_compound:
    case no_param:
    case no_setmeta:
    case no_omitted:
    case no_range:
    case no_rangeto:
    case no_rangefrom:
    case no_rangecommon:
    case no_nofold:
    case no_nohoist:
    case no_nocombine:
    case no_section:
    case no_sectioninput:
    case no_alias:
    case no_unknown:                // used for callbacks
    case no_attr:
    case no_attr_link:
    case no_attr_expr:
    case no_cachealias:
    case no_subgraph:
    case no_rowvalue:
    case no_loopbody:
    case no_complex:

//Not implemented anywhere:
    case no_impure:             // not really used
    case no_dependenton:
    case no_alias_project:
    case no_nolink:
    case no_joincount:
    case no_guard:
    case no_hint:
    case no_cloned:
    case no_childdataset:
    case no_envsymbol:
    case no_bound_type:
    case no_mix:
    case no_persist_check:
    case no_dataset_from_transform:
    case no_id:

    case no_unused6:
    case no_unused13: case no_unused14: case no_unused15:
    case no_unused28: case no_unused29:
    case no_unused30: case no_unused31: case no_unused32: case no_unused33: case no_unused34: case no_unused35: case no_unused36: case no_unused37: case no_unused38:
    case no_unused40: case no_unused41: case no_unused42: case no_unused43: case no_unused44: case no_unused45: case no_unused46: case no_unused47: case no_unused48: case no_unused49:
    case no_unused50: case no_unused52:
    case no_unused80: case no_unused83:
    case no_unused102:
    case no_is_null:
    case no_position:
    case no_current_time:
    case no_current_date:
    case no_current_timestamp:

    case no_update:

//The following never get created IHqlExpressions, they are used as constants in the PARSE internal structures.
    case no_pat_compound:
    case no_pat_begintoken:
    case no_pat_endtoken:
    case no_pat_begincheck:
    case no_pat_endcheckin:
    case no_pat_endchecklength:
    case no_pat_beginseparator:
    case no_pat_endseparator:
    case no_pat_separator:
    case no_pat_beginvalidate:
    case no_pat_endvalidate:
    case no_pat_dfa:
    case no_pat_singlechar:
    case no_pat_beginrecursive:
    case no_pat_endrecursive:
    case no_pat_utf8single:
    case no_pat_utf8lead:
    case no_pat_utf8follow:
    case no_eclcrc:
        return 0;
    case no_isomitted:
        return 0;
    default:
        DBGLOG("**** Missing meta flags for operator %d ***", (int)op);
        return 0;
    }
}


//---------------------------------------------------------------------------------

inline unsigned truncMaxlength(unsigned __int64 value)
{
    return (value > MAX_MAXLENGTH) ? MAX_MAXLENGTH : (unsigned)value;
}

static unsigned getMaxSize(ITypeInfo * type, IHqlExpression * maxLength, IHqlExpression * maxSize, IHqlExpression * maxCount)
{
    unsigned size = type->getSize();
    if (size != UNKNOWN_LENGTH)
        return size;

    if (!maxLength) maxLength = queryAttributeChild(type, maxLengthAtom, 0);
    if (!maxSize) maxSize = queryAttributeChild(type, maxSizeAtom, 0);
    if (!maxCount) maxCount = queryAttributeChild(type, maxCountAtom, 0);

    if (maxSize)
        return (unsigned)getIntValue(maxSize, 0);

    if (maxLength)
    {
        unsigned __int64 len = (unsigned)getIntValue(maxLength, 0);
        switch (type->getTypeCode())
        {
        case type_string:
        case type_data:
            return truncMaxlength(sizeof(size32_t) + len);
        case type_unicode:
            return truncMaxlength(sizeof(size32_t) + len*sizeof(UChar));
        case type_qstring:
            return truncMaxlength(sizeof(size32_t) + rtlQStrSize((unsigned)len));
        case type_varstring:
            return truncMaxlength(len + 1);
        case type_varunicode:
            return truncMaxlength((len + 1) * sizeof(UChar));
        case type_utf8:
            return truncMaxlength(sizeof(size32_t) + (len * 4));
        case type_set:
            return truncMaxlength(len);
        }
    }

    if (maxCount)
    {
        unsigned __int64 count = getIntValue(maxCount, 0);
        switch (type->getTypeCode())
        {
        case type_set:
            {
                ITypeInfo * childType = type->queryChildType();
                if (!childType)
                    break;
                unsigned elemSize = getMaxSize(childType, NULL, NULL, NULL);
                if (elemSize != UNKNOWN_LENGTH)
                    return truncMaxlength(sizeof(bool) + sizeof(size32_t) + count * elemSize);
                break;
            }
        }
    }

    return UNKNOWN_LENGTH;
}

static unsigned getMaxSize(IHqlExpression * field)
{
    switch (field->getOperator())
    {
    case no_select:
        return getMaxSize(field->queryChild(1));
    case no_indirect:
        return getMaxSize(field->queryChild(0));
    }

    ITypeInfo * type = field->queryType();
    IHqlExpression * maxLength = queryAttributeChild(field, maxLengthAtom, 0);
    IHqlExpression * maxSize = queryAttributeChild(field, maxSizeAtom, 0);
    IHqlExpression * maxCount = queryAttributeChild(field, maxCountAtom, 0);
    unsigned max = getMaxSize(type, maxLength, maxSize, maxCount);
    if (max != UNKNOWN_LENGTH)
        return max;
    ITypeInfo * indirect = queryModifier(type, typemod_indirect);
    if (indirect)
    {
        IHqlExpression * original = static_cast<IHqlExpression *>(indirect->queryModifierExtra());
        return getMaxSize(original);
    }
    return max;
}

//Some arbitrary guess at the size of a variable length string field.
static double twoThirds = 2.0/3.0;
static unsigned guessSize(unsigned minLen, unsigned maxLen)
{
    if (maxLen == UNKNOWN_LENGTH)
        maxLen = 4096;
    if (maxLen < minLen)
        return minLen;
    double value = pow((double)(maxLen-minLen), twoThirds);
    return truncMaxlength(minLen + (unsigned __int64)ceil(value));
}


static IHqlExpression * querySerializedForm(IHqlExpression * expr, IAtom * variation)
{
    if (expr)
    {
        ExprPropKind kind;
        if (variation == diskAtom)
            kind = EPdiskserializedForm;
        else if (variation == internalAtom)
            kind = EPinternalserializedForm;
        else
            throwUnexpected();

        IHqlExpression * attr = expr->queryProperty(kind);
        if (attr)
            return attr;
    }
    return expr;
}


static HqlTransformerInfo serializedRecordCreatorInfo("SerializedRecordCreator");
class SerializedRecordCreator : public QuickHqlTransformer
{
public:
    SerializedRecordCreator(IAtom * _variety) : QuickHqlTransformer(serializedRecordCreatorInfo, NULL), variety(_variety) {}

    virtual IHqlExpression * createTransformedBody(IHqlExpression * expr)
    {
        switch (expr->getOperator())
        {
        case no_field:
            {
                //Remove the default value before transforming to avoid waste when processing before the expression tree is normalized
                if (queryRealChild(expr, 0))
                {
                    HqlExprArray children;
                    unwindChildren(children, expr, 1);
                    OwnedHqlExpr plain = expr->clone(children);
                    return transform(plain);
                }
                if (expr->hasAttribute(_linkCounted_Atom))
                {
                    OwnedHqlExpr transformed = QuickHqlTransformer::createTransformedBody(expr);
                    return removeAttribute(transformed, _linkCounted_Atom);
                }
                break;
            }
        }
        return QuickHqlTransformer::createTransformedBody(expr);
    }

    virtual ITypeInfo * transformType(ITypeInfo * type)
    {
        Owned<ITypeInfo> transformed = QuickHqlTransformer::transformType(type);
        return getSerializedForm(transformed, variety);
    }

protected:
    IAtom * variety;
};

static IHqlExpression * evaluateSerializedRecord(IHqlExpression * expr, IAtom * variation)
{
    SerializedRecordCreator transformer(variation);
    return transformer.transform(expr);
}

//---------------------------------------------------------------------------------

class CHqlExprMeta
{
public:
    static inline void addProperty(IHqlExpression * expr, ExprPropKind kind, IInterface * value)
    {
        CHqlExpression * cexpr = static_cast<CHqlExpression *>(expr);
        cexpr->addProperty(kind, value);
    }
    static inline IInterface * queryExistingProperty(IHqlExpression * expr, ExprPropKind kind)
    {
        CHqlExpression * cexpr = static_cast<CHqlExpression *>(expr);
        return cexpr->queryExistingProperty(kind);
    }
} meta;

//-- Attribute: serialized form -------------------------------------------------------------------------------

static IHqlExpression * evaluatePropSerializedForm(IHqlExpression * expr, ExprPropKind kind, IAtom * variation)
{
    if (expr->getOperator() == no_record || expr->getOperator() == no_field)
    {
        OwnedHqlExpr serialized = evaluateSerializedRecord(expr, variation);
        if (serialized != expr)
        {
            //Tag serialized form so don't re-evaluated
            meta.addProperty(serialized, kind, serialized);
        }
        meta.addProperty(expr, kind, serialized);
        return serialized;
    }
    return NULL;
}

//-- Attribute: size -------------------------------------------------------------------------------

//no_field
static IHqlExpression * evaluateFieldAttrSize(IHqlExpression * expr) 
{ 
    ITypeInfo * type = expr->queryType();
    unsigned minSize = UNKNOWN_LENGTH;
    unsigned maxSize = 0;
    unsigned thisSize = type->getSize();
    OwnedHqlExpr thisMaxSizeExpr;

    if (expr->hasAttribute(_isBlobInIndex_Atom))
    {
        thisSize = sizeof(unsigned __int64);
    }
    else
    {
        switch (type->getTypeCode())
        {
        case type_bitfield:
            {
                thisSize = type->queryChildType()->getSize();
                break;
            }
        case type_record:
        case type_row:
            {
                if (hasReferenceModifier(type))
                    thisSize = sizeof(void *);
                else
                {
                    IHqlExpression * ret = expr->queryRecord()->queryProperty(EPsize);
                    meta.addProperty(expr, EPsize, ret);
                    return ret;
                }
                break;
            }
        case type_dictionary:
        case type_groupedtable:
        case type_table:
            {
                if (expr->hasAttribute(_linkCounted_Atom))
                {
                    thisSize = sizeof(size32_t) + sizeof(byte * *);
                    break;
                }
                IHqlExpression * count = NULL;
                IHqlExpression * size = NULL;
                IHqlExpression * maxLength = NULL;
                IHqlExpression * maxCount = NULL;

                ForEachChild(i, expr)
                {
                    IHqlExpression * attr = expr->queryChild(i);
                    if (attr->isAttribute())
                    {
                        IAtom * name = attr->queryName();
                        if (name == countAtom)
                            count = attr->queryChild(0);
                        else if (name == sizeofAtom)
                            size = attr->queryChild(0);
                        else if (name == maxLengthAtom)
                            maxLength = attr->queryChild(0);
                        else if (name == maxSizeAtom)
                            maxLength = attr->queryChild(0);
                        else if (name == maxCountAtom)
                            maxCount = attr->queryChild(0);
                        else if ((name == choosenAtom) && attr->queryChild(0)->queryValue())
                            maxCount = attr->queryChild(0);
                    }
                }

                IHqlExpression * record = expr->queryRecord();
                IHqlExpression * childRecordSizeExpr = record->queryProperty(EPsize);
                unsigned childExpectedSize = (unsigned)getIntValue(childRecordSizeExpr->queryChild(0));
                unsigned childMinimumSize = (unsigned)getIntValue(childRecordSizeExpr->queryChild(1));
                IHqlExpression * childMaximumSizeExpr = childRecordSizeExpr->queryChild(2);
                unsigned childMaximumSize = (unsigned)getIntValue(childMaximumSizeExpr, UNKNOWN_LENGTH);
                ITypeInfo * sizetType = childMaximumSizeExpr->queryType();
                if (count || size)
                {
                    minSize = 0;
                    if (size && size->queryValue())
                        thisSize = (unsigned)getIntValue(size);
                    else if (count && count->queryValue())
                    {
                        unsigned __int64 num = (unsigned)getIntValue(count);
                        thisSize = truncMaxlength(num * childExpectedSize);
                        minSize = truncMaxlength(num * childMinimumSize);
                        if (childMaximumSize != UNKNOWN_LENGTH)
                            maxSize = truncMaxlength(num * childMaximumSize);
                        else
                            thisMaxSizeExpr.setown(createValue(no_mul, LINK(sizetType), ensureExprType(count, sizetType), LINK(childMaximumSizeExpr)));
                    }
                    else
                    {
                        thisSize = UNKNOWN_LENGTH;
                        if (maxLength)
                            maxSize = (unsigned)getIntValue(maxLength);
                        else if (maxCount)
                        {
                            if (childMaximumSize != UNKNOWN_LENGTH)
                                maxSize = truncMaxlength((unsigned __int64)getIntValue(maxCount) * childMaximumSize);
                            else
                                thisMaxSizeExpr.setown(createValue(no_mul, LINK(sizetType), ensureExprType(maxCount, sizetType), LINK(childMaximumSizeExpr)));
                        }
                        else
                            maxSize = UNKNOWN_LENGTH;
                    }
                }
                else
                {
                    minSize = sizeof(size32_t);
                    if (maxLength)
                        maxSize = (unsigned)getIntValue(maxLength);
                    else if (maxCount)
                    {
                        if (childMaximumSize != UNKNOWN_LENGTH)
                            maxSize = truncMaxlength(sizeof(size32_t) + (unsigned __int64)getIntValue(maxCount) * childMaximumSize);
                        else
                            thisMaxSizeExpr.setown(createValue(no_add, LINK(sizetType), getSizetConstant(sizeof(size32_t)),
                                                    createValue(no_mul, LINK(sizetType), ensureExprType(maxCount, sizetType), LINK(childMaximumSizeExpr))));
                    }
                    else
                        maxSize = UNKNOWN_LENGTH;
                }
                break;
            }
        case type_string:
        case type_data:
        case type_unicode:
        case type_qstring:
        case type_utf8:
            if (thisSize == UNKNOWN_LENGTH)
            {
                minSize = sizeof(size32_t);
                maxSize = getMaxSize(expr);
            }
            break;
        case type_varstring:
            if (thisSize == UNKNOWN_LENGTH)
            {
                minSize = 1;
                maxSize = getMaxSize(expr);
            }
            break;
        case type_varunicode:
            if (thisSize == UNKNOWN_LENGTH)
            {
                minSize = sizeof(UChar);
                maxSize = getMaxSize(expr);
            }
            break;
        case type_set:
            if (thisSize == UNKNOWN_LENGTH)
            {
                minSize = sizeof(size32_t)+sizeof(bool);
                maxSize = getMaxSize(expr);
            }
            break;
        case type_alien:
            {
                IHqlAlienTypeInfo * alien = queryAlienType(type);
                thisSize = alien->getPhysicalTypeSize();
                
                if (thisSize == UNKNOWN_LENGTH)
                {
                    IHqlExpression * lengthAttr = queryUncastExpr(alien->queryLengthFunction());
                    if (lengthAttr->isConstant() && !lengthAttr->isFunction())
                    {
                        OwnedHqlExpr folded = foldHqlExpression(lengthAttr);
                        if (folded->queryValue())
                            thisSize = (unsigned)getIntValue(folded);
                    }
                }
                if (thisSize == UNKNOWN_LENGTH)
                {
                    minSize = 0;
                    IHqlExpression * maxSizeExpr = expr->queryAttribute(maxSizeAtom);
                    if (!maxSizeExpr)
                        maxSizeExpr = expr->queryAttribute(maxLengthAtom);

                    if (maxSizeExpr)
                        maxSize = (unsigned)getIntValue(maxSizeExpr->queryChild(0));
                    else
                        maxSize = alien->getMaxSize();
                }
                break;
            }
        case type_packedint:
            minSize = 1;
            maxSize = (type->queryPromotedType()->getSize()+1);
            thisSize = (maxSize > 2) ? 2 : 1;
            break;
        case type_any:
            minSize = 1;
            maxSize = getMaxSize(expr);
            break;
        default:
            assertex(thisSize != UNKNOWN_LENGTH);
            break;
        }
    }
    if (thisMaxSizeExpr)
        maxSize = UNKNOWN_LENGTH;
    if (thisSize == UNKNOWN_LENGTH)
        thisSize = guessSize(minSize, maxSize);
    else
    {
        if (minSize == UNKNOWN_LENGTH)
            minSize = thisSize;
        if (maxSize == 0)
            maxSize = thisSize;
    }
    if ((thisSize == minSize) && (minSize == maxSize))
    {
        OwnedHqlExpr attr = getFixedSizeAttr(thisSize);
        meta.addProperty(expr, EPsize, attr);
        return attr;
    }

    if (!thisMaxSizeExpr)
        thisMaxSizeExpr.setown((maxSize == UNKNOWN_LENGTH) ? createAttribute(unknownSizeFieldAtom) : getSizetConstant(maxSize));
    OwnedHqlExpr attr = createExprAttribute(_propSize_Atom, getSizetConstant(thisSize), getSizetConstant(minSize), thisMaxSizeExpr.getClear());
    meta.addProperty(expr, EPsize, attr);
    return attr;
}

//no_ifblock
static IHqlExpression * evaluateIfBlockAttrSize(IHqlExpression * expr) 
{ 
    IHqlExpression * size = expr->queryChild(1)->queryProperty(EPsize);
    unsigned averageSize = (unsigned)getIntValue(size->queryChild(0), 0)/2;
    OwnedHqlExpr attr = createExprAttribute(_propSize_Atom, getSizetConstant(averageSize), getSizetConstant(0), LINK(size->queryChild(2)));
    meta.addProperty(expr, EPsize, attr);
    return attr;
}

//no_record
static IHqlExpression * evaluateRecordAttrSize(IHqlExpression * expr) 
{ 
    unsigned __int64 expectedSize = 0;
    unsigned __int64 minimumSize = 0;
    unsigned __int64 maximumSize = 0;
    OwnedHqlExpr maximumSizeExpr;
    OwnedHqlExpr derivedSizeExpr;
    bool hasUnknownMaxSizeField = false;
    BitfieldPacker packer;

    ForEachChild(i, expr)
    {
        IHqlExpression * cur = expr->queryChild(i);
        ITypeInfo * type = cur->queryType();
        if (type && type->getTypeCode() == type_bitfield)
        {
            unsigned thisBitOffset, thisBits;
            if (!packer.checkSpaceAvailable(thisBitOffset, thisBits, type))
            {
                size32_t thisSize = type->queryChildType()->getSize();
                expectedSize += thisSize;
                minimumSize += thisSize;
                maximumSize += thisSize;
            }
        }
        else
        {
            packer.reset();
            IHqlExpression * size = cur->queryProperty(EPsize);
            if (size)
            {
                expectedSize += (size32_t)getIntValue(size->queryChild(0));
                minimumSize += (size32_t)getIntValue(size->queryChild(1));
                IHqlExpression * maxExpr = size->queryChild(2);
                if (maxExpr->queryValue())
                    maximumSize += (size32_t)getIntValue(maxExpr);
                else if (maxExpr->isAttribute())
                {
                    assertex(maxExpr->queryName() == unknownSizeFieldAtom);
                    hasUnknownMaxSizeField = true;
                }
                else
                    extendAdd(maximumSizeExpr, maxExpr);
            }
        }
    }

    if ((minimumSize != maximumSize) || maximumSizeExpr || hasUnknownMaxSizeField)
    {
        IHqlExpression * maxLength = queryAttributeChild(expr, maxLengthAtom, 0);
        if (maxLength)
        {
            if (!hasUnknownMaxSizeField)
            {
                if (maximumSize || !maximumSizeExpr)
                {
                    OwnedHqlExpr maxExpr = getSizetConstant(truncMaxlength(maximumSize));
                    extendAdd(maximumSizeExpr, maxExpr);
                }
                derivedSizeExpr.set(maximumSizeExpr);
            }

            maximumSize = (unsigned)getIntValue(maxLength, UNKNOWN_LENGTH);
            maximumSizeExpr.clear();

            if (derivedSizeExpr)
            {
                //If not a constant then it is derived from the default maxlength, so the explicit maxlength is better
                //otherwise use the minimum value
                if (derivedSizeExpr->queryValue())
                {
                    unsigned maxDerived = (unsigned)getIntValue(derivedSizeExpr);
                    if (maximumSize > maxDerived)
                        maximumSize = maxDerived;
                }
            }
        }
        else if (hasUnknownMaxSizeField)
        {
            OwnedHqlExpr maxExpr = getSizetConstant(truncMaxlength(maximumSize));
            extendAdd(maximumSizeExpr, maxExpr);
            maximumSize = 0;

            //?create an expression to represent
            // if (totalSize * 2 > defaultMaxRecordSize, totalSize + defaultMaxRecordSize / 2, defaultMaxRecordSize);

            IHqlExpression * defaultMaxLength = queryDefaultMaxRecordLengthExpr();
            OwnedHqlExpr minmax = LINK(maximumSizeExpr);
            OwnedHqlExpr minMaxTimes2 = createValue(no_mul, defaultMaxLength->getType(), LINK(minmax), getSizetConstant(2));
            OwnedHqlExpr cond = createBoolExpr(no_gt, LINK(minMaxTimes2), LINK(defaultMaxLength));
            OwnedHqlExpr trueExpr = createValue(no_add, defaultMaxLength->getType(), LINK(minmax), createValue(no_div, defaultMaxLength->getType(), LINK(defaultMaxLength), getSizetConstant(2)));
            maximumSizeExpr.setown(createValue(no_if, defaultMaxLength->getType(), LINK(cond), LINK(trueExpr), LINK(defaultMaxLength)));
        }
    }

    if ((maximumSize == 0) && expr->hasAttribute(_nonEmpty_Atom))
    {
        expectedSize = 1;
        minimumSize = 1;
        maximumSize = 1;
    }

    if (maximumSize || !maximumSizeExpr)
    {
        OwnedHqlExpr maxExpr = getSizetConstant(truncMaxlength(maximumSize));
        extendAdd(maximumSizeExpr, maxExpr);
    }

    HqlExprArray args;
    args.append(*getSizetConstant(truncMaxlength(expectedSize)));
    args.append(*getSizetConstant(truncMaxlength(minimumSize)));
    args.append(*LINK(maximumSizeExpr));
    if (derivedSizeExpr)
        args.append(*LINK(derivedSizeExpr));

    OwnedHqlExpr sizeAttr = createExprAttribute(_propSize_Atom, args);
    meta.addProperty(expr, EPsize, sizeAttr);
    return sizeAttr;
}


static IHqlExpression * evalautePropSize(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_field:
        return evaluateFieldAttrSize(expr);
    case no_ifblock:
        return evaluateIfBlockAttrSize(expr);
    case no_record:
        return evaluateRecordAttrSize(expr);
    case no_transform:
    case no_newtransform:
        {
            //MORE: This could calculate a better estimate for the size of the record by taking into account any constant values or datasets that are assigned.
            IHqlExpression * record = expr->queryRecord();
            IHqlExpression * recordSize = record->queryProperty(EPsize);
            meta.addProperty(expr, EPsize, recordSize);
            return recordSize;
        }
    }
    IHqlExpression * record = expr->queryRecord();
    if (record)
        return record->queryProperty(EPsize);
    return NULL;
}


IHqlExpression * getSerializedForm(IHqlExpression * expr, IAtom * variation)
{
    return LINK(querySerializedForm(expr, variation));
}

ITypeInfo * getSerializedForm(ITypeInfo * type, IAtom * variation)
{
    if (!type)
        return NULL;
    switch (type->getTypeCode())
    {
    case type_record:
        {
            IHqlExpression * record = queryRecord(queryUnqualifiedType(type));
            IHqlExpression * serializedRecord = querySerializedForm(record, variation);
            if (record == serializedRecord)
                return LINK(type);
            return cloneModifiers(type, serializedRecord->queryType());
        }
    case type_row:
    case type_transform:
    case type_table:
    case type_groupedtable:
        {
            //MORE: If (variant == internalAtom) consider using a format that prefixes the dataset with a count instead of a size
            OwnedITypeInfo noOutOfLineType = removeModifier(type, typemod_outofline);
            OwnedITypeInfo noLinkCountType = removeAttribute(noOutOfLineType, _linkCounted_Atom);
            ITypeInfo * childType = noLinkCountType->queryChildType();
            OwnedITypeInfo newChild = getSerializedForm(childType, variation);
            return replaceChildType(noLinkCountType, newChild);
        }
    case type_dictionary:
        {
            OwnedITypeInfo noOutOfLineType = removeModifier(type, typemod_outofline);
            OwnedITypeInfo noLinkCountType = removeAttribute(noOutOfLineType, _linkCounted_Atom);
            ITypeInfo * childType = noLinkCountType->queryChildType();
            OwnedITypeInfo newChild = getSerializedForm(childType, variation);
            if (variation == internalAtom)
                return replaceChildType(noLinkCountType, newChild);
            OwnedITypeInfo datasetType = makeTableType(LINK(newChild));
            return cloneModifiers(noLinkCountType, datasetType);
        }
    }
    return LINK(type);
}

//-- Attribute: unadorned form (no annotations) -------------------------------------------------------------------------------

//Use a transformer to implement the mapping - since it contains the logic for processing types etc, but use the attributes as an extra cache.

class HqlCachedPropertyTransformer : public QuickHqlTransformer
{
public:
    HqlCachedPropertyTransformer(HqlTransformerInfo & _transformInfo, ExprPropKind _propKind);

    virtual IHqlExpression * transform(IHqlExpression * expr);

protected:
    ExprPropKind propKind;
};

HqlCachedPropertyTransformer::HqlCachedPropertyTransformer(HqlTransformerInfo & _transformInfo, ExprPropKind _propKind)
: QuickHqlTransformer(_transformInfo, NULL), propKind(_propKind)
{
}


IHqlExpression * HqlCachedPropertyTransformer::transform(IHqlExpression * expr)
{
    IInterface * match = meta.queryExistingProperty(expr, propKind);
    if (match)
        return static_cast<IHqlExpression *>(LINK(match));

    OwnedHqlExpr transformed = QuickHqlTransformer::transform(expr);

    if (transformed != expr)
    {
        //Tag serialized form so don't re-evaluate
        meta.addProperty(transformed, propKind, transformed);
    }

    meta.addProperty(expr, propKind, transformed);

    return transformed.getClear();
}


class HqlUnadornedNormalizer : public HqlCachedPropertyTransformer
{
public:
    HqlUnadornedNormalizer();

    virtual IHqlExpression * createTransformed(IHqlExpression * expr);
    virtual ITypeInfo * transformType(ITypeInfo * type);
};

static HqlTransformerInfo hqlUnadornedInfo("HqlUnadornedNormalizer");
HqlUnadornedNormalizer::HqlUnadornedNormalizer() : HqlCachedPropertyTransformer(hqlUnadornedInfo, EPunadorned)
{
}

ITypeInfo * HqlUnadornedNormalizer::transformType(ITypeInfo * type)
{
    return HqlCachedPropertyTransformer::transformType(queryUnqualifiedType(type));
}

IHqlExpression * HqlUnadornedNormalizer::createTransformed(IHqlExpression * expr)
{
    IHqlExpression * body = expr->queryBody(false);
    if (expr != body)
        return transform(body);

    node_operator op = expr->getOperator();
    switch (op)
    {
    case no_field:
        {
            //Remove the default values...
            HqlExprArray children;
            bool same = true;
            ForEachChild(idx, expr)
            {
                IHqlExpression * cur = expr->queryChild(idx);
                if (cur->isAttribute())
                {
                    IHqlExpression * mapped = transform(cur);
                    children.append(*mapped);
                    if (mapped != cur)
                        same = false;
                }
                else
                    same = false;
            }

            ITypeInfo * type = expr->queryType();
            OwnedITypeInfo newType = transformType(type);
            IIdAtom * id = expr->queryId();
            //Fields names compare case-insignificantly therefore the field name is converted to lower case so that
            //equivalent fields are mapped to the same normalized expression.
            IIdAtom * newid = createIdAtom(str(lower(id)));
            if ((type != newType) || (id != newid))
                return createField(newid, newType.getClear(), children);

            if (same)
                return LINK(expr);
            return expr->clone(children);
        }
    case no_param:
        {
            ITypeInfo * type = expr->queryType();
            OwnedITypeInfo newType = transformType(type);
            HqlExprArray children;
            transformChildren(expr, children);      // could just unwind
            return createParameter(expr->queryId(), UnadornedParameterIndex, newType.getClear(), children);
        }
    }

    return HqlCachedPropertyTransformer::createTransformed(expr);
}

static IHqlExpression * evalautePropUnadorned(IHqlExpression * expr)
{
    HqlUnadornedNormalizer normalizer;
    //NB: Also has the side-effect of adding any missing attributes
    OwnedHqlExpr dummy = normalizer.transform(expr);
    return static_cast<IHqlExpression *>(meta.queryExistingProperty(expr, EPunadorned));
}

//---------------------------------------------------------------------------------

static unsigned queryFieldAlignment(ITypeInfo * type)
{
    unsigned size = type->getSize();
    type_t tc = type->getTypeCode();
    switch (tc)
    {
    case type_int:
    case type_swapint:
        switch (size)
        {
        case 2:
        case 4:
        case 8:
            return size;
        }
        return 1;
    case type_boolean:
    case type_real:
        return size;
    case type_string:
    case type_varstring:
    case type_data:
        if (hasLinkCountedModifier(type))
            return sizeof(void *);
        return 1;
    case type_char:
    case type_decimal:
    case type_packedint:
    case type_set:                  // would be nicer if properly aligned.  Even nicer if the (isall,size) were separate
    case type_utf8:
    case type_qstring:
    case type_any:
        return 1;
    case type_bitfield:
    case type_array:
    case type_enumerated:
        return queryFieldAlignment(type->queryChildType());
    case type_pointer:
        return sizeof(void *);
    case type_table:
    case type_row:
    case type_groupedtable:
        if (hasLinkCountedModifier(type))
            return sizeof(void *);
        return 1;
    case type_alien:
        return queryFieldAlignment(type->queryChildType());
    case type_unicode:
    case type_varunicode:
        return sizeof(UChar);
    default:
        throwUnexpectedType(type);
    }
}

static unsigned queryFieldAlignment(IHqlExpression * field)
{
    return queryFieldAlignment(field->queryType());
}


class FieldAlignCompare : public ICompare
{
public:
    FieldAlignCompare(const HqlExprCopyArray & _original) : original(_original) {}

    virtual int docompare(const void * inleft, const void * inright) const
    {
        IInterface * pleft = (IInterface *)(inleft);
        IInterface * pright = (IInterface *)(inright);
        IHqlExpression * left = static_cast<IHqlExpression *>(pleft);
        IHqlExpression * right = static_cast<IHqlExpression *>(pright);
        IHqlExpression * leftSizeAttr = left->queryProperty(EPsize);
        IHqlExpression * rightSizeAttr = right->queryProperty(EPsize);

        bool leftIsFixedSize = leftSizeAttr->queryChild(1) == leftSizeAttr->queryChild(2);
        bool rightIsFixedSize = rightSizeAttr->queryChild(1) == rightSizeAttr->queryChild(2);
        if (leftIsFixedSize && rightIsFixedSize)
        {
            bool leftIsBitfield = (left->queryType()->getTypeCode() == type_bitfield);
            bool rightIsBitfield = (right->queryType()->getTypeCode() == type_bitfield);
            if (!leftIsBitfield && !rightIsBitfield)
            {
                //First choose the largest alignment first
                unsigned leftAlign = queryFieldAlignment(left);
                unsigned rightAlign = queryFieldAlignment(right);
                if (leftAlign != rightAlign)
                    return (int)(rightAlign - leftAlign);

#if 0
                //Then choose smallest item next - so access is more compact
                unsigned leftSize = getIntValue(leftSizeAttr->queryChild(0));
                unsigned rightSize = getIntValue(rightSizeAttr->queryChild(0));
                if (leftSize != rightSize)
                    return (int)(leftSize - rightSize);
#endif

                //fall through to default 
            }
            else if (!leftIsBitfield)
                return -1;
            else if (!rightIsBitfield)
                return +1;
            else
            {
                //Two bitfields - need better handling
            }
        }
        else if (leftIsFixedSize)
            return -1;
        else if (rightIsFixedSize)
            return +1;
        else
        {
            //both variable size
        }

        //default processing currently by name - may change to use original order
        return original.find(*left) - original.find(*right);
//      return stricmp(left->queryName()->str(), right->queryName()->str());
    }
protected:
    const HqlExprCopyArray & original;
} ;

static bool optimizeFieldOrder(HqlExprArray & out, const HqlExprCopyArray & in)
{
    HqlExprCopyArray sorted;
    appendArray(sorted, in);

    FieldAlignCompare compare(in);
    qsortvec((void * *)sorted.getArray(), sorted.ordinality(), compare);
    ForEachItemIn(i, sorted)
        out.append(OLINK(sorted.item(i)));
    return true;
}

static IHqlExpression * evalautePropAligned(IHqlExpression * expr)
{
    bool same = true;
    HqlExprArray result;
    HqlExprCopyArray reorder;
    assertex(expr->getOperator() == no_record);
    ForEachChild(i, expr)
    {
        IHqlExpression * cur = expr->queryChild(i);
        switch (cur->getOperator())
        {
        case no_field:
            reorder.append(*cur);
            break;
        case no_ifblock:
        case no_record:
        default:
            if (optimizeFieldOrder(result, reorder))
                same = false;
            result.append(*LINK(cur));
            reorder.kill();
            break;
        }
    }
    if (optimizeFieldOrder(result, reorder))
        same = false;
    OwnedHqlExpr newRecord = same ? LINK(expr) : expr->clone(result);
    if (expr == newRecord)
    {
        meta.addProperty(expr, EPaligned, queryAlignedAttr());
        return queryAlignedAttr();
    }
    meta.addProperty(newRecord, EPaligned, queryAlignedAttr());
    
    OwnedHqlExpr alignAttr = createExprAttribute(_propAligned_Atom, newRecord.getClear());
    meta.addProperty(expr, EPaligned, alignAttr);
    return alignAttr;
}

//---------------------------------------------------------------------------------

MODULE_INIT(INIT_PRIORITY_HQLMETA)
{
    for (node_operator op = (node_operator)(no_none+1); op < no_last_op; op = (node_operator)(op+1))
        getOperatorMetaFlags(op);
    return true;
}
MODULE_EXIT()
{
}

//---------------------------------------------------------------------------------

// Functions that provide simple information about an operator, that don't require tree traversal.

bool isLocalActivity(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_distribute:
    case no_keyeddistribute:
    case no_if:
    case no_chooseds:
        return false;
    case no_forcelocal:
    case no_combinegroup:
    case no_regroup:
        return true;
        //local makes no sense for the following
    case no_throughaggregate:
    case no_filter:
    case no_related:
        return false;
    case no_group:
    case no_grouped:
    case no_dedup:
    case no_cogroup:
    case no_cosort:
    case no_sort:
    case no_subsort:
    case no_sorted:
    case no_topn:
    case no_iterate:
    case no_rollup:
    case no_newaggregate:
    case no_merge:
    case no_choosen:
    case no_choosesets:
    case no_enth:
    case no_sample:
    case no_buildindex:
    case no_limit:
    case no_catchds:
    case no_newkeyindex:
    case no_table:
    case no_process:
    case no_assertsorted:
    case no_assertgrouped:
    case no_nonempty:
    case no_loop:
    case no_graphloop:
    case no_aggregate:
    case no_combine:
    case no_denormalize:
    case no_denormalizegroup:
    case no_join:
    case no_mergejoin:      //???
    case no_nwayjoin:
    case no_nwaymerge:
    case no_selfjoin:
    case no_joincount:
        assertex(localChangesActivity(expr));
        return expr->hasAttribute(localAtom);
    case no_newusertable:
        if (isAggregateDataset(expr))
            return expr->hasAttribute(localAtom);
        return false;
    case no_hqlproject:                 // count project may result in distributed output, but not be local(!)
        if (expr->hasAttribute(_countProject_Atom))
            return expr->hasAttribute(localAtom);
        return false;
    case no_compound:
        return isLocalActivity(expr->queryChild(1));
    case no_compound_diskread:
    case no_compound_disknormalize:
    case no_compound_diskaggregate:
    case no_compound_diskcount:
    case no_compound_diskgroupaggregate:
    case no_compound_indexread:
    case no_compound_indexnormalize:
    case no_compound_indexaggregate:
    case no_compound_indexcount:
    case no_compound_indexgroupaggregate:
        {
            if (expr->hasAttribute(localAtom))
                return true;
            IHqlExpression * root = queryRoot(expr);
            while (root->getOperator() == no_select)
            {
                bool isNew;
                IHqlExpression * ds = querySelectorDataset(root, isNew);
                if (!isNew)
                    break;
                root = queryRoot(ds);
            }
            return isLocalActivity(root);
        }
    case no_compound_childread:
    case no_compound_childnormalize:
    case no_compound_childaggregate:
    case no_compound_childcount:
    case no_compound_childgroupaggregate:
    case no_compound_selectnew:
    case no_compound_inline:
        return true;
    case no_dataset_from_transform:
        return expr->hasAttribute(localAtom);
    default:
        {
            assertex(!localChangesActivity(expr));
            return false;
        }
    }
}

bool isGroupedAggregateActivity(IHqlExpression * expr, IHqlExpression * grouping)
{
    if (grouping && !grouping->isAttribute())
        return expr->hasAttribute(groupedAtom);

    return isGrouped(expr->queryChild(0));
}

bool isGroupedActivity(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_group:
    case no_enth:
    case no_distribute:
    case no_fetch:
    case no_keyeddistribute:
    case no_merge:
    case no_graphloop:
        return false;
    case no_denormalize:
    case no_denormalizegroup:
    case no_regroup:
    case no_addfiles:
    case no_join:
    case no_mergejoin:
    case no_nwayjoin:
    case no_nwaymerge:
    case no_selfjoin:
    case no_combine:
    case no_combinegroup:
    case no_if:
    case no_chooseds:
    case no_case:
    case no_map:
    case no_loop:
    case no_choosen:
    case no_process:
    case no_nonempty:
    case no_related:
    case no_pipe:
        return isGrouped(expr->queryType());
    case no_selectfields:
    case no_usertable:
        return isGroupedAggregateActivity(expr, expr->queryChild(2));
    case no_aggregate:
    case no_newaggregate:
    case no_newusertable:
        return isGroupedAggregateActivity(expr, expr->queryChild(3));
    case no_null:
    case no_anon:
    case no_pseudods:
    case no_fail:
    case no_skip:
    case no_all:
    case no_workunit_dataset:
    case no_getgraphresult:
    case no_getgraphloopresult:
    case no_getresult:
    case no_rows:
    case no_internalselect:
    case no_delayedselect:
    case no_unboundselect:
    case no_libraryselect:
    case no_purevirtual:
    case no_libraryinput:
        //All the source activities
        return isGrouped(expr->queryType());
    case no_compound:
        return isGroupedActivity(expr->queryChild(1));
    case no_output:
        return expr->hasAttribute(groupedAtom) && isGroupedActivity(expr->queryChild(0));
    default:
        if (getNumChildTables(expr) == 1)
            return isGrouped(expr->queryChild(0));
        return false;
    }
}

bool localChangesActivityData(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_compound_diskread:
    case no_compound_disknormalize:
    case no_compound_diskaggregate:
    case no_compound_diskcount:
    case no_compound_diskgroupaggregate:
    case no_compound_indexread:
    case no_compound_indexnormalize:
    case no_compound_indexaggregate:
    case no_compound_indexcount:
    case no_compound_indexgroupaggregate:
    case no_newkeyindex:
    case no_table:
        return true;
    case no_denormalize:
    case no_denormalizegroup:
    case no_join:
    case no_joincount:
        return isKeyedJoin(expr);       // keyed join, local means only look at the local key part.
    //case no_fetch:////????
    }
    return false;
}

bool localChangesActivityAction(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_dedup:
    case no_group:
    case no_grouped:
    case no_cogroup:
    case no_cosort:
    case no_sort:
    case no_subsort:
    case no_sorted:
    case no_topn:
    case no_iterate:
    case no_rollup:
    case no_newaggregate:
    case no_aggregate:
    case no_merge:
    case no_choosen:
    case no_choosesets:
    case no_enth:
    case no_sample:
    case no_buildindex:
    case no_limit:
    case no_catchds:
    case no_compound_diskaggregate:
    case no_compound_diskgroupaggregate:
    case no_compound_indexaggregate:
    case no_compound_indexgroupaggregate:
    case no_process:
    case no_assertsorted:
    case no_assertgrouped:
    case no_nonempty:
    case no_loop:
    case no_graphloop:
    case no_combine:
    case no_dataset_from_transform:
        return true;
    case no_hqlproject:
        return expr->hasAttribute(_countProject_Atom);
    case no_newusertable:
        return isAggregateDataset(expr);
    case no_denormalize:
    case no_denormalizegroup:
    case no_join:
    case no_mergejoin:      //???
    case no_nwayjoin:
    case no_nwaymerge:
    case no_selfjoin:
    case no_joincount:
        return !isKeyedJoin(expr);          // Keyed joins always 
    }
    return false;
}

bool localChangesActivity(IHqlExpression * expr)
{
    return localChangesActivityData(expr) || localChangesActivityAction(expr);
}

unsigned isStreamingActivity(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_sort:
    case no_topn:
        if (isGrouped(expr))
            return 0;
        return 1;
    case no_join:
    case no_denormalize:
        if (isKeyedJoin(expr))
            return 0;
        if (expr->hasAttribute(lookupAtom))
            return 2;
        return 3;       // ok if lhs/rhs are sorted...
    case no_selfjoin:
        return 1;       // ok if sorted.
    case no_dedup:
        if (isGrouped(expr))
            return 0;
        if (expr->hasAttribute(hashAtom) || expr->hasAttribute(allAtom))
            return false;
        break;
    case no_addfiles:
        //if ordered and same item is read by lhs and rhs
//          ordered addfiles?
        break;
    case no_libraryselect:
        //????
        return 1;
    case no_spillgraphresult:
    case no_setgraphresult:
    case no_setgraphloopresult:
        break;      //except for default loop output because likely to be read by a child as a whole
    }
    return 0;
}

// More complex derived information which requires tree traversal.

bool isInlineTrivialDataset(IHqlExpression * expr)
{
    loop
    {
        switch (expr->getOperator())
        {
        case no_selectnth:
            switch (expr->queryChild(1)->getOperator())
            {
            case no_constant:
            case no_counter:
                break;
            default:
                return false;
            }
            expr = expr->queryChild(0);
            break;
        case no_workunit_dataset:
        case no_getresult:
            return !expr->hasAttribute(wuidAtom);
        case no_null:
            return true;
        case no_getgraphresult:
            return !expr->hasAttribute(_distributed_Atom);
        default:
            return false;
        }
    }
}


bool isTrivialDataset(IHqlExpression * expr)
{
    loop
    {
        if (isInlineTrivialDataset(expr))
            return true;

        switch (expr->getOperator())
        {
        case no_translated:
        case no_null:
        case no_temprow:
        case no_projectrow:
        case no_left:
        case no_right:
        case no_id2blob:
        case no_activerow:
        case no_typetransfer:
        case no_rows:
        case no_skip:
        case no_matchattr:
        case no_matchrow:
        case no_libraryinput:
        case no_workunit_dataset:
        case no_activetable:
        case no_top:
            return true;
        case no_select:
            if (!isNewSelector(expr))
                return false;
            if (expr->isDataset())
                return true;
            expr = expr->queryChild(0);
            break;
        case no_selectnth:
        case no_alias:
        case no_sorted:
        case no_distributed:
        case no_grouped:
        case no_preservemeta:
        case no_dataset_alias:
        case no_filter:
            expr = expr->queryChild(0);
            break;
        case no_inlinetable:
            return isConstantDataset(expr);
        default:
            return false;
        }
    }
}

static unsigned estimateRowSize(IHqlExpression * record)
{
    IHqlExpression * size = record->queryProperty(EPsize);
    if (!size || !size->queryChild(2)->queryValue())
        return UNKNOWN_LENGTH;
    return (unsigned)getIntValue(size->queryChild(0));
}


bool reducesRowSize(IHqlExpression * expr)
{
    //More: This should be improved...., but slightly tricky without doing lots more processing.
    IHqlExpression * transform = queryNewColumnProvider(expr);
    IHqlExpression * prevRecord = expr->queryChild(0)->queryRecord();
    unsigned newRowSize = estimateRowSize(transform->queryRecord());
    unsigned prevRowSize = estimateRowSize(prevRecord);
    if ((newRowSize != UNKNOWN_LENGTH) && (prevRowSize != UNKNOWN_LENGTH))
        return newRowSize < prevRowSize;

    IHqlExpression * record = expr->queryRecord();
    if (getFlatFieldCount(record) < getFlatFieldCount(prevRecord))
        return true;
    return false;
}

bool increasesRowSize(IHqlExpression * expr)
{
    IHqlExpression * transform = queryNewColumnProvider(expr);
    IHqlExpression * prevRecord = expr->queryChild(0)->queryRecord();
    unsigned newRowSize = estimateRowSize(transform);
    unsigned prevRowSize = estimateRowSize(prevRecord);
    if ((newRowSize != UNKNOWN_LENGTH) && (prevRowSize != UNKNOWN_LENGTH))
        return newRowSize > prevRowSize;

    IHqlExpression * record = expr->queryRecord();
    if (getFlatFieldCount(record) > getFlatFieldCount(prevRecord))
        return true;
    return false;
}

bool isLimitedDataset(IHqlExpression * expr, bool onFailOnly)
{
    loop
    {
        if (expr->hasAttribute(limitAtom))
            return true;

        switch (expr->getOperator())
        {
        case no_choosen:
        case no_limit:
            if (!onFailOnly || expr->hasAttribute(onFailAtom))
                return true;
            break;
        case no_keyedlimit:
            if (onFailOnly && expr->hasAttribute(onFailAtom))
                return true;
            break;
        case no_table:
        case no_newkeyindex:
            return false;
        default:
            if (getNumChildTables(expr) != 1)
                return false;
            break;
        }
        expr = expr->queryChild(0);
    }
}

bool containsAnyActions(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_comma:
    case no_compound:
    case no_actionlist:
    case no_orderedactionlist:
        {
            ForEachChild(i, expr)
            {
                if (containsAnyActions(expr->queryChild(i)))
                    return true;
            }
            return false;
        }
    case no_setmeta:
        return false;
    default:
        return true;
    }
}

//-- Attribute: record count -------------------------------------------------------------------------------

unsigned getCardinality(IHqlExpression * expr)
{
    loop
    {
        switch (expr->getOperator())
        {
        case no_select:
            expr = expr->queryChild(1);
            break;
        case no_constant:
            return 1;
        case no_field:
            {
                IHqlExpression * cardinality = queryAttributeChild(expr, cardinalityAtom, 0);
                if (cardinality)
                    return (unsigned)getIntValue(cardinality);
            }
            //fall through:
        default:
            return expr->queryType()->getCardinality();
        }
    }
}

bool isSmallGrouping(IHqlExpression * sortlist)
{
    unsigned __int64 totalCardinality = 1;
    unsigned max = sortlist->numChildren();
    for (unsigned idx = 0; idx < max; idx++)
    {
        IHqlExpression * cur = sortlist->queryChild(idx);
        unsigned cardinality = getCardinality(cur);
        if (!cardinality)
            return false;
        totalCardinality *= cardinality;

        //don't use hash aggregation if larger than 100,000 potential elements
        if (totalCardinality >= 100000)
            return false;
    }
    return true;
}



//An estimate of the order of magnitude of the number of rows in a dataset.  See function below for artificial thresholds.
const static unsigned __int64 RCtinyLimit   =       10;
const static unsigned __int64 RCgroupLimit  =     1000;
const static unsigned __int64 RCfewLimit    =   100000;
const static unsigned __int64 RCmemoryLimit = 50000000;

const static unsigned __int64 RCclusterSizeEstimate = 5000;

enum RowCountMagnitude
{
    RCMnone,        // 0
    RCMtiny,        // < 10
    RCMgroup,       // < 1000
    RCMfew,         // < 100,000
    RCMmemory,      // < memory
    RCMdisk,        // who knows?
    RCMunknown
};
const char * const magnitudeText[] = {
    "empty",
    "tiny",
    "group",
    "few",
    "memory",
    "disk",
    "unknown"
};
inline RowCountMagnitude getRowCountMagnitude(__int64 num)
{
    if (num == 0)
        return RCMnone;
    if (num <= RCtinyLimit)
        return RCMtiny;
    if (num <= RCgroupLimit)
        return RCMgroup;
    if (num <= RCfewLimit)
        return RCMfew;
    if (num <= RCmemoryLimit)
        return RCMmemory;
    return RCMdisk;
}

static IHqlExpression * makeConstant(__int64 value)
{
    if ((value >= 0) && (size32_t)value == value)
        return getSizetConstant((size32_t)value);
    return createConstant(value);
}

struct HqlRowCountInfo
{
public:
    HqlRowCountInfo() { setUnknown(RCMnone); }

    void applyChoosen(__int64 limit, bool isLocal);
    void combineAlternatives(const HqlRowCountInfo & other);
    void combineBoth(const HqlRowCountInfo & other);
    bool extractHint(IHqlExpression * hint);
    void limitMin(__int64 value);
    void setEstimate(__int64 n);
    void scaleFixed(__int64 scale);
    void scaleRange(__int64 scale);
    void setMin(__int64 n) { min.setown(makeConstant(n)); }
    void setN(__int64 n);
    void setRange(__int64 low, __int64 high);
    void setUnknown(RowCountMagnitude _magnitude);
    void setMaxMagnitude(RowCountMagnitude _magnitude)
    {
        if (magnitude > _magnitude)
            magnitude = _magnitude;
    }

    IHqlExpression * createRecordCountAttr()
    {
        return createExprAttribute(_propRecordCount_Atom, makeConstant(magnitude), LINK(min), LINK(max));//  , LINK(estimate));
    }

    void extract(IHqlExpression * attr)
    {
        assertex(attr->queryName() == _propRecordCount_Atom);
        magnitude = (RowCountMagnitude)getIntValue(attr->queryChild(0));
        min.set(attr->queryChild(1));
        max.set(attr->queryChild(2));
        //estimate.set(attr->queryChild(3));
    }

    inline void setSingleRow() { setN(1); }

    void getText(StringBuffer & text) const;
    __int64 getMin() const { return getIntValue(min); }
    inline bool isSingleRow() const
    {
        return matchesConstantValue(min, 1) && matchesConstantValue(max, 1);
    }
    inline bool alwaysHasRow() const
    {
        return !matchesConstantValue(min, 0);
    }

public:
    OwnedHqlExpr min;               // Absolute minimum - can't be fewer records
    OwnedHqlExpr max;               // Absolute maximum - can't be more records
    RowCountMagnitude magnitude;    // Expected magnitude.  Normally matches max, but may occasionally diverge,.

    //It might be possible to calculate an estimate of the number of rows, but I'm not sure if it
    //is possible to make it significantly more useful than the magnitude.
//  OwnedHqlExpr estimate;
};


void HqlRowCountInfo::applyChoosen(__int64 limit, bool isLocal)
{
    if (getMin() > limit)
        min.setown(makeConstant(limit));

    __int64 maxLimit = isLocal ? RCclusterSizeEstimate*limit : limit;
    if (getIntValue(max, maxLimit+1) > maxLimit)
        max.setown(makeConstant(maxLimit));
    RowCountMagnitude newMagnitude = getRowCountMagnitude(maxLimit);
    if (magnitude > newMagnitude)
        magnitude = newMagnitude;
}

void HqlRowCountInfo::combineAlternatives(const HqlRowCountInfo & other)
{
    if (other.getMin() < getMin())
        min.set(other.min);

    IValue * maxValue = max->queryValue();
    if (maxValue)
    {
        IValue * otherMaxValue = other.max->queryValue();
        if (!otherMaxValue || (otherMaxValue->getIntValue() > maxValue->getIntValue()))
            max.set(other.max);
    }
    if (magnitude < other.magnitude)
        magnitude = other.magnitude;
}

void HqlRowCountInfo::combineBoth(const HqlRowCountInfo & other)
{
    min.setown(makeConstant(getMin()+other.getMin()));

    IValue * maxValue = max->queryValue();
    IValue * otherMaxValue = other.max->queryValue();
    if (!otherMaxValue)
        max.set(other.max);
    else if (maxValue)
    {
        __int64 newMax = maxValue->getIntValue()+otherMaxValue->getIntValue();
        max.setown(makeConstant(newMax));
    }

    //Appending shouldn't change to a larger magnitude.
    if (magnitude < other.magnitude)
        magnitude = other.magnitude;
}

bool HqlRowCountInfo::extractHint(IHqlExpression * hint)
{
    IHqlExpression * arg = hint->queryChild(0);
    if (!arg)
        return false;
    switch (arg->getOperator())
    {
    case no_constant:
        setN(getIntValue(arg));
        return true;
    case no_rangeto:
        setRange(0, getIntValue(arg->queryChild(0)));
        return true;
    case no_range:
        setRange(getIntValue(arg->queryChild(0)), getIntValue(arg->queryChild(1)));
        return true;
    case no_attr:
        {
            IAtom * name = arg->queryName();
            RowCountMagnitude magnitude = RCMnone;
            if (name == tinyAtom)
                magnitude = RCMtiny;
            else if (name == groupAtom)
                magnitude = RCMgroup;
            else if (name == fewAtom)
                magnitude = RCMfew;
            else if (name == memoryAtom)
                magnitude = RCMmemory;
            if (magnitude != RCMnone)
            {
                setUnknown(magnitude);
                return true;
            }
            break;
        }
    }
    return false;
}

void HqlRowCountInfo::getText(StringBuffer & text) const
{
    min->queryValue()->generateECL(text);
    if (min != max)
    {
        text.append("..");
        if (max->queryValue())
            max->queryValue()->generateECL(text);
        else
            text.append("?");
        text.append("[").append(magnitudeText[magnitude]).append("]");
    }
}

void HqlRowCountInfo::limitMin(__int64 value)
{
    if (getMin() > value)
        min.setown(makeConstant(value));
}


void HqlRowCountInfo::scaleFixed(__int64 scale)
{
    __int64 minValue = getMin();
    __int64 maxValue = getIntValue(max, 0);
    if (maxValue)
    {
        setRange(minValue * scale, maxValue * scale);       // MORE: Worry about 64bit overflow
    }
    else
    {
        setUnknown(RCMdisk);
        setMin(minValue * scale);
    }
}


void HqlRowCountInfo::scaleRange(__int64 scale)
{
    scaleFixed(scale);
    min.setown(makeConstant(0));
}


void HqlRowCountInfo::setEstimate(__int64 n)
{
    magnitude = getRowCountMagnitude(n);
}


void HqlRowCountInfo::setN(__int64 n)
{
    setMin(n);
    max.set(min);
    magnitude = getRowCountMagnitude(n);
}


void HqlRowCountInfo::setRange(__int64 low, __int64 high)
{
    min.setown(makeConstant(low));
    max.setown(makeConstant(high));
    magnitude = getRowCountMagnitude(high);
}

void HqlRowCountInfo::setUnknown(RowCountMagnitude _magnitude)
{
    min.setown(getSizetConstant(0));
    max.setown(getUnknownAttribute());
    magnitude = _magnitude;
}


//MORE: This information should be cached in an attribute, once it is working, and used in more than one place.
void retrieveRowInformation(HqlRowCountInfo & info, IHqlExpression * expr)
{
    IHqlExpression * attr = expr->queryProperty(EPrecordCount);
    info.extract(attr);
}

static void calcIntersectingRowInformation(HqlRowCountInfo & info, IHqlExpression * expr, unsigned firstDs)
{
   retrieveRowInformation(info, expr->queryChild(firstDs));

    ForEachChildFrom(i, expr, firstDs+1)
    {
        IHqlExpression * cur = expr->queryChild(i);
        if (!cur->isAttribute())
        {
            HqlRowCountInfo nextInfo;
            retrieveRowInformation(nextInfo, cur);
            info.combineBoth(nextInfo);
        }
    }
}

//MORE: This would benefit from knowing if the target is hthor/roxie (or a thoir child query) so it could tell if local means
//anything.  The best solution is to annotate the graph with _global_ for thor, or _single_ for the others.  One day....
IHqlExpression * calcRowInformation(IHqlExpression * expr)
{
    HqlRowCountInfo info;
    IHqlExpression * hint = queryHint(expr, outputAtom);
    if (hint && info.extractHint(hint))
        return info.createRecordCountAttr();

    IHqlExpression * ds = expr->queryChild(0);
    node_operator op  = expr->getOperator();
    switch (op)
    {
    case no_nothor:
    case no_thor:
    case no_compound_diskread:
    case no_compound_disknormalize:
    case no_compound_diskaggregate:
    case no_compound_diskcount:
    case no_compound_diskgroupaggregate:
    case no_compound_indexread:
    case no_compound_indexnormalize:
    case no_compound_indexaggregate:
    case no_compound_indexcount:
    case no_compound_indexgroupaggregate:
    case no_compound_childread:
    case no_compound_childnormalize:
    case no_compound_childaggregate:
    case no_compound_childcount:
    case no_compound_childgroupaggregate:
    case no_compound_inline:
    case no_compound_selectnew:
    case no_compound_fetch:
    case no_alias:
    case no_forcelocal:
    case no_distribute:
    case no_distributed:
    case no_preservemeta:
    case no_keyeddistribute:
    case no_sorted:
    case no_stepped:
    case no_assertsorted:
    case no_assertgrouped:
    case no_assertdistributed:
    case no_sort:
    case no_subsort:
    case no_nohoist:
    case no_section:
    case no_sectioninput:
    case no_assert_ds:
    case no_readspill:
    case no_writespill:
    case no_commonspill:
    case no_forcegraph:
    case no_split:
    case no_spill:
    case no_spillgraphresult:
    case no_outofline:
    case no_globalscope:
    case no_throughaggregate:
    case no_alias_scope:
    case no_thisnode:
    case no_preload:
    case no_combine:
    case no_catchds:
    case no_metaactivity:
    case no_cosort:
    case no_serialize:
    case no_deserialize:
    case no_executewhen:
    case no_owned_ds:
    case no_dataset_alias:
    case no_nocombine:
        {
            return getRecordCountInfo(ds);
        }
    case no_allnodes:
        {
            retrieveRowInformation(info, ds);
            info.scaleRange(RCclusterSizeEstimate);
            break;
        }
    case no_limit:
    case no_keyedlimit:
        {
            retrieveRowInformation(info, ds);

            __int64 limit = getIntValue(expr->queryChild(1), 0);
            if ((limit != 0) && !isGrouped(expr))
                info.applyChoosen(limit, isLocalActivity(expr));
            else
                info.limitMin(limit);
            break;
        }
    case no_hqlproject:
    case no_iterate:
        {
            retrieveRowInformation(info, ds);
            if (transformContainsSkip(expr->queryChild(1)))
                info.limitMin(0);
            break;
        }
    case no_fetch:
        {
            retrieveRowInformation(info, expr->queryChild(1));
            if (transformContainsSkip(expr->queryChild(3)))
                info.limitMin(0);
            break;
        }
    case no_dedup:
        {
            retrieveRowInformation(info, ds);
            //Only affect minimum => Grouped, local and non grouped may all reduce to 1
            info.limitMin(1);
            break;
        }
    case no_rollup:
    case no_rollupgroup:
        {
            //rollup on a single row is a single row, rollup on non single may or may not be.
            retrieveRowInformation(info, ds);
            if (transformContainsSkip(queryNewColumnProvider(expr)))
                info.limitMin(0);
            else
                info.limitMin(1);
            break;
        }
    case no_aggregate:
    case no_newaggregate:
    case no_newusertable:
    case no_selectfields:
    case no_usertable:
        {
            retrieveRowInformation(info, ds);
            if (isAggregateDataset(expr))
            {
                IHqlExpression * grouping = queryDatasetGroupBy(expr);
                if (!grouping)
                    grouping = queryGrouping(ds);
                if (grouping)
                {
                    //Either aggregate grouped dataset, or grouping supplied.  Similar semantics.
                    //minimum is 1 unless inputs has minimum of 0
                    info.limitMin(1);
                    if (expr->hasAttribute(fewAtom))
                        info.setMaxMagnitude(RCMfew);
                    else if (isSmallGrouping(grouping))
                        info.setMaxMagnitude(RCMfew);
                }
                else if (isLocalActivity(expr))
                {
                    info.setRange(1, RCclusterSizeEstimate);            // local,ungrouped -> one per node
                }
                else
                    info.setSingleRow();
            }
            else
            {
                if (transformContainsSkip(queryNewColumnProvider(expr)))
                    info.limitMin(0);
            }
            break;      // maybe a project of an aggregate
        }
    case no_selectnth:
    case no_datasetfromrow:
    case no_activerow:
        {
            info.setSingleRow();
            break;
        }
    case no_rows:
        {
            info.setUnknown(RCMgroup);
            break;
        }
    case no_rowsetindex:
    case no_rowsetrange:
        {
            info.setUnknown(RCMmemory);
            break;
        }
    case no_workunit_dataset:
    case no_getgraphresult:
    case no_getgraphloopresult:
    case no_getresult:
        {
            IHqlExpression * attr = expr->queryAttribute(_propRecordCount_Atom);
            if (attr)
                return LINK(attr);
            if (expr->isDatarow() || expr->hasAttribute(rowAtom))
            {
                info.setSingleRow();
            }
            else
            {
                if (expr->hasAttribute(_distributed_Atom))
                    info.setUnknown(RCMdisk);
                else
                    info.setUnknown(RCMfew);
            }
            break;
        }
    case no_table:
    case no_keyindex:
    case no_newkeyindex:
        {
            IHqlExpression * attr = expr->queryAttribute(_propRecordCount_Atom);
            if (attr)
                return LINK(attr);
            if (expr->isDatarow() || expr->hasAttribute(rowAtom))
            {
                info.setSingleRow();
            }
            else
            {
                info.setUnknown(RCMdisk);
                //Allow an annotation on a dataset to specify exact and ranges of counts.
                IHqlExpression * count = queryAttributeChild(expr, countAtom, 0);
                IHqlExpression * maxCount = queryAttributeChild(expr, maxCountAtom, 0);
                IHqlExpression * aveCount = queryAttributeChild(expr, aveAtom, 0);
                if (count)
                    info.setN(getIntValue(count));
                else if (maxCount)
                    info.setRange(0, getIntValue(maxCount));
                else if (aveCount)
                    info.setEstimate(getIntValue(aveCount));
            }
            break;
        }
    case no_filter:
    case no_filtergroup:
    case no_sample:
        {
            retrieveRowInformation(info, ds);
            info.limitMin(0);
            //More sample could potentially reduce the magnitude
            break;
        }
    case no_temptable:
        {
            IHqlExpression * values = expr->queryChild(0);
            if (values->getOperator() == no_recordlist)
                info.setN(values->numChildren());
            else
                info.setUnknown(RCMfew);
            break;
        }
    case no_inlinetable:
        {
            IHqlExpression * transforms = expr->queryChild(0);
            unsigned maxValue = transforms->numChildren();
            unsigned minValue = 0;
            for (unsigned i=0; i < maxValue; i++)
            {
                if (!containsSkip(transforms->queryChild(i)))
                    minValue++;
            }
            info.setRange(minValue, maxValue);
            break;
        }
    case no_dataset_from_transform:
        {
            // only if the count is a constant value
            IHqlExpression * count = expr->queryChild(0);
            IValue * value = count->queryValue();
            if (value)
            {
                IHqlExpression * transform = expr->queryChild(1);
                __int64 maxCount = value->getIntValue();
                if (containsSkip(transform))
                {
                    if (expr->hasAttribute(localAtom))
                        info.setUnknown(RCMunknown);
                    else
                        info.setRange(0, maxCount);
                }
                else
                {
                    if (expr->hasAttribute(localAtom))
                    {
                        info.setUnknown(RCMdisk);
                        info.setMin(maxCount);
                    }
                    else
                        info.setN(maxCount);
                }
            }
            else
                info.setUnknown(RCMdisk);
            // leave it be, if it's a constant expression or a variable
            break;
        }
    case no_null:
        info.setN(expr->isDatarow() ? 1 : 0);
        break;
    case no_fail:
        info.setN(0);
        break;
    case no_if:
        {
            retrieveRowInformation(info, expr->queryChild(1));
            IHqlExpression * rhs = expr->queryChild(2);
            if (rhs)
            {
                HqlRowCountInfo rhsInfo;
                retrieveRowInformation(rhsInfo, rhs);
                info.combineAlternatives(rhsInfo);
            }
            else
            {
                info.min.setown(getSizetConstant(0));
            }
            break;
        }
    case no_nonempty:
        {
            retrieveRowInformation(info, ds);

            //Go through the children so we get a sensible value for the magnitude
            unsigned max = expr->numChildren();
            for (unsigned i=1; i< max; i++)
            {
                if (!isZero(info.min))
                    break;

                IHqlExpression * cur = expr->queryChild(i);
                if (!cur->isAttribute())
                {
                    HqlRowCountInfo nextInfo;
                    retrieveRowInformation(nextInfo, cur);
                    info.min.set(nextInfo.min);
                    info.combineAlternatives(nextInfo);
                }
            }
            break;
        }
    case no_chooseds:
    case no_regroup:
    case no_combinegroup:
    case no_addfiles:
    case no_merge:
        {
            unsigned firstDataset = getFirstActivityArgument(expr);
            calcIntersectingRowInformation(info, expr, firstDataset);
            break;
        }
    case no_choosen:
        {
            retrieveRowInformation(info, ds);

            __int64 choosenLimit = getIntValue(expr->queryChild(1), 0);
            if (choosenLimit == CHOOSEN_ALL_LIMIT)
                info.limitMin(0);   // play safe - could be clever if second value is constant, and min/max known.
            else if ((choosenLimit != 0) && !isGrouped(expr))
                info.applyChoosen(choosenLimit, isLocalActivity(expr));
            else
                info.limitMin(choosenLimit);
        }
        break;
    case no_quantile:
        {
            __int64 parts = getIntValue(expr->queryChild(1), 0);
            if ((parts > 0) && !isGrouped(expr) && !isLocalActivity(expr))
            {
                if (expr->hasAttribute(firstAtom))
                    parts++;
                if (expr->hasAttribute(lastAtom))
                    parts++;

                IHqlExpression * transform = queryNewColumnProvider(expr);
                if (transformContainsSkip(transform) || expr->hasAttribute(dedupAtom))
                    info.setRange(0,parts-1);
                else
                    info.setN(parts-1);
            }
            else
                info.setUnknown(RCMfew);
        }
        break;

    case no_topn:
        {
            retrieveRowInformation(info, ds);

            __int64 choosenLimit = getIntValue(expr->queryChild(2), 0);
            if ((choosenLimit > 0) && !isGrouped(expr))
                info.applyChoosen(choosenLimit, isLocalActivity(expr));
            else
                info.limitMin(choosenLimit);
        }
        break;
    case no_select:
        {
            bool isNew;
            IHqlExpression * realDs = querySelectorDataset(expr, isNew);
            if (isNew)
                retrieveRowInformation(info, realDs);
            else
                info.setSingleRow();

            if (!expr->isDatarow())
            {
                IHqlExpression * field = expr->queryChild(1);
                __int64 count = getIntValue(queryAttributeChild(field, countAtom, 0), 0);
                __int64 maxcount = getIntValue(queryAttributeChild(field, maxCountAtom, 0), 0);
                if (count)
                    info.scaleFixed(count);
                else if (maxcount)
                    info.scaleRange(maxcount);
                else if (info.isSingleRow())
                    info.setUnknown(RCMfew);
                else
                    info.setUnknown(RCMdisk);
            }
            break;
        }
    case no_normalize:
        {
            retrieveRowInformation(info, ds);
            IValue * numRows = expr->queryChild(1)->queryValue();
            if (numRows)
            {
                __int64 scale = numRows->getIntValue();
                if (containsSkip(expr->queryChild(2)))
                    info.scaleRange(scale);
                else
                    info.scaleFixed(scale);
            }
            else
                info.setUnknown(RCMdisk);
            break;
        }
    case no_group:
    case no_grouped:
        //MORE: Not completely sure how we should handle groups.
        return getRecordCountInfo(ds);
    case no_join:
    case no_selfjoin:
        {
            __uint64 maxMatchesPerLeftRow = (__uint64)-1;
            if (expr->hasAttribute(leftonlyAtom))
                maxMatchesPerLeftRow = 1;
            else if (isLeftJoin(expr) || isInnerJoin(expr))
            {
                //MORE: Could process other small scalings e.g., < 10 from keep/atmost attributes
                IHqlExpression * keep = queryAttributeChild(expr, keepAtom, 0);
                if (matchesConstantValue(keep, 1))
                    maxMatchesPerLeftRow = 1;
                IHqlExpression * atmost = queryAttributeChild(expr, atmostAtom, 0);
                if (matchesConstantValue(atmost, 1))
                    maxMatchesPerLeftRow = 1;
            }
            if (maxMatchesPerLeftRow == 1)
            {
                retrieveRowInformation(info, ds);
                if (!expr->hasAttribute(leftouterAtom) || containsSkip(expr->queryChild(3)))
                    info.limitMin(0);
            }
            else
                info.setUnknown(RCMdisk);
            break;
        }
    case no_denormalize:
    case no_denormalizegroup:
        {
            retrieveRowInformation(info, ds);
            if (containsSkip(expr->queryChild(3)))
                info.limitMin(0);
            break;
        }
    case no_mergejoin:
    case no_nwayjoin:
    case no_nwaymerge:
        info.setUnknown(RCMdisk);
        break;
    case no_loop:
    case no_graphloop:
    case no_libraryselect:
    case no_libraryinput:
    case no_param:
    case no_anon:
    case no_nofold:             // assume nothing - to stop subsequent optimizations
    case no_delayedselect:
    case no_unboundselect:
    case no_internalselect:
        info.setUnknown(RCMdisk);
        break;
    case no_parse:
    case no_newparse:
    case no_xmlparse:
    case no_newxmlparse:
    case no_soapcall:
    case no_soapcall_ds:
    case no_newsoapcall:
    case no_newsoapcall_ds:
    case no_httpcall:
    case no_process:
    case no_pipe:
    case no_translated:
    case no_datasetfromdictionary:
        //MORE could improve each of these
        info.setUnknown(RCMdisk);
        break;
    case no_map:
    case no_case:
        {
            if (expr->isDatarow())
            {
                info.setSingleRow();
                break;
            }

            //This is primarily implemented so the annotations in the graph look correct
            unsigned start = (op == no_case) ? 1 : 0;
            IHqlExpression * dft = NULL;
            ForEachChildFrom(i1, expr, start)
            {
                IHqlExpression * cur = expr->queryChild(i1);
                if (cur->getOperator() != no_mapto)
                {
                    if (!cur->isAttribute())
                        dft = cur;
                    break;
                }
            }
            if (dft)
                retrieveRowInformation(info, dft);
            else
                info.setN(0);

            ForEachChildFrom(i2, expr, start)
            {
                IHqlExpression * cur = expr->queryChild(i2);
                if (cur->getOperator() == no_mapto)
                {
                    HqlRowCountInfo rhsInfo;
                    retrieveRowInformation(rhsInfo, cur->queryChild(1));
                    info.combineAlternatives(rhsInfo);
                }
            }
            break;
        }
    case no_id2blob:
    case no_xmlproject:
    case no_call:
    case no_externalcall:
        info.setUnknown(RCMfew);
        break;
    case no_colon:
        {
            IHqlExpression * workflow = expr->queryChild(1);
            //For either of 
            if (queryOperatorInList(no_stored, workflow) || queryOperatorInList(no_recovery, workflow))
            {
                info.setUnknown(RCMdisk);
                break;
            }
            //MORE: Could restrict based on few flags
            return getRecordCountInfo(ds);
        }
    case no_choosesets:
    case no_enth:
        //MORE: Could sum the numbers to return
        return getRecordCountInfo(ds);
    case no_compound:
        return getRecordCountInfo(expr->queryChild(1));
    default:
        if (expr->isDataset())
            UNIMPLEMENTED_XY("Record count calculation for operator", getOpString(op));
        if (expr->isDatarow())
            info.setSingleRow();
        else
            info.setUnknown(RCMdisk);               //Assume the worse case...
        break;
    }

    return info.createRecordCountAttr();
}

static IHqlExpression * evalautePropRecordCount(IHqlExpression * expr)
{
    OwnedHqlExpr info = calcRowInformation(expr);
    meta.addProperty(expr, EPrecordCount, info);
    return info;
}


void getRecordCountText(StringBuffer & result, IHqlExpression * expr)
{
    HqlRowCountInfo info;
    retrieveRowInformation(info, expr);
    info.getText(result);
}

//---------------------------------------------------------------------------------


bool hasFewRows(IHqlExpression * expr)
{
    HqlRowCountInfo info;
    retrieveRowInformation(info, expr);
    return (info.magnitude <= RCMfew);
}

bool spillToWorkunitNotFile(IHqlExpression * expr, ClusterType platform)
{
    if (platform == RoxieCluster)
        return true;

    if (isThorCluster(platform))
    {
        //In thor, all rows will get sent to master and written to dali, and then read back on slave 0
        //not likely to be more efficient unless only a single row - although the generated code accessing
        //from a child query is better
        return hasNoMoreRowsThan(expr, 1);
    }
    return hasFewRows(expr);
}

bool hasSingleRow(IHqlExpression * expr)
{
    HqlRowCountInfo info;
    retrieveRowInformation(info, expr);
    return info.isSingleRow();
}

bool hasNoMoreRowsThan(IHqlExpression * expr, __int64 limit)
{
    HqlRowCountInfo info;
    retrieveRowInformation(info, expr);
    return getIntValue(info.max, limit+1) <= limit;
}


// Functions for testing whether 

// Functions for accessing attributes from types etc.

IHqlExpression * queryAttribute(ITypeInfo * type, IAtom * search)
{
    loop
    {
        typemod_t curModifier = type->queryModifier();
        switch (curModifier)
        {
        case typemod_none:
            return NULL;
        case typemod_attr:
            {
                IHqlExpression * prop = static_cast<IHqlExpression *>(type->queryModifierExtra());
                if (prop->queryName() == search)
                    return prop;
                break;
            }
        case typemod_original:
            {
                IHqlExpression * original = static_cast<IHqlExpression *>(type->queryModifierExtra());
                IHqlExpression * match = original->queryAttribute(search);
                if (match)
                    return match;
                break;
            }
        }
        type = type->queryTypeBase();
    }
}

IHqlExpression * queryAttributeChild(ITypeInfo * type, IAtom * search, unsigned idx)
{
    IHqlExpression * match = queryAttribute(type, search);
    if (match)
        return match->queryChild(idx);
    return NULL;
}


// Functions for extracting and preserving attribute information on types and fields.

void cloneFieldModifier(Shared<ITypeInfo> & type, ITypeInfo * donorType, IAtom * attr)
{
    IHqlExpression * match = queryAttribute(donorType, attr);
    if (!match)
        return;
    IHqlExpression * existing = queryAttribute(type, attr);
    if (match == existing)
        return;
    type.setown(makeAttributeModifier(type.getClear(), LINK(match)));
}

ITypeInfo * cloneEssentialFieldModifiers(ITypeInfo * donor, ITypeInfo * rawtype)
{
    Linked<ITypeInfo> type = rawtype;
    cloneFieldModifier(type, donor, maxLengthAtom);
    cloneFieldModifier(type, donor, maxSizeAtom);
    cloneFieldModifier(type, donor, maxCountAtom);
    return type.getClear();
}

ITypeInfo * removeAttribute(ITypeInfo * t, IAtom * search)
{
    typemod_t curModifier = t->queryModifier();
    if (curModifier == typemod_none)
        return LINK(t);

    ITypeInfo * base = t->queryTypeBase();
    if (curModifier == typemod_attr)
    {
        IHqlExpression * attr = (IHqlExpression *)t->queryModifierExtra();
        if (attr->queryName() == search)
            return LINK(base);
    }

    OwnedITypeInfo newBase = removeAttribute(base, search);
    if (newBase == base)
        return LINK(t);
    return makeModifier(newBase.getClear(), curModifier, LINK(t->queryModifierExtra()));
}

bool isUninheritedFieldAttribute(IHqlExpression * expr)
{
    if (expr->isAttribute())
    {
        IAtom * name = expr->queryName();
        //MORE: Attributes of datasets need a different representation - should probably be include in the type somehow...
        if ((name == virtualAtom) || (name == countAtom))
            return true;
    }
    return false;
}


bool hasUninheritedAttribute(IHqlExpression * field)
{
    ForEachChild(i, field)
        if (isUninheritedFieldAttribute(field->queryChild(i)))
            return true;
    return false;
}


IHqlExpression * extractFieldAttrs(IHqlExpression * field)
{
    IHqlExpression * attrs = NULL;
    ForEachChild(idx, field)
    {
        IHqlExpression * child = field->queryChild(idx);
        if (child->isAttribute())
        {
            //MORE: Attributes of datasets need a different representation - should probably be include in the type somehow...
            if (!isUninheritedFieldAttribute(child))
            {
                // which others should we ignore?
                attrs = createComma(attrs, LINK(child));
            }
        }
    }
    return attrs;
}


IHqlExpression * extractAttrsFromExpr(IHqlExpression * value)
{
    if (!value)
        return NULL;
    if (value->getOperator() == no_select)
        value = value->queryChild(1);
    if (value->getOperator() == no_field)
        return extractFieldAttrs(value);
    return NULL;
}


// Type processing
ITypeInfo * getPromotedECLType(ITypeInfo * lType, ITypeInfo * rType)
{
    return ::getPromotedType(lType, rType);
}

ITypeInfo * getPromotedECLCompareType(ITypeInfo * lType, ITypeInfo * rType)
{
    return ::getPromotedCompareType(lType, rType);
}


unsigned getMaxRecordSize(IHqlExpression * record, unsigned defaultMaxRecordSize, bool & hasKnownSize, bool & usedDefault)
{
    IHqlExpression * size = record->queryProperty(EPsize);
    IHqlExpression * minSizeExpr = size->queryChild(1);
    IHqlExpression * maxSizeExpr = size->queryChild(2);
    unsigned maxSize = (unsigned)getIntValue(maxSizeExpr, UNKNOWN_LENGTH);
    hasKnownSize = (minSizeExpr == maxSizeExpr);
    if (maxSize == UNKNOWN_LENGTH)
    {
        OwnedHqlExpr defaultExpr = getSizetConstant(defaultMaxRecordSize);
        OwnedHqlExpr value = replaceExpression(maxSizeExpr, queryDefaultMaxRecordLengthExpr(), defaultExpr);
        OwnedHqlExpr folded = foldHqlExpression(value);
        assertex(folded);
        maxSize = (unsigned)getIntValue(folded);
        unsigned minSize = (unsigned)getIntValue(minSizeExpr);
        if (maxSize < minSize)
            maxSize = minSize;
        usedDefault = true;
    }
    else
        usedDefault = false;
    return maxSize;
}

size32_t getExpectedRecordSize(IHqlExpression * record)
{
    IHqlExpression * size = record->queryProperty(EPsize);
    return size ? (size32_t)getIntValue(size->queryChild(0)) : 0;
}

size32_t getMinRecordSize(IHqlExpression * record)
{
    IHqlExpression * size = record->queryProperty(EPsize);
    return size ? (size32_t)getIntValue(size->queryChild(1)) : 0;
}

unsigned getMaxRecordSize(IHqlExpression * record, unsigned defaultMaxRecordSize)
{
    bool isKnownSize, usedDefault;
    return getMaxRecordSize(record, defaultMaxRecordSize, isKnownSize, usedDefault);
}

bool maxRecordSizeUsesDefault(IHqlExpression * record)
{
    IHqlExpression * maxSize = record->queryProperty(EPsize)->queryChild(2);
    return (maxSize->queryValue() == NULL);
}

bool isVariableSizeRecord(IHqlExpression * record)
{
    IHqlExpression * sizeAttr = record->queryProperty(EPsize);
    return sizeAttr->queryChild(1) != sizeAttr->queryChild(2);
}

bool maxRecordSizeIsAmbiguous(IHqlExpression * record, size32_t & specifiedSize, size32_t & derivedSize)
{
    IHqlExpression * sizeAttr = record->queryProperty(EPsize);
    IHqlExpression * derivedSizeExpr = sizeAttr->queryChild(3);
    if (!derivedSizeExpr || !derivedSizeExpr->isConstant())
        return false;
    OwnedHqlExpr foldedDerivedSize = foldHqlExpression(derivedSizeExpr);
    if (!foldedDerivedSize->queryValue())
        return false;

    IHqlExpression * maxLength = sizeAttr->queryChild(2);
    OwnedHqlExpr foldedMaxLength = foldHqlExpression(maxLength);
    if (!foldedMaxLength->queryValue())
        return false;

    specifiedSize = (size32_t)foldedMaxLength->queryValue()->getIntValue();
    derivedSize = (size32_t) foldedDerivedSize->queryValue()->getIntValue();
    return derivedSize != specifiedSize;
}

bool maxRecordSizeCanBeDerived(IHqlExpression * record)
{
    if (!isVariableSizeRecord(record))
        return true;
    if (record->hasAttribute(maxLengthAtom))
    {
        IHqlExpression * sizeAttr = record->queryProperty(EPsize);
        IHqlExpression * derivedSizeExpr = sizeAttr->queryChild(3);
        return (derivedSizeExpr != NULL);
    }
    return !maxRecordSizeUsesDefault(record);
}


//---------------------------------------------------------------------------------


bool recordRequiresSerialization(IHqlExpression * expr, IAtom * serializeForm)
{
    if (!expr)
        return false;

    if (querySerializedForm(expr, serializeForm) != expr)
        return true;

    return false;
}

bool recordRequiresDestructor(IHqlExpression * expr)
{
    if (!expr)
        return false;

    IHqlExpression * body = expr->queryBody();
    //true if the internal serialized form is different
    if (querySerializedForm(body, internalAtom) != body)
        return true;

    return false;
}

bool recordRequiresLinkCount(IHqlExpression * expr)
{
    //MORE: This should strictly speaking check if any of the child fields are link counted.
    //This function is a sufficient proxy at the moment
    return recordRequiresDestructor(expr);
}

bool recordSerializationDiffers(IHqlExpression * expr, IAtom * serializeForm1, IAtom * serializeForm2)
{
    return querySerializedForm(expr, serializeForm1) != querySerializedForm(expr, serializeForm2);
}

extern HQL_API bool typeRequiresDeserialization(ITypeInfo * type, IAtom * serializeForm)
{
    Owned<ITypeInfo> serializedType = getSerializedForm(type, serializeForm);

    if (queryUnqualifiedType(serializedType) == queryUnqualifiedType(type))
        return false;

    type_t stc = serializedType->getTypeCode();
    if (stc != type->getTypeCode())
        return true;

    if (stc == type_table)
    {
        if (recordTypesMatch(serializedType, type))
            return false;
        return true;
    }

    return true;
}

//---------------------------------------------------------------------------------

IHqlExpression * queryRecordCountInfo(IHqlExpression * expr)
{
    return expr->queryProperty(EPrecordCount);
}

IHqlExpression * getRecordCountInfo(IHqlExpression * expr)
{
    return LINK(expr->queryProperty(EPrecordCount));
}

IHqlExpression * queryExpectedRecordCount(IHqlExpression * expr)
{
    IHqlExpression * attr = expr->queryProperty(EPrecordCount);
    return attr ? attr->queryChild(0) : NULL;
}

IHqlExpression * getPackedRecord(IHqlExpression * expr)
{
    IHqlExpression * attr = expr->queryProperty(EPaligned);
    IHqlExpression * packed = attr->queryChild(0);
    if (!packed) packed = expr;
    return LINK(packed);
}

/*
 * This function can be called while parsing (or later) to find a "normalized" version of a record or a field.
 * It ignores default values for fields, and removes named symbols/ other location specific information - so
 * that identical records defined in macros etc. are treated as identical.
 */
IHqlExpression * getUnadornedRecordOrField(IHqlExpression * expr)
{
    if (!expr)
        return NULL;
    IHqlExpression * attr = expr->queryProperty(EPunadorned);
    return LINK(attr);
}

//---------------------------------------------------------------------------------

inline bool isAlwaysLocationIndependent(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_constant:
    case no_param:
    case no_quoted:
    case no_variable:
        return true;
    case no_attr:
        return (expr->numChildren() == 0);
    }
    return false;
}

class HqlLocationIndependentNormalizer : public QuickHqlTransformer
{
public:
    HqlLocationIndependentNormalizer();

    virtual IHqlExpression * createTransformed(IHqlExpression * expr);
    virtual ITypeInfo * transformType(ITypeInfo * type);

protected:
    IHqlExpression * doCreateTransformed(IHqlExpression * expr);
};

static HqlTransformerInfo hqlLocationIndependentInfo("HqlLocationIndependentNormalizer");
HqlLocationIndependentNormalizer::HqlLocationIndependentNormalizer() : QuickHqlTransformer(hqlLocationIndependentInfo, NULL)
{
}

ITypeInfo * HqlLocationIndependentNormalizer::transformType(ITypeInfo * type)
{
    switch (type->queryModifier())
    {
    case typemod_original:
        return transformType(type->queryTypeBase());
    case typemod_none:
        return QuickHqlTransformer::transformType(type);
    case typemod_indirect:
        {
            IHqlExpression * original = static_cast<IHqlExpression *>(type->queryModifierExtra());
            OwnedHqlExpr transformed = transform(original);
            return makeModifier(transformed->getType(), typemod_indirect, LINK(transformed));
        }
    default:
        {
            ITypeInfo * typeBase = type->queryTypeBase();
            Owned<ITypeInfo> newType = transformType(typeBase);
            if (typeBase == newType)
                return LINK(type);
            return cloneModifier(type, newType);
        }
    }
}

IHqlExpression * HqlLocationIndependentNormalizer::doCreateTransformed(IHqlExpression * expr)
{
    node_operator op = expr->getOperator();
    switch (op)
    {
    case no_attr:
        {
            //Original attributes cause chaos => remove all children from attributes
            if (expr->numChildren() != 0)
            {
                IAtom * name = expr->queryName();
                if ((name != _countProject_Atom) && (name != _metadata_Atom))
                    return createAttribute(expr->queryName());
            }
            return LINK(expr);
        }
    case no_field:
        {
            //Remove the default values from fields since they just confuse.
            HqlExprArray children;
            bool same = true;
            ForEachChild(idx, expr)
            {
                IHqlExpression * cur = expr->queryChild(idx);
                if (cur->isAttribute())
                {
                    IHqlExpression * mapped = transform(cur);
                    children.append(*mapped);
                    if (mapped != cur)
                        same = false;
                }
                else
                    same = false;
            }

            ITypeInfo * type = expr->queryType();
            OwnedITypeInfo newType = transformType(type);
            if (type != newType)
                return createField(expr->queryId(), newType.getClear(), children);

            if (same)
                return LINK(expr);
            return expr->clone(children);
        }
    }

    return QuickHqlTransformer::createTransformed(expr);
}


IHqlExpression * HqlLocationIndependentNormalizer::createTransformed(IHqlExpression * expr)
{
    //Remove all annotations.  It is vaguely possible there are some annotations we would want to retain, but I don't know of any
    IHqlExpression * body = expr->queryBody(false);
    if (expr != body)
        return transform(body);

    if (isAlwaysLocationIndependent(expr))
        return LINK(expr);

    IInterface * match = meta.queryExistingProperty(expr, EPlocationIndependent);
    if (match)
        return static_cast<IHqlExpression *>(LINK(match));

    OwnedHqlExpr transformed = doCreateTransformed(expr);

    meta.addProperty(expr, EPlocationIndependent, transformed);

    return transformed.getClear();
}

IHqlExpression * evalautePropLocationIndependent(IHqlExpression * expr)
{
    if (isAlwaysLocationIndependent(expr))
        return expr->queryBody();

    //Because the transformers contain all the logic for how scopes etc. are transformed it is much better to
    //use a transformer which caches the result in the expression tree instead of trying to replicate
    //all the rules in some member functions.
    HqlLocationIndependentNormalizer normalizer;
    OwnedHqlExpr transformed = normalizer.transform(expr);
    return transformed;         // NB: no getClear().  Because it is cached it is guaranteed to exist even when this link is released.
}

IHqlExpression * queryLocationIndependent(IHqlExpression * expr)
{
    IHqlExpression * match = expr->queryProperty(EPlocationIndependent);
    if (match)
        return match;
    return expr;
}


static void cloneAttributeAsModifier(Owned<ITypeInfo> & type, IHqlExpression * donor, IAtom * attr)
{
    if (queryAttribute(type, attr))
        return;
    IHqlExpression * match = donor->queryAttribute(attr);
    if (!match)
        return;
    type.setown(makeAttributeModifier(type.getClear(), LINK(match)));
}


ITypeInfo * preserveTypeQualifiers(ITypeInfo * ownedType, IHqlExpression * donor)
{
    //The following would be a good idea, but it won't work until we introduce a recordof() operator
    //and use that whenever queryRecord() is currenly called (see bug46863)
//  type = makeModifier(type, typemod_indirect, LINK(arg));

    //Instead, just clone the attributes we need
    IHqlExpression * field = queryFieldFromExpr(donor);
    switch (field->getOperator())
    {
    case no_field:
//  case no_record:
        break;
    default:
        return ownedType;
    }

    OwnedITypeInfo type = ownedType;
    cloneAttributeAsModifier(type, field, maxLengthAtom);
    cloneAttributeAsModifier(type, field, maxSizeAtom);
    cloneAttributeAsModifier(type, field, maxCountAtom);
    return type.getClear();
}

static bool cloneModifierAsAttribute(HqlExprArray & args, ITypeInfo * donor, IAtom * attr)
{
    IHqlExpression * match = queryAttribute(donor, attr);
    if (!match)
        return true;
    if (queryAttribute(attr, args))
        return true;
    args.append(*LINK(match));
    return false;
}


bool preserveTypeQualifiers(HqlExprArray & args, ITypeInfo * donor)
{
    bool same = true;
    same = cloneModifierAsAttribute(args, donor, maxLengthAtom) && same;
    same = cloneModifierAsAttribute(args, donor, maxSizeAtom) && same;
    same = cloneModifierAsAttribute(args, donor, maxCountAtom) && same;
    return same;
}

IHqlExpression * preserveTypeQualifiers(IHqlExpression * ownedField, ITypeInfo * donor)
{
    OwnedHqlExpr field = ownedField;
    HqlExprArray args;
    unwindChildren(args, field);
    if (preserveTypeQualifiers(args, donor))
        return field.getClear();
    return field->clone(args);
}

bool isLinkedRowset(ITypeInfo * t)
{
    switch (t->getTypeCode())
    {
    case type_table:
    case type_groupedtable:
    case type_dictionary:
        return hasLinkCountedModifier(t);
    }
    return false;
}

bool isArrayRowset(ITypeInfo * t)
{
    switch (t->getTypeCode())
    {
    case type_table:
    case type_groupedtable:
    case type_array:
    case type_dictionary:
        {
            if (hasStreamedModifier(t))
                return false;
            if (hasLinkCountedModifier(t))
                assertex(hasLinkCountedModifier(t->queryChildType()));
            if (hasOutOfLineModifier(t) || hasLinkCountedModifier(t))
                return true;
            ITypeInfo * rowType = t->queryChildType();
            if (hasOutOfLineModifier(rowType) || hasLinkCountedModifier(rowType))
                throwUnexpected();
            return false;
        }
    case type_row:
        throwUnexpected();
    }
    return false;
}

bool hasLinkedRow(ITypeInfo * t)
{
    switch (t->getTypeCode())
    {
    case type_table:
    case type_groupedtable:
    case type_dictionary:
        return hasLinkedRow(t->queryChildType());
    case type_row:
        return hasLinkCountedModifier(t);
    }
    return false;
}


ITypeInfo * setLinkCountedAttr(ITypeInfo * _type, bool setValue)
{
    Linked<ITypeInfo> type = _type;

    switch (type->getTypeCode())
    {
    case type_table:
    case type_groupedtable:
    case type_dictionary:
        {
            ITypeInfo * rowType = type->queryChildType();
            Owned<ITypeInfo> newRowType = setLinkCountedAttr(rowType, setValue);
            if (rowType != newRowType)
                type.setown(replaceChildType(type, newRowType));
            break;
        }
    case type_row:
        break;
    default:
        return type.getClear();
    }

    if (hasLinkCountedModifier(type))
    {
        if (setValue)
            return LINK(type);
        return removeAttribute(type, _linkCounted_Atom);
    }
    else
    {
        if (setValue)
            return makeAttributeModifier(LINK(type), getLinkCountedAttr());
        return LINK(type);
    }
}

ITypeInfo * setStreamedAttr(ITypeInfo * _type, bool setValue)
{
    Linked<ITypeInfo> type = _type;

    switch (type->getTypeCode())
    {
    case type_groupedtable:
        {
            ITypeInfo * dsType = type->queryChildType();
            Owned<ITypeInfo> newDsType = setStreamedAttr(dsType, setValue);
            if (dsType != newDsType)
                type.setown(replaceChildType(type, newDsType));
            break;
        }
    case type_table:
        break;
    default:
        return type.getClear();
    }

    if (hasStreamedModifier(type))
    {
        if (setValue)
            return LINK(type);
        return removeAttribute(type, streamedAtom);
    }
    else
    {
        if (setValue)
            return makeAttributeModifier(LINK(type), getStreamedAttr());
        return LINK(type);
    }
}


//---------------------------------------------------------------------------------------------------------------------

IInterface * CHqlExpression::queryExistingProperty(ExprPropKind propKind) const
{
    SpinBlock block(*propertyLock);
    CHqlDynamicProperty * cur = attributes;
    while (cur)
    {
        if (cur->kind == propKind)
        {
            IInterface * value = cur->value;
            if (value)
                return value;
            return static_cast<IHqlExpression *>(const_cast<CHqlExpression *>(this));
        }
        cur = cur->next;
    }
    return NULL;
}

void CHqlExpression::addProperty(ExprPropKind kind, IInterface * value)
{
    if (value == static_cast<IHqlExpression *>(this))
        value = NULL;
    CHqlDynamicProperty * attr = new CHqlDynamicProperty(kind, value);

    SpinBlock block(*propertyLock);
    //theoretically we should test if the attribute has already been added by another thread, but in practice there is no
    //problem if the attribute is present twice.
    attr->next = attributes;
    attributes = attr;
}


//A specialised version of addProperty/queryExistingProperty.  Uses sizeof(void*) all the time instead
//of 3*sizeof(void *)+heap overhead when used.
//Possibly saves a bit of time, but more useful as a proof of concept in case it was useful elsewhere..
void CHqlDataset::addProperty(ExprPropKind kind, IInterface * value)
{
    if (kind == EPmeta)
    {
        SpinBlock block(*propertyLock);
        //ensure once meta is set it is never modified
        if (!metaProperty)
            metaProperty.set(value);
    }
    else
        CHqlExpression::addProperty(kind, value);

}

IInterface * CHqlDataset::queryExistingProperty(ExprPropKind kind) const
{
    if (kind == EPmeta)
    {
        SpinBlock block(*propertyLock);
        return metaProperty;
    }

    return CHqlExpression::queryExistingProperty(kind);
}

//---------------------------------------------------------------------------------------------------------------------

IHqlExpression * CHqlExpression::queryProperty(ExprPropKind kind)
{
    IInterface * match = queryExistingProperty(kind);
    if (match)
        return static_cast<IHqlExpression *>(match);

    switch (kind)
    {
    case EPrecordCount:
        return evalautePropRecordCount(this);
    case EPdiskserializedForm:
        return evaluatePropSerializedForm(this, kind, diskAtom);
    case EPinternalserializedForm:
        return evaluatePropSerializedForm(this, kind, internalAtom);
    case EPsize:
        return evalautePropSize(this);
    case EPaligned:
        return evalautePropAligned(this);
    case EPunadorned:
        return evalautePropUnadorned(this);
    case EPlocationIndependent:
        return evalautePropLocationIndependent(this);
    }
    return NULL;
}

CHqlMetaProperty * queryMetaProperty(IHqlExpression * expr)
{
    IHqlExpression * body = expr->queryBody();
    IInterface * match = CHqlExprMeta::queryExistingProperty(body, EPmeta);
    if (match)
        return static_cast<CHqlMetaProperty *>(match);

    CHqlMetaProperty * simple = querySimpleDatasetMeta(body);
    if (simple)
    {
        CHqlExprMeta::addProperty(body, EPmeta, simple);
        return simple;
    }

    Owned<CHqlMetaProperty> info = new CHqlMetaProperty;
    calculateDatasetMeta(info->meta, body);
    CHqlExprMeta::addProperty(body, EPmeta, info);
    return info;
}
