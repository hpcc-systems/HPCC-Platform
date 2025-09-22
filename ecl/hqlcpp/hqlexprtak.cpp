/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2025 HPCC Systems®.

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

#include "platform.h"

#include "jliball.hpp"
#include "hql.hpp"
#include "hqlexpr.hpp"
#include "eclhelper.hpp"
#include "hqlattr.hpp"
#include "hqlatoms.hpp"

#include "hqlexprtak.hpp"

/**
 * Helper function to analyze file format from expression attributes.
 */
node_operator getExpressionFormat(IHqlExpression * expr)
{
    if (!expr)
        return no_none;
    
    IHqlExpression * mode = expr->queryChild(2);
    if (mode)
        return mode->getOperator();
    
    return no_none;
}

/**
 * Maps IHqlExpression instances to ThorActivityKind enum values.
 */
extern ThorActivityKind mapExpressionToActivityKind(IHqlExpression * expr)
{
    if (!expr)
        return TAKnone;
    
    node_operator op = expr->getOperator();
    node_operator format = getExpressionFormat(expr);
    
    // Handle format-specific cases first for common operations
    switch (op)
    {
        case no_table:
        case no_compound_diskread:
        {
            switch (format)
            {
                case no_csv:
                    return TAKcsvread;
                case no_xml:
                    return TAKxmlread;
                case no_json:
                    return TAKjsonread;
                case no_pipe:
                    return TAKpiperead;
                default:
                    return TAKdiskread;
            }
        }
        
        case no_output:
        {
            switch (format)
            {
                case no_csv:
                    return TAKcsvwrite;
                case no_xml:
                    return TAKxmlwrite;
                case no_json:
                    return TAKjsonwrite;
                case no_pipe:
                    return TAKpipewrite;
                default:
                    return TAKdiskwrite;
            }
        }
        
        case no_fetch:
        case no_compound_fetch:
        {
            switch (format)
            {
                case no_csv:
                    return TAKcsvfetch;
                case no_xml:
                    return TAKxmlfetch;
                default:
                    return TAKfetch;
            }
        }
    }
    
    // Now handle the main operator-based mapping with context considerations
    switch (op)
    {
        // Dataset operations
        case no_temptable:
            return TAKinlinetable;
        
        case no_inlinetable:
            return TAKinlinetable;
        
        case no_workunit_dataset:
            return TAKworkunitread;
        
        case no_null:
        {
            // Context-dependent: could be dataset or action
            if (expr->isAction())
                return TAKemptyaction;
            return TAKnull;
        }
        
        // Index operations
        case no_keyindex:
        case no_newkeyindex:
        case no_compound_indexread:
            return TAKindexread;
        
        case no_compound_indexaggregate:
            return TAKindexaggregate;
        
        case no_compound_indexcount:
            return TAKindexcount;
        
        case no_compound_indexgroupaggregate:
            return TAKindexgroupaggregate;
        
        case no_compound_indexnormalize:
            return TAKindexnormalize;
        
        // Child dataset operations
        case no_compound_childread:
            return TAKchildread;
        
        case no_compound_childnormalize:
            return TAKchildnormalize;
        
        case no_compound_childaggregate:
            return TAKchildaggregate;
        
        case no_compound_childcount:
            return TAKchildcount;
        
        case no_compound_childgroupaggregate:
            return TAKchildgroupaggregate;
        
        // Disk compound operations
        case no_compound_disknormalize:
            return TAKdisknormalize;
        
        case no_compound_diskaggregate:
            return TAKdiskaggregate;
        
        case no_compound_diskcount:
            return TAKdiskcount;
        
        case no_compound_diskgroupaggregate:
            return TAKdiskgroupaggregate;
        
        // Join operations - check for optimization attributes
        case no_join:
        {
            if (expr->hasAttribute(keyedAtom))
                return TAKkeyedjoin;
            if (expr->hasAttribute(lookupAtom))
                return TAKlookupjoin;
            if (expr->hasAttribute(hashAtom))
                return TAKhashjoin;
            if (expr->hasAttribute(allAtom))
                return TAKalljoin;
            return TAKjoin;  // Default join
        }
        
        case no_selfjoin:
        {
            if (expr->hasAttribute(lightweightAtom))
                return TAKselfjoinlight;
            return TAKselfjoin;
        }
        
        case no_denormalize:
        {
            if (expr->hasAttribute(keyedAtom))
                return TAKkeyeddenormalize;
            if (expr->hasAttribute(lookupAtom))
                return TAKlookupdenormalize;
            if (expr->hasAttribute(hashAtom))
                return TAKhashdenormalize;
            if (expr->hasAttribute(allAtom))
                return TAKalldenormalize;
            return TAKdenormalize;
        }
        
        case no_denormalizegroup:
            return TAKdenormalizegroup;
        
        case no_mergejoin:
        case no_nwayjoin:
            return TAKnwayjoin;
        
        case no_nwaymerge:
            return TAKnwaymerge;
        
        // Aggregation operations - check for optimization hints
        case no_aggregate:
        case no_newaggregate:
        {
            if (expr->hasAttribute(hashAtom))
                return TAKhashaggregate;
            // Check if it's a count-only aggregate
            if (expr->numChildren() >= 2)
            {
                IHqlExpression * transform = expr->queryChild(1);
                if (transform->getOperator() == no_newtransform)
                {
                    // If transform only has COUNT operations, use count aggregate;
                    // If transform only has EXISTS operations, use exists aggregate
                    bool onlyCount = true;
                    bool onlyExists = true;
                    ForEachChild(i, transform)
                    {
                        IHqlExpression * assignment = transform->queryChild(i);
                        if (assignment->getOperator() == no_assign)
                        {
                            IHqlExpression * value = assignment->queryChild(1);
                            if (value)
                            {
                                onlyCount = onlyCount && (value->getOperator() == no_countgroup);
                                onlyExists = onlyExists && (value->getOperator() == no_existsgroup);
                                // Is it worth testing to see if we can break out of the loop early?
                            }
                        }
                    }
                    if (onlyCount)
                    {
                        return TAKcountaggregate;
                    }
                    if (onlyExists)
                    {
                        return TAKexistsaggregate;
                    }
                }
            }
            return TAKaggregate;
        }
        
        case no_throughaggregate:
            return TAKthroughaggregate;
        
        // Sorting operations - check for optimization attributes
        case no_sort:
        case no_cosort:
            return TAKsort;
        
        case no_topn:
            return TAKtopn;
        
        case no_assertsorted:
            return TAKsorted;
        
        case no_subsort:
            return TAKsubsort;
        
        // Grouping operations - check for optimization attributes  
        case no_group:
        case no_cogroup:
            return TAKgroup;
        
        case no_assertgrouped:
            return TAKgrouped;
        
        // Deduplication - check for optimization attributes
        case no_dedup:
        {
            if (expr->hasAttribute(hashAtom) || expr->hasAttribute(allAtom))
                return TAKhashdedup;
            return TAKdedup;
        }
        
        // Distribution operations
        case no_distribute:
        case no_assertdistributed:
            return TAKhashdistribute;
        
        case no_nwaydistribute:
            return TAKnwaydistribute;
        
        case no_keyeddistribute:
            return TAKkeyeddistribute;
        
        // Projection operations
        case no_hqlproject:
        case no_transformascii:
        case no_transformebcdic:
        case no_projectrow:
        {
            if (expr->hasAttribute(prefetchAtom))
                return TAKprefetchproject;
            if (expr->hasAttribute(_countProject_Atom))
                return TAKcountproject;
            return TAKproject;
        }
        
        case no_usertable:
        case no_newusertable:
            return TAKproject;
        
        // Normalization operations
        case no_normalize:
            return TAKnormalize;
        
        // Row operations
        case no_createrow:
            return TAKinlinetable;
        
        // Iteration and processing
        case no_iterate:
            return TAKiterate;
        
        case no_rollup:
            return TAKrollup;
        
        case no_rollupgroup:
            return TAKrollupgroup;
        
        // Sampling and selection
        case no_choosen:
            return TAKfirstn;
        
        case no_choosesets:
            return TAKchoosesets;
        
        case no_enth:
            return TAKenth;
        
        case no_sample:
            return TAKsample;
        
        case no_index:
        case no_selectnth:
            return TAKselectn;
        
        // Filtering and limiting  
        case no_filter:
            return TAKfilter;
        
        case no_limit:
        {
            if (expr->hasAttribute(skipAtom))
                return TAKskiplimit;
            if (expr->hasAttribute(countAtom))
                return TAKcreaterowlimit;
            return TAKlimit;
        }
        
        case no_catchds:
        {
            if (expr->hasAttribute(skipAtom))
                return TAKskipcatch;
            return TAKcatch;
        }
        
        // File operations
        case no_buildindex:
            return TAKindexwrite;
        
        case no_spill:
            return TAKspill;
        
        // Result operations
        case no_extractresult:
        case no_setresult:
            return TAKworkunitwrite;
        
        case no_returnresult:
            return TAKremoteresult;
        
        case no_getgraphresult:
            return TAKlocalresultread;
        
        case no_getgraphloopresult:
            return TAKgraphloopresultread;
        
        case no_setgraphresult:
            return TAKlocalresultwrite;
        
        case no_spillgraphresult:
            return TAKlocalresultspill;
        
        case no_setgraphloopresult:
            return TAKgraphloopresultwrite;
        
        // Control flow - check for child context optimization
        case no_if:
        {
            if (expr->isAction())
                return TAKifaction;
            return TAKif;
        }
        
        case no_case:
        case no_map:
        {
            if (expr->isAction())
                return TAKifaction;  // Cases often become if-action chains
            return TAKcase;
        }
        
        case no_chooseds:
        case no_choose:
            return TAKcase;
        
        // Loop operations - check for optimization attributes
        case no_loop:
        {
            if (expr->hasAttribute(countAtom))
                return TAKloopcount;
            if (expr->isDataset())
                return TAKloopdataset;
            return TAKlooprow;
        }
        
        case no_graphloop:
        {
            if (expr->hasAttribute(parallelAtom))
                return TAKparallelgraphloop;
            return TAKgraphloop;
        }
        
        // Merge and combine operations
        case no_merge:
            return TAKmerge;
        
        case no_addfiles:
            return TAKfunnel;
        
        case no_combine:
            return TAKcombine;
        
        case no_combinegroup:
            return TAKcombinegroup;
        
        case no_regroup:
            return TAKregroup;
        
        case no_nonempty:
            return TAKnonempty;
        
        case no_filtergroup:
            return TAKfiltergroup;
        
        // Parsing operations
        case no_parse:
        case no_newparse:
            return TAKparse;
        
        case no_xmlparse:
        case no_newxmlparse:
            return TAKxmlparse;
        
        // External operations
        case no_pipe:
            return TAKpipethrough;
        
        case no_process:
            return TAKprocess;
        
        case no_httpcall:
            if (expr->isAction())
                return TAKsoap_rowaction;
            return TAKhttp_rowdataset;
        
        case no_soapcall:
        case no_soapcall_ds:
        case no_soapaction_ds:
        case no_newsoapcall:
        case no_newsoapcall_ds:
        case no_newsoapaction_ds:
        {
            if (expr->isAction())
                return TAKsoap_rowaction;
            return TAKsoap_rowdataset;
        }

        // Control structures
        case no_parallel:
            return TAKparallel;
        
        case no_sequential:
        case no_actionlist:
        case no_orderedactionlist:
            return TAKsequential;
        
        case no_apply:
            return TAKapply;
        
        // Distribution and metadata
        case no_distribution:
            return TAKdistribution;
        
        case no_keydiff:
            return TAKkeydiff;
        
        case no_keypatch:
            return TAKkeypatch;
        
        // Remote operations
        case no_allnodes:
            return TAKremotegraph;
        
        // When operations
        case no_executewhen:
            if (expr->isAction())
                return TAKwhen_action;
            return TAKwhen_dataset;
        
        // Quantile operations
        case no_quantile:
            return TAKquantile;
        
        // Other operations
        case no_split:
            return TAKsplit;
        
        // Handle compound select operations
        case no_compound_selectnew:
        case no_compound_inline:
            return TAKproject;  // These typically become projections
        
        // Dataset list operations
        case no_datasetlist:
            return TAKnwayinput;
        
        // Stepped operations
        case no_stepped:
            return TAKnull;  // Stepped is typically a property, not an activity
        
        case NO_AGGREGATE:
            return TAKaggregate;

        // Default case for operators without direct activity mapping
        default:
            // Many operators are expression-level and don't map to activities:
            // - Mathematical operations (no_add, no_sub, etc.)
            // - Comparison operations (no_eq, no_ne, etc.)
            // - String operations (no_concat, no_substring, etc.)
            // - Field access (no_field, no_select)
            // - Type operations (no_cast, no_implicitcast)
            // - Metadata operations (no_record, no_attr, etc.)
            return TAKnone;
    }
}

