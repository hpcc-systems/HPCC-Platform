/*##############################################################################

    Copyright (C) 2012 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */

#include "jstring.hpp"
#include "jiface.hpp"
#include "hqlir.hpp"

/*
 * The general format of IR is the following:
 *
 * %value = op_type operation(arg1, arg2, etc) @annotation1 etc
 *
 * Where:
 *  * %value is an auto-increment value that stores temporary values
 *    that can be used on subsequent operations.
 *  * op_type is the type of the operation, which will be the type of
 *    the return value
 *  * operation is the op-name of the node, which can be found by adding
 *    a leading op_ to it (ex. normalize -> op_normalize)
 *  * argN are the arguments (including flags) to the operation
 *  * annotation are all extra information on the operation
 *
 * The type of every value is defined by the operation that created it,
 * and it shall not be repeated when using it as argument of other
 * operations, because type information can be big enough on its own.
 *
 * For the same reasons, annotations are only printed on the operation.
 *
 * The IR will be printed in logical order, with the operation being
 * dumped at the end, and all its dependencies (and their dependencies)
 * printed in dependency order, upwards.
 *
 * Note that operations do accept multiple types for each argument
 * (think integerN, stringN, any numerical value, etc), so it's important
 * to make sure the argument is compatible, but there's no way to
 * enforce type safety with the current set of operators.
 *
 * Note that this code is intentionally not throwing exceptions or asserting,
 * since it will be largely used during debug sessions (step through or
 * core analysis) and debuggers are not that comfortable with calls throwing
 * exceptions or asserting.
 *
 * In case where an invalid tree might be produced (missing information),
 * the dumper should print "unknown_*", so that you can continue debugging
 * your program, and open an issue to fix the dumper or the expression tree
 * in separate.
 */

// For now, this is exclusive to the dumper, but could be extended to
// generate the importer tables, as well, with more clever macros
#define DUMP_CASE(t,x) case t##_##x: return #x
static const char * getOperatorText(node_operator op);
static const char * getTypeText(type_t type);

//-------------------------------------------------------------------------------------------------
/* IR Expression Dumper : A complete description of an expression in IR form
 *
 * This dumper should produce a complete description of the expression graph into textual
 * form, thus, it should be possible to read back and build the exact same graph.
 *
 * However, this feature is in development, and should be so until this comment is
 * removed. Make sure the IR you're printing can be read back before relying on it.
 *
 * Implementation details:
 *  * This dumper is depth first, to create the most basic nodes first. Not only they
 *    get printed first (logically) but they also receive the lower names
 *  * Names are, for now, numerical sequence. To implement text+num would require a
 *    more complex logic, which is not the point in this first implementation
 *  * attributes(attr, attr_expr) are not printed within the expression, but expanded
 *    like other dependencies. This might change in the future
 *  *
 */
class HQL_API IRExpressionDumper
{
public:
    IRExpressionDumper();
    ~IRExpressionDumper();

    // Re-use IRExtra information across multiple calls
    const char * dump(IHqlExpression * expr);

protected:
    void expandGraphText(IHqlExpression * expr);
    void expandExprDefinition(IHqlExpression * expr);
    void expandExprUse(IHqlExpression * expr);

    // For now only id, but should have more
    struct IRExtra : public IInterface, CInterface {
        IMPLEMENT_IINTERFACE;
        unsigned id; // unique identifier (%NNN)
        IRExtra(unsigned _id) : id(_id) {}
    };
    bool visited(IHqlExpression * expr);
    IRExtra * queryExtra(IHqlExpression * expr);   // assign or return

    // Information specific dumpers
    void appendType(IHqlExpression * expr);
    void appendOperation(IHqlExpression * expr);
    void appendAnnotation(IHqlExpression * expr);

    unsigned lastUsedId; // thread unsafe
    StringBuffer string; // output
};

IRExpressionDumper::IRExpressionDumper()
    : lastUsedId(0)
{
    lockTransformMutex();
}

IRExpressionDumper::~IRExpressionDumper()
{
    unlockTransformMutex();
}

// Public methods
const char * IRExpressionDumper::dump(IHqlExpression * expr)
{
    string.clear();
    expandGraphText(expr);
    return string.str();
}

// Private methods
void IRExpressionDumper::expandGraphText(IHqlExpression * expr)
{
    if (visited(expr))
        return;

    // Body with annotations
    IHqlExpression * body = expr->queryBody(true);
    if (body != expr)
    {
        expandGraphText(body);
    }
    else
    {
        // Depth first, to get deeper dependencies with lower names
        unsigned max = expr->numChildren();
        for (unsigned i=0; i < max; i++)
            expandGraphText(expr->queryChild(i));
    }

    // Now, dump the current expression
    expandExprDefinition(expr);
}

void IRExpressionDumper::expandExprDefinition(IHqlExpression * expr)
{
    string.append('%').append(queryExtra(expr)->id).append(" = ");

    if (expr->getAnnotationKind() != annotate_none)
    {
        // Like an alias, %n+1 = %n @annotation MORE - this will change soon
        IHqlExpression * body = expr->queryBody(true);
        expandExprUse(body); // should have been named already
        string.append(' ');
        appendAnnotation(expr);
    }
    else
        appendOperation(expr);

    // Finalise
    string.append(';').newline();
}

void IRExpressionDumper::expandExprUse(IHqlExpression * expr)
{
    string.append("%");
    if (visited(expr))
        string.append(queryExtra(expr)->id);
    else
        string.append("unknown_node");
    return;
}

void IRExpressionDumper::appendAnnotation(IHqlExpression * expr)
{
    switch (expr->getAnnotationKind())
    {
    case annotate_symbol:
        string.append("@symbol ").append(expr->queryName()->getAtomNamePtr());
        break;
    case annotate_location:
        string.append("@location \"").append(expr->querySourcePath()->getNamePtr());
        string.append("\":").append(expr->getStartLine());
        break;
    case annotate_meta:
        string.append("@meta");
        break;
    case annotate_warning:
        string.append("@warning");
        break;
    case annotate_parsemeta:
        string.append("@parsemeta");
        break;
    case annotate_max:
        string.append("@max");
        break;
    case annotate_javadoc:
        string.append("@javadoc");
        break;
    default:
        string.append("@unknown_annotation");
        break;
    }
}

void IRExpressionDumper::appendOperation(IHqlExpression * expr)
{
    appendType(expr);
    node_operator op = expr->getOperator();
    string.append(' ').append(getOperatorText(op));

    switch(op)
    {
    // Symbols that have value associated
    case no_constant:
        {
            string.append(' ');
            expr->queryValue()->generateECL(string);
            break;
        }
    // Symbols that have a name
    case no_field:
    case no_attr:
    case no_attr_expr:
        {
            _ATOM name = expr->queryName();
            string.append(' ').append(name->str());
            break;
        }
    // Other operations should have more data printed
    }

    // Now, populate arguments
    bool comma = false;
    unsigned max = expr->numChildren();
    if (max)
    {
        string.append('(');
        for (unsigned i=0; i < max; i++)
        {
            IHqlExpression * child = expr->queryChild(i);
            if (comma)
                string.append(", ");
            expandExprUse(child); // should have been named already
            comma = true;
        }
        string.append(')');
    }
}

void IRExpressionDumper::appendType(IHqlExpression * expr)
{
    if (expr->queryType())
        string.append(getTypeText(expr->queryType()->getTypeCode()));
    else
        string.append("no_type");
}

bool IRExpressionDumper::visited(IHqlExpression * expr)
{
    IRExtra * match = static_cast<IRExtra *>(expr->queryTransformExtra());
    return (match != NULL);
}

IRExpressionDumper::IRExtra * IRExpressionDumper::queryExtra(IHqlExpression * expr)
{
    IRExtra * match = static_cast<IRExtra *>(expr->queryTransformExtra());
    if (match)
        return match;

    IRExtra * extra = new IRExtra(++lastUsedId);
    expr->setTransformExtraOwned(extra);
    return extra;
}

// --------------------------------------------------------------- Common functions
static const char * getOperatorText(node_operator op)
{
    switch(op)
    {
    DUMP_CASE(no,band);
    DUMP_CASE(no,bor);
    DUMP_CASE(no,bxor);
    DUMP_CASE(no,mul);
    DUMP_CASE(no,div);
    DUMP_CASE(no,modulus);
    DUMP_CASE(no,exp);
    DUMP_CASE(no,round);
    DUMP_CASE(no,roundup);
    DUMP_CASE(no,truncate);
    DUMP_CASE(no,power);
    DUMP_CASE(no,ln);
    DUMP_CASE(no,sin);
    DUMP_CASE(no,cos);
    DUMP_CASE(no,tan);
    DUMP_CASE(no,asin);
    DUMP_CASE(no,acos);
    DUMP_CASE(no,atan);
    DUMP_CASE(no,atan2);
    DUMP_CASE(no,sinh);
    DUMP_CASE(no,cosh);
    DUMP_CASE(no,tanh);
    DUMP_CASE(no,log10);
    DUMP_CASE(no,sqrt);
    DUMP_CASE(no,negate);
    DUMP_CASE(no,sub);
    DUMP_CASE(no,add);
    DUMP_CASE(no,addfiles);
    DUMP_CASE(no,merge);
    DUMP_CASE(no,concat);
    DUMP_CASE(no,eq);
    DUMP_CASE(no,ne);
    DUMP_CASE(no,lt);
    DUMP_CASE(no,le);
    DUMP_CASE(no,gt);
    DUMP_CASE(no,ge);
    DUMP_CASE(no,order);
    DUMP_CASE(no,unicodeorder);
    DUMP_CASE(no,not);
    DUMP_CASE(no,and);
    DUMP_CASE(no,or);
    DUMP_CASE(no,xor);
    DUMP_CASE(no,notin);
    DUMP_CASE(no,in);
    DUMP_CASE(no,notbetween);
    DUMP_CASE(no,between);
    DUMP_CASE(no,comma);
    DUMP_CASE(no,compound);
    DUMP_CASE(no,count);
    DUMP_CASE(no,counter);
    DUMP_CASE(no,countgroup);
    DUMP_CASE(no,distribution);
    DUMP_CASE(no,max);
    DUMP_CASE(no,min);
    DUMP_CASE(no,sum);
    DUMP_CASE(no,ave);
    DUMP_CASE(no,variance);
    DUMP_CASE(no,covariance);
    DUMP_CASE(no,correlation);
    DUMP_CASE(no,map);
    DUMP_CASE(no,if);
    DUMP_CASE(no,mapto);
    DUMP_CASE(no,constant);
    DUMP_CASE(no,field);
    DUMP_CASE(no,exists);
    DUMP_CASE(no,existsgroup);
    DUMP_CASE(no,select);
    DUMP_CASE(no,table);
    DUMP_CASE(no,temptable);
    DUMP_CASE(no,workunit_dataset);
    DUMP_CASE(no,scope);
    DUMP_CASE(no,remotescope);
    DUMP_CASE(no,mergedscope);
    DUMP_CASE(no,privatescope);
    DUMP_CASE(no,list);
    DUMP_CASE(no,selectnth);
    DUMP_CASE(no,filter);
    DUMP_CASE(no,param);
    DUMP_CASE(no,within);
    DUMP_CASE(no,notwithin);
    DUMP_CASE(no,index);
    DUMP_CASE(no,all);
    DUMP_CASE(no,left);
    DUMP_CASE(no,right);
    DUMP_CASE(no,outofline);
    DUMP_CASE(no,dedup);
    DUMP_CASE(no,enth);
    DUMP_CASE(no,sample);
    DUMP_CASE(no,sort);
    DUMP_CASE(no,shuffle);
    DUMP_CASE(no,sorted);
    DUMP_CASE(no,choosen);
    DUMP_CASE(no,choosesets);
    DUMP_CASE(no,buildindex);
    DUMP_CASE(no,output);
    DUMP_CASE(no,record);
    DUMP_CASE(no,fetch);
    DUMP_CASE(no,compound_fetch);
    DUMP_CASE(no,join);
    DUMP_CASE(no,selfjoin);
    DUMP_CASE(no,newusertable);
    DUMP_CASE(no,usertable);
    DUMP_CASE(no,aggregate);
    DUMP_CASE(no,which);
    DUMP_CASE(no,case);
    DUMP_CASE(no,choose);
    DUMP_CASE(no,rejected);
    DUMP_CASE(no,evaluate);
    DUMP_CASE(no,cast);
    DUMP_CASE(no,implicitcast);
    DUMP_CASE(no,external);
    DUMP_CASE(no,externalcall);
    DUMP_CASE(no,macro);
    DUMP_CASE(no,failure);
    DUMP_CASE(no,success);
    DUMP_CASE(no,recovery);
    DUMP_CASE(no,sql);
    DUMP_CASE(no,flat);
    DUMP_CASE(no,csv);
    DUMP_CASE(no,xml);
    DUMP_CASE(no,when);
    DUMP_CASE(no,priority);
    DUMP_CASE(no,rollup);
    DUMP_CASE(no,iterate);
    DUMP_CASE(no,assign);
    DUMP_CASE(no,asstring);
    DUMP_CASE(no,assignall);
    DUMP_CASE(no,update);
    DUMP_CASE(no,alias);
    DUMP_CASE(no,denormalize);
    DUMP_CASE(no,denormalizegroup);
    DUMP_CASE(no,normalize);
    DUMP_CASE(no,group);
    DUMP_CASE(no,grouped);
    DUMP_CASE(no,unknown);
    DUMP_CASE(no,any);
    DUMP_CASE(no,is_null);
    DUMP_CASE(no,is_valid);
    DUMP_CASE(no,abs);
    DUMP_CASE(no,substring);
    DUMP_CASE(no,newaggregate);
    DUMP_CASE(no,trim);
    DUMP_CASE(no,realformat);
    DUMP_CASE(no,intformat);
    DUMP_CASE(no,regex_find);
    DUMP_CASE(no,regex_replace);
    DUMP_CASE(no,current_date);
    DUMP_CASE(no,current_time);
    DUMP_CASE(no,current_timestamp);
    DUMP_CASE(no,cogroup);
    DUMP_CASE(no,cosort);
    DUMP_CASE(no,sortlist);
    DUMP_CASE(no,recordlist);
    DUMP_CASE(no,transformlist);
    DUMP_CASE(no,transformebcdic);
    DUMP_CASE(no,transformascii);
    DUMP_CASE(no,hqlproject);
    DUMP_CASE(no,dataset_from_transform);
    DUMP_CASE(no,newtransform);
    DUMP_CASE(no,transform);
    DUMP_CASE(no,attr);
    DUMP_CASE(no,attr_expr);
    DUMP_CASE(no,attr_link);
    DUMP_CASE(no,self);
    DUMP_CASE(no,selfref);
    DUMP_CASE(no,thor);
    DUMP_CASE(no,distribute);
    DUMP_CASE(no,distributed);
    DUMP_CASE(no,keyeddistribute);
    DUMP_CASE(no,rank);
    DUMP_CASE(no,ranked);
    DUMP_CASE(no,ordered);
    DUMP_CASE(no,hash);
    DUMP_CASE(no,hash32);
    DUMP_CASE(no,hash64);
    DUMP_CASE(no,hashmd5);
    DUMP_CASE(no,none);
    DUMP_CASE(no,notnot);
    DUMP_CASE(no,range);
    DUMP_CASE(no,rangeto);
    DUMP_CASE(no,rangefrom);
    DUMP_CASE(no,service);
    DUMP_CASE(no,mix);
    DUMP_CASE(no,funcdef);
    DUMP_CASE(no,wait);
    DUMP_CASE(no,notify);
    DUMP_CASE(no,event);
    DUMP_CASE(no,persist);
    DUMP_CASE(no,omitted);
    DUMP_CASE(no,setconditioncode);
    DUMP_CASE(no,selectfields);
    DUMP_CASE(no,quoted);
    DUMP_CASE(no,variable);
    DUMP_CASE(no,bnot);
    DUMP_CASE(no,charlen);
    DUMP_CASE(no,sizeof);
    DUMP_CASE(no,offsetof);
    DUMP_CASE(no,postinc);
    DUMP_CASE(no,postdec);
    DUMP_CASE(no,preinc);
    DUMP_CASE(no,predec);
    DUMP_CASE(no,pselect);
    DUMP_CASE(no,address);
    DUMP_CASE(no,deref);
    DUMP_CASE(no,nullptr);
    DUMP_CASE(no,decimalstack);
    DUMP_CASE(no,typetransfer);
    DUMP_CASE(no,apply);
    DUMP_CASE(no,pipe);
    DUMP_CASE(no,cloned);
    DUMP_CASE(no,cachealias);
    DUMP_CASE(no,joined);
    DUMP_CASE(no,lshift);
    DUMP_CASE(no,rshift);
    DUMP_CASE(no,colon);
    DUMP_CASE(no,global);
    DUMP_CASE(no,stored);
    DUMP_CASE(no,checkpoint);
    DUMP_CASE(no,compound_indexread);
    DUMP_CASE(no,compound_diskread);
    DUMP_CASE(no,translated);
    DUMP_CASE(no,ifblock);
    DUMP_CASE(no,crc);
    DUMP_CASE(no,random);
    DUMP_CASE(no,childdataset);
    DUMP_CASE(no,envsymbol);
    DUMP_CASE(no,null);
    DUMP_CASE(no,ensureresult);
    DUMP_CASE(no,getresult);
    DUMP_CASE(no,setresult);
    DUMP_CASE(no,extractresult);
    DUMP_CASE(no,type);
    DUMP_CASE(no,position);
    DUMP_CASE(no,bound_func);
    DUMP_CASE(no,bound_type);
    DUMP_CASE(no,hint);
    DUMP_CASE(no,metaactivity);
    DUMP_CASE(no,loadxml);
    DUMP_CASE(no,fieldmap);
    DUMP_CASE(no,template_context);
    DUMP_CASE(no,nofold);
    DUMP_CASE(no,nohoist);
    DUMP_CASE(no,fail);
    DUMP_CASE(no,filepos);
    DUMP_CASE(no,file_logicalname);
    DUMP_CASE(no,alias_project);
    DUMP_CASE(no,alias_scope);
    DUMP_CASE(no,sequential);
    DUMP_CASE(no,parallel);
    DUMP_CASE(no,actionlist);
    DUMP_CASE(no,nolink);
    DUMP_CASE(no,workflow);
    DUMP_CASE(no,workflow_action);
    DUMP_CASE(no,failcode);
    DUMP_CASE(no,failmessage);
    DUMP_CASE(no,eventname);
    DUMP_CASE(no,eventextra);
    DUMP_CASE(no,independent);
    DUMP_CASE(no,keyindex);
    DUMP_CASE(no,newkeyindex);
    DUMP_CASE(no,keyed);
    DUMP_CASE(no,split);
    DUMP_CASE(no,subgraph);
    DUMP_CASE(no,dependenton);
    DUMP_CASE(no,spill);
    DUMP_CASE(no,setmeta);
    DUMP_CASE(no,throughaggregate);
    DUMP_CASE(no,joincount);
    DUMP_CASE(no,countcompare);
    DUMP_CASE(no,limit);
    DUMP_CASE(no,fromunicode);
    DUMP_CASE(no,tounicode);
    DUMP_CASE(no,keyunicode);
    DUMP_CASE(no,parse);
    DUMP_CASE(no,newparse);
    DUMP_CASE(no,skip);
    DUMP_CASE(no,matched);
    DUMP_CASE(no,matchtext);
    DUMP_CASE(no,matchlength);
    DUMP_CASE(no,matchposition);
    DUMP_CASE(no,matchunicode);
    DUMP_CASE(no,matchrow);
    DUMP_CASE(no,matchutf8);
    DUMP_CASE(no,pat_select);
    DUMP_CASE(no,pat_index);
    DUMP_CASE(no,pat_const);
    DUMP_CASE(no,pat_pattern);
    DUMP_CASE(no,pat_follow);
    DUMP_CASE(no,pat_first);
    DUMP_CASE(no,pat_last);
    DUMP_CASE(no,pat_repeat);
    DUMP_CASE(no,pat_instance);
    DUMP_CASE(no,pat_anychar);
    DUMP_CASE(no,pat_token);
    DUMP_CASE(no,pat_imptoken);
    DUMP_CASE(no,pat_set);
    DUMP_CASE(no,pat_checkin);
    DUMP_CASE(no,pat_x_before_y);
    DUMP_CASE(no,pat_x_after_y);
    DUMP_CASE(no,pat_before_y);
    DUMP_CASE(no,pat_after_y);
    DUMP_CASE(no,pat_beginpattern);
    DUMP_CASE(no,pat_endpattern);
    DUMP_CASE(no,pat_checklength);
    DUMP_CASE(no,pat_use);
    DUMP_CASE(no,pat_validate);
    DUMP_CASE(no,topn);
    DUMP_CASE(no,outputscalar);
    DUMP_CASE(no,penalty);
    DUMP_CASE(no,rowdiff);
    DUMP_CASE(no,wuid);
    DUMP_CASE(no,featuretype);
    DUMP_CASE(no,pat_guard);
    DUMP_CASE(no,xmltext);
    DUMP_CASE(no,xmlunicode);
    DUMP_CASE(no,xmlproject);
    DUMP_CASE(no,newxmlparse);
    DUMP_CASE(no,xmlparse);
    DUMP_CASE(no,xmldecode);
    DUMP_CASE(no,xmlencode);
    DUMP_CASE(no,pat_featureparam);
    DUMP_CASE(no,pat_featureactual);
    DUMP_CASE(no,pat_featuredef);
    DUMP_CASE(no,evalonce);
    DUMP_CASE(no,distributer);
    DUMP_CASE(no,impure);
    DUMP_CASE(no,addsets);
    DUMP_CASE(no,rowvalue);
    DUMP_CASE(no,pat_case);
    DUMP_CASE(no,pat_nocase);
    DUMP_CASE(no,evaluate_stmt);
    DUMP_CASE(no,return_stmt);
    DUMP_CASE(no,activetable);
    DUMP_CASE(no,preload);
    DUMP_CASE(no,createset);
    DUMP_CASE(no,assertkeyed);
    DUMP_CASE(no,assertwild);
    DUMP_CASE(no,httpcall);
    DUMP_CASE(no,soapcall);
    DUMP_CASE(no,soapcall_ds);
    DUMP_CASE(no,newsoapcall);
    DUMP_CASE(no,newsoapcall_ds);
    DUMP_CASE(no,soapaction_ds);
    DUMP_CASE(no,newsoapaction_ds);
    DUMP_CASE(no,temprow);
    DUMP_CASE(no,projectrow);
    DUMP_CASE(no,createrow);
    DUMP_CASE(no,activerow);
    DUMP_CASE(no,newrow);
    DUMP_CASE(no,catch);
    DUMP_CASE(no,reference);
    DUMP_CASE(no,callback);
    DUMP_CASE(no,keyedlimit);
    DUMP_CASE(no,keydiff);
    DUMP_CASE(no,keypatch);
    DUMP_CASE(no,returnresult);
    DUMP_CASE(no,id2blob);
    DUMP_CASE(no,blob2id);
    DUMP_CASE(no,anon);
    DUMP_CASE(no,cppbody);
    DUMP_CASE(no,sortpartition);
    DUMP_CASE(no,define);
    DUMP_CASE(no,globalscope);
    DUMP_CASE(no,forcelocal);
    DUMP_CASE(no,typedef);
    DUMP_CASE(no,matchattr);
    DUMP_CASE(no,pat_production);
    DUMP_CASE(no,guard);
    DUMP_CASE(no,datasetfromrow);
    DUMP_CASE(no,assertconstant);
    DUMP_CASE(no,clustersize);
    DUMP_CASE(no,compound_disknormalize);
    DUMP_CASE(no,compound_diskaggregate);
    DUMP_CASE(no,compound_diskcount);
    DUMP_CASE(no,compound_diskgroupaggregate);
    DUMP_CASE(no,compound_indexnormalize);
    DUMP_CASE(no,compound_indexaggregate);
    DUMP_CASE(no,compound_indexcount);
    DUMP_CASE(no,compound_indexgroupaggregate);
    DUMP_CASE(no,compound_childread);
    DUMP_CASE(no,compound_childnormalize);
    DUMP_CASE(no,compound_childaggregate);
    DUMP_CASE(no,compound_childcount);
    DUMP_CASE(no,compound_childgroupaggregate);
    DUMP_CASE(no,compound_selectnew);
    DUMP_CASE(no,compound_inline);
    DUMP_CASE(no,setworkflow_cond);
    DUMP_CASE(no,recovering);
    DUMP_CASE(no,nothor);
    DUMP_CASE(no,call);
    DUMP_CASE(no,getgraphresult);
    DUMP_CASE(no,setgraphresult);
    DUMP_CASE(no,assert);
    DUMP_CASE(no,assert_ds);
    DUMP_CASE(no,namedactual);
    DUMP_CASE(no,combine);
    DUMP_CASE(no,combinegroup);
    DUMP_CASE(no,rows);
    DUMP_CASE(no,rollupgroup);
    DUMP_CASE(no,regroup);
    DUMP_CASE(no,inlinetable);
    DUMP_CASE(no,spillgraphresult);
    DUMP_CASE(no,enum);
    DUMP_CASE(no,pat_or);
    DUMP_CASE(no,loop);
    DUMP_CASE(no,loopbody);
    DUMP_CASE(no,cluster);
    DUMP_CASE(no,forcenolocal);
    DUMP_CASE(no,allnodes);
    DUMP_CASE(no,last_op);
    DUMP_CASE(no,pat_compound);
    DUMP_CASE(no,pat_begintoken);
    DUMP_CASE(no,pat_endtoken);
    DUMP_CASE(no,pat_begincheck);
    DUMP_CASE(no,pat_endcheckin);
    DUMP_CASE(no,pat_endchecklength);
    DUMP_CASE(no,pat_beginseparator);
    DUMP_CASE(no,pat_endseparator);
    DUMP_CASE(no,pat_separator);
    DUMP_CASE(no,pat_beginvalidate);
    DUMP_CASE(no,pat_endvalidate);
    DUMP_CASE(no,pat_dfa);
    DUMP_CASE(no,pat_singlechar);
    DUMP_CASE(no,pat_beginrecursive);
    DUMP_CASE(no,pat_endrecursive);
    DUMP_CASE(no,pat_utf8single);
    DUMP_CASE(no,pat_utf8lead);
    DUMP_CASE(no,pat_utf8follow);
    DUMP_CASE(no,sequence);
    DUMP_CASE(no,forwardscope);
    DUMP_CASE(no,virtualscope);
    DUMP_CASE(no,concretescope);
    DUMP_CASE(no,purevirtual);
    DUMP_CASE(no,internalvirtual);
    DUMP_CASE(no,delayedselect);
    DUMP_CASE(no,libraryselect);
    DUMP_CASE(no,libraryscope);
    DUMP_CASE(no,libraryscopeinstance);
    DUMP_CASE(no,libraryinput);
    DUMP_CASE(no,process);
    DUMP_CASE(no,thisnode);
    DUMP_CASE(no,graphloop);
    DUMP_CASE(no,rowset);
    DUMP_CASE(no,loopcounter);
    DUMP_CASE(no,getgraphloopresult);
    DUMP_CASE(no,setgraphloopresult);
    DUMP_CASE(no,rowsetindex);
    DUMP_CASE(no,rowsetrange);
    DUMP_CASE(no,assertstepped);
    DUMP_CASE(no,assertsorted);
    DUMP_CASE(no,assertgrouped);
    DUMP_CASE(no,assertdistributed);
    DUMP_CASE(no,datasetlist);
    DUMP_CASE(no,mergejoin);
    DUMP_CASE(no,nwayjoin);
    DUMP_CASE(no,nwaymerge);
    DUMP_CASE(no,stepped);
    DUMP_CASE(no,getgraphloopresultset);
    DUMP_CASE(no,attrname);
    DUMP_CASE(no,nonempty);
    DUMP_CASE(no,filtergroup);
    DUMP_CASE(no,rangecommon);
    DUMP_CASE(no,section);
    DUMP_CASE(no,nobody);
    DUMP_CASE(no,deserialize);
    DUMP_CASE(no,serialize);
    DUMP_CASE(no,eclcrc);
    DUMP_CASE(no,pure);
    DUMP_CASE(no,pseudods);
    DUMP_CASE(no,top);
    DUMP_CASE(no,uncommoned_comma);
    DUMP_CASE(no,nameof);
    DUMP_CASE(no,processing);
    DUMP_CASE(no,merge_pending);
    DUMP_CASE(no,merge_nomatch);
    DUMP_CASE(no,toxml);
    DUMP_CASE(no,catchds);
    DUMP_CASE(no,readspill);
    DUMP_CASE(no,writespill);
    DUMP_CASE(no,commonspill);
    DUMP_CASE(no,forcegraph);
    DUMP_CASE(no,sectioninput);
    DUMP_CASE(no,related);
    DUMP_CASE(no,definesideeffect);
    DUMP_CASE(no,executewhen);
    DUMP_CASE(no,callsideeffect);
    DUMP_CASE(no,fromxml);
    DUMP_CASE(no,preservemeta);
    DUMP_CASE(no,normalizegroup);
    DUMP_CASE(no,indirect);
    DUMP_CASE(no,selectindirect);
    DUMP_CASE(no,isomitted);
    DUMP_CASE(no,getenv);
    DUMP_CASE(no,once);
    DUMP_CASE(no,persist_check);
    DUMP_CASE(no,create_initializer);
    DUMP_CASE(no,owned_ds);
    DUMP_CASE(no,complex);
    DUMP_CASE(no,assign_addfiles);
    DUMP_CASE(no,debug_option_value);
    DUMP_CASE(no,dataset_alias);
    DUMP_CASE(no,childquery);

    case no_unused3: case no_unused4: case no_unused5: case no_unused6:
    case no_unused13: case no_unused14: case no_unused15: case no_unused18: case no_unused19:
    case no_unused20: case no_unused21: case no_unused22: case no_unused23: case no_unused24: case no_unused25: case no_unused26: case no_unused27: case no_unused28: case no_unused29:
    case no_unused30: case no_unused31: case no_unused32: case no_unused33: case no_unused34: case no_unused35: case no_unused36: case no_unused37: case no_unused38:
    case no_unused40: case no_unused41: case no_unused42: case no_unused43: case no_unused44: case no_unused45: case no_unused46: case no_unused47: case no_unused48: case no_unused49:
    case no_unused50: case no_unused52:
        return "unused";
    }
    return "unknown_op";
}

static const char * getTypeText(type_t type)
{
    switch(type)
    {
    DUMP_CASE(type,boolean);
    DUMP_CASE(type,int);
    DUMP_CASE(type,real);
    DUMP_CASE(type,decimal);
    DUMP_CASE(type,string);
    DUMP_CASE(type,date);
    DUMP_CASE(type,bitfield);
    DUMP_CASE(type,char);
    DUMP_CASE(type,enumerated);
    DUMP_CASE(type,record);
    DUMP_CASE(type,varstring);
    DUMP_CASE(type,blob);
    DUMP_CASE(type,data);
    DUMP_CASE(type,pointer);
    DUMP_CASE(type,class);
    DUMP_CASE(type,array);
    DUMP_CASE(type,table);
    DUMP_CASE(type,set);
    DUMP_CASE(type,row);
    DUMP_CASE(type,groupedtable);
    DUMP_CASE(type,void);
    DUMP_CASE(type,alien);
    DUMP_CASE(type,swapint);
    DUMP_CASE(type,none);
    DUMP_CASE(type,packedint);
    DUMP_CASE(type,qstring);
    DUMP_CASE(type,unicode);
    DUMP_CASE(type,any);
    DUMP_CASE(type,varunicode);
    DUMP_CASE(type,pattern);
    DUMP_CASE(type,rule);
    DUMP_CASE(type,token);
    DUMP_CASE(type,feature);
    DUMP_CASE(type,event);
    DUMP_CASE(type,null);
    DUMP_CASE(type,scope);
    DUMP_CASE(type,utf8);
    DUMP_CASE(type,transform);
    DUMP_CASE(type,ifblock);
    DUMP_CASE(type,function);
    DUMP_CASE(type,sortlist);
    DUMP_CASE(type,modifier);
    DUMP_CASE(type,unsigned);
    DUMP_CASE(type,ebcdic);
    DUMP_CASE(type,stringorunicode);
    DUMP_CASE(type,numeric);
    DUMP_CASE(type,scalar);

    case type_unused1:
    case type_unused2:
    case type_unused3:
    case type_unused4:
    case type_unused5:
        return "unused";
    }
    return "unknown_type";
}

// --------------------------------------------------------------- Exported functions
extern HQL_API void dump_ir(IHqlExpression * expr)
{
    IRExpressionDumper dumper;
    printf("\nIR Expression Dumper\n====================\n\n");
    printf("%s", dumper.dump(expr));
}

extern HQL_API void dump_ir(HqlExprArray list)
{
    IRExpressionDumper dumper;
    printf("\nIR Expression Dumper\n====================\n");
    ForEachItemIn(i, list)
    {
        printf("\n%s", dumper.dump(&list.item(i)));
    }
}
