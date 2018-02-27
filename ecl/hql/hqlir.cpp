/*##############################################################################

    Copyright (C) 2012 HPCC Systems®.

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

#include "jstring.hpp"
#include "jiface.hpp"
#include "hqlir.hpp"

//#define ADD_ACTIVE_SCOPE_AS_COMMENT

namespace EclIR
{

/*
The general format of IR is the following:

<type> := <complex-type>
       | <simple-type>
       | %typelabel
       | <type> { annotation }
       | typeof <expr>

complex-type
    := set '(' <type> ')'
    | dataset '(' record-type ')'
    | row '(' record-type ')'
    | grouped <type>
    ;

<simple-type>
    := [u]int<n>            // [unsigned] integer[n]
    | [u]swap<n>            //
    | [u]packed
    | [e]str<n>
    | [u]dec(<n>[,m)
    | real<n>
    | vstr<n>
    | data<n>
    | uni<n>
    | vuni<n>
    | any
    | action
    | null
    ;

%typelabel = 'type' <type>;

<expression> := operator[#name][(sequence(n))][(arg1, arg2, ... argn) : <type>]
             | %exprlabel
             | <expression> { annotation };

%exprlabel = <expression>;

By convention the labels are generated as follows:

Types
  %t<nnnnn> - name of a type
  %tr<nnnnn> - type of a record
  %tw<nnnnn> - type of a row
  %td<nnnnn> - type of a dataset

Expressions
  %ds<nnnnn> - a dataset
  %rw<nnnnn> - a row
  %rc<nnnnn> - a record
  %dt<nnnnn> - a dictionary
  %e<nnnnn>  - a general expression

Where <nnnnn> is an auto incremented number.

There are options on generating to
a) Expand unshared annotations inline
b) Expand simple operands inline (e.g., constants, attributes)
c) Use #name as a synonym for attr:name
d) Expand simple types inline instead of using type labels.

The IR will be printed in logical order, with the operation being
dumped at the end, and all its dependencies (and their dependencies)
printed in dependency order, upwards.

Note that operations do accept multiple types for each argument
(think integerN, stringN, any numerical value, etc), so it's important
to make sure the argument is compatible, but there's no way to
enforce type safety with the current set of operators.

Note that this code is intentionally not throwing exceptions or asserting,
since it will be largely used during debug sessions (step through or
core analysis) and debuggers are not that comfortable with calls throwing
exceptions or asserting.

In case where an invalid tree might be produced (missing information),
the dumper should print "unknown_*", so that you can continue debugging
your program, and open an issue to fix the dumper or the expression tree
in separate.

The test cases at the end of this file give some examples of the output that is expected.

*/

// -------------------------------------------------------------------------------------------------------------------

#define EXPAND_CASE(prefix,suffix) case prefix##_##suffix: return #suffix

const char * getOperatorIRText(node_operator op)
{
    switch(op)
    {
    EXPAND_CASE(no,none);
    EXPAND_CASE(no,scope);
    EXPAND_CASE(no,list);
    EXPAND_CASE(no,mul);
    EXPAND_CASE(no,div);
    EXPAND_CASE(no,modulus);
    EXPAND_CASE(no,negate);
    EXPAND_CASE(no,add);
    EXPAND_CASE(no,sub);
    EXPAND_CASE(no,eq);
    EXPAND_CASE(no,ne);
    EXPAND_CASE(no,lt);
    EXPAND_CASE(no,le);
    EXPAND_CASE(no,gt);
    EXPAND_CASE(no,ge);
    EXPAND_CASE(no,not);
    EXPAND_CASE(no,notnot);
    EXPAND_CASE(no,and);
    EXPAND_CASE(no,or);
    EXPAND_CASE(no,xor);
    EXPAND_CASE(no,concat);
    EXPAND_CASE(no,notin);
    EXPAND_CASE(no,in);
    EXPAND_CASE(no,notbetween);
    EXPAND_CASE(no,between);
    EXPAND_CASE(no,comma);
    EXPAND_CASE(no,count);
    EXPAND_CASE(no,countgroup);
    EXPAND_CASE(no,selectmap);
    EXPAND_CASE(no,exists);
    EXPAND_CASE(no,within);
    EXPAND_CASE(no,notwithin);
    EXPAND_CASE(no,param);
    EXPAND_CASE(no,constant);
    EXPAND_CASE(no,field);
    EXPAND_CASE(no,map);
    EXPAND_CASE(no,if);
    EXPAND_CASE(no,max);
    EXPAND_CASE(no,min);
    EXPAND_CASE(no,sum);
    EXPAND_CASE(no,ave);
    EXPAND_CASE(no,maxgroup);
    EXPAND_CASE(no,mingroup);
    EXPAND_CASE(no,sumgroup);
    EXPAND_CASE(no,avegroup);
    EXPAND_CASE(no,exp);
    EXPAND_CASE(no,power);
    EXPAND_CASE(no,round);
    EXPAND_CASE(no,roundup);
    EXPAND_CASE(no,range);
    EXPAND_CASE(no,rangeto);
    EXPAND_CASE(no,rangefrom);
    EXPAND_CASE(no,substring);
    EXPAND_CASE(no,transform);
    EXPAND_CASE(no,rollup);
    EXPAND_CASE(no,iterate);
    EXPAND_CASE(no,hqlproject);
    EXPAND_CASE(no,assign);
    EXPAND_CASE(no,assignall);
    EXPAND_CASE(no,asstring);
    EXPAND_CASE(no,group);
    EXPAND_CASE(no,cogroup);
    EXPAND_CASE(no,cosort);
    EXPAND_CASE(no,truncate);
    EXPAND_CASE(no,ln);
    EXPAND_CASE(no,log10);
    EXPAND_CASE(no,sin);
    EXPAND_CASE(no,cos);
    EXPAND_CASE(no,tan);
    EXPAND_CASE(no,asin);
    EXPAND_CASE(no,acos);
    EXPAND_CASE(no,atan);
    EXPAND_CASE(no,atan2);
    EXPAND_CASE(no,sinh);
    EXPAND_CASE(no,cosh);
    EXPAND_CASE(no,tanh);
    EXPAND_CASE(no,sqrt);
    EXPAND_CASE(no,evaluate);
    EXPAND_CASE(no,choose);
    EXPAND_CASE(no,which);
    EXPAND_CASE(no,rejected);
    EXPAND_CASE(no,mapto);
    EXPAND_CASE(no,record);
    EXPAND_CASE(no,service);
    EXPAND_CASE(no,index);
    EXPAND_CASE(no,all);
    EXPAND_CASE(no,left);
    EXPAND_CASE(no,right);
    EXPAND_CASE(no,outofline);
    EXPAND_CASE(no,cast);
    EXPAND_CASE(no,implicitcast);
    EXPAND_CASE(no,once);
    EXPAND_CASE(no,csv);
    EXPAND_CASE(no,sql);
    EXPAND_CASE(no,thor);
    EXPAND_CASE(no,flat);
    EXPAND_CASE(no,pipe);
    EXPAND_CASE(no,mix);
    EXPAND_CASE(no,selectnth);
    EXPAND_CASE(no,stored);
    EXPAND_CASE(no,failure);
    EXPAND_CASE(no,success);
    EXPAND_CASE(no,recovery);
    EXPAND_CASE(no,external);
    EXPAND_CASE(no,funcdef);
    EXPAND_CASE(no,externalcall);
    EXPAND_CASE(no,wait);
    EXPAND_CASE(no,event);
    EXPAND_CASE(no,persist);
    EXPAND_CASE(no,buildindex);
    EXPAND_CASE(no,output);
    EXPAND_CASE(no,omitted);
    EXPAND_CASE(no,when);
    EXPAND_CASE(no,setconditioncode);
    EXPAND_CASE(no,priority);
    EXPAND_CASE(no,intformat);
    EXPAND_CASE(no,realformat);
    EXPAND_CASE(no,abs);
    EXPAND_CASE(no,nofold);
    EXPAND_CASE(no,table);
    EXPAND_CASE(no,keyindex);
    EXPAND_CASE(no,temptable);
    EXPAND_CASE(no,usertable);
    EXPAND_CASE(no,choosen);
    EXPAND_CASE(no,filter);
    EXPAND_CASE(no,fetch);
    EXPAND_CASE(no,join);
    EXPAND_CASE(no,joined);
    EXPAND_CASE(no,sort);
    EXPAND_CASE(no,sorted);
    EXPAND_CASE(no,sortlist);
    EXPAND_CASE(no,dedup);
    EXPAND_CASE(no,enth);
    EXPAND_CASE(no,sample);
    EXPAND_CASE(no,selectfields);
    EXPAND_CASE(no,persist_check);
    EXPAND_CASE(no,create_initializer);
    EXPAND_CASE(no,owned_ds);
    EXPAND_CASE(no,complex);
    EXPAND_CASE(no,assign_addfiles);
    EXPAND_CASE(no,debug_option_value);
    EXPAND_CASE(no,hash);
    EXPAND_CASE(no,hash32);
    EXPAND_CASE(no,hash64);
    EXPAND_CASE(no,crc);
    EXPAND_CASE(no,return_stmt);
    EXPAND_CASE(no,update);
    EXPAND_CASE(no,subsort);
    EXPAND_CASE(no,chooseds);
    EXPAND_CASE(no,alias);
    EXPAND_CASE(no,datasetfromdictionary);
    EXPAND_CASE(no,delayedscope);
    EXPAND_CASE(no,assertconcrete);
    EXPAND_CASE(no,unboundselect);
    EXPAND_CASE(no,id);
    EXPAND_CASE(no,orderedactionlist);
    EXPAND_CASE(no,dataset_from_transform);
    EXPAND_CASE(no,childquery);
    EXPAND_CASE(no,unknown);
    EXPAND_CASE(no,createdictionary);
    EXPAND_CASE(no,indict);
    EXPAND_CASE(no,countdict);
    EXPAND_CASE(no,any);
    EXPAND_CASE(no,existsdict);
    EXPAND_CASE(no,quantile);
    EXPAND_CASE(no,nocombine);
    EXPAND_CASE(no,unordered);
    EXPAND_CASE(no,critical);
    EXPAND_CASE(no,likely);
    EXPAND_CASE(no,unlikely);
    EXPAND_CASE(no,inline);
    EXPAND_CASE(no,unused33);
    EXPAND_CASE(no,unused34);
    EXPAND_CASE(no,unused35);
    EXPAND_CASE(no,unused36);
    EXPAND_CASE(no,unused37);
    EXPAND_CASE(no,unused38);
    EXPAND_CASE(no,is_null);
    EXPAND_CASE(no,dataset_alias);
    EXPAND_CASE(no,unused40);
    EXPAND_CASE(no,unused41);
    EXPAND_CASE(no,unused52);
    EXPAND_CASE(no,trim);
    EXPAND_CASE(no,position);
    EXPAND_CASE(no,charlen);
    EXPAND_CASE(no,unused42);
    EXPAND_CASE(no,unused43);
    EXPAND_CASE(no,unused44);
    EXPAND_CASE(no,unused45);
    EXPAND_CASE(no,unused46);
    EXPAND_CASE(no,unused47);
    EXPAND_CASE(no,unused48);
    EXPAND_CASE(no,unused49);
    EXPAND_CASE(no,unused50);
    EXPAND_CASE(no,nullptr);
    EXPAND_CASE(no,sizeof);
    EXPAND_CASE(no,offsetof);
    EXPAND_CASE(no,current_date);
    EXPAND_CASE(no,current_time);
    EXPAND_CASE(no,current_timestamp);
    EXPAND_CASE(no,variable);
    EXPAND_CASE(no,libraryselect);
    EXPAND_CASE(no,case);
    EXPAND_CASE(no,band);
    EXPAND_CASE(no,bor);
    EXPAND_CASE(no,bxor);
    EXPAND_CASE(no,bnot);
    EXPAND_CASE(no,postinc);
    EXPAND_CASE(no,postdec);
    EXPAND_CASE(no,preinc);
    EXPAND_CASE(no,predec);
    EXPAND_CASE(no,pselect);
    EXPAND_CASE(no,address);
    EXPAND_CASE(no,deref);
    EXPAND_CASE(no,order);
    EXPAND_CASE(no,hint);
    EXPAND_CASE(no,attr);
    EXPAND_CASE(no,self);
    EXPAND_CASE(no,rank);
    EXPAND_CASE(no,ranked);
    EXPAND_CASE(no,mergedscope);
    EXPAND_CASE(no,ordered);
    EXPAND_CASE(no,typetransfer);
    EXPAND_CASE(no,decimalstack);
    EXPAND_CASE(no,type);
    EXPAND_CASE(no,apply);
    EXPAND_CASE(no,ifblock);
    EXPAND_CASE(no,translated);
    EXPAND_CASE(no,addfiles);
    EXPAND_CASE(no,distribute);
    EXPAND_CASE(no,macro);
    EXPAND_CASE(no,cloned);
    EXPAND_CASE(no,cachealias);
    EXPAND_CASE(no,lshift);
    EXPAND_CASE(no,rshift);
    EXPAND_CASE(no,colon);
    EXPAND_CASE(no,setworkflow_cond);
    EXPAND_CASE(no,unused102);
    EXPAND_CASE(no,unused15);
    EXPAND_CASE(no,random);
    EXPAND_CASE(no,select);
    EXPAND_CASE(no,normalize);
    EXPAND_CASE(no,counter);
    EXPAND_CASE(no,distributed);
    EXPAND_CASE(no,grouped);
    EXPAND_CASE(no,denormalize);
    EXPAND_CASE(no,transformebcdic);
    EXPAND_CASE(no,transformascii);
    EXPAND_CASE(no,childdataset);
    EXPAND_CASE(no,envsymbol);
    EXPAND_CASE(no,null);
    EXPAND_CASE(no,quoted);
    EXPAND_CASE(no,bound_func);
    EXPAND_CASE(no,bound_type);
    EXPAND_CASE(no,metaactivity);
    EXPAND_CASE(no,fail);
    EXPAND_CASE(no,filepos);
    EXPAND_CASE(no,aggregate);
    EXPAND_CASE(no,distribution);
    EXPAND_CASE(no,newusertable);
    EXPAND_CASE(no,newaggregate);
    EXPAND_CASE(no,newtransform);
    EXPAND_CASE(no,fromunicode);
    EXPAND_CASE(no,tounicode);
    EXPAND_CASE(no,keyunicode);
    EXPAND_CASE(no,loadxml);
    EXPAND_CASE(no,isomitted);
    EXPAND_CASE(no,fieldmap);
    EXPAND_CASE(no,template_context);
    EXPAND_CASE(no,ensureresult);
    EXPAND_CASE(no,getresult);
    EXPAND_CASE(no,setresult);
    EXPAND_CASE(no,is_valid);
    EXPAND_CASE(no,alias_project);
    EXPAND_CASE(no,alias_scope);
    EXPAND_CASE(no,global);
    EXPAND_CASE(no,eventname);
    EXPAND_CASE(no,sequential);
    EXPAND_CASE(no,parallel);
    EXPAND_CASE(no,writespill);
    EXPAND_CASE(no,readspill);
    EXPAND_CASE(no,nolink);
    EXPAND_CASE(no,workflow);
    EXPAND_CASE(no,workflow_action);
    EXPAND_CASE(no,commonspill);
    EXPAND_CASE(no,choosesets);
    EXPAND_CASE(no,regex_find);
    EXPAND_CASE(no,regex_replace);
    EXPAND_CASE(no,workunit_dataset);
    EXPAND_CASE(no,failcode);
    EXPAND_CASE(no,failmessage);
    EXPAND_CASE(no,independent);
    EXPAND_CASE(no,keyed);
    EXPAND_CASE(no,compound);
    EXPAND_CASE(no,checkpoint);
    EXPAND_CASE(no,split);
    EXPAND_CASE(no,spill);
    EXPAND_CASE(no,subgraph);
    EXPAND_CASE(no,dependenton);
    EXPAND_CASE(no,setmeta);
    EXPAND_CASE(no,throughaggregate);
    EXPAND_CASE(no,joincount);
    EXPAND_CASE(no,merge_nomatch);
    EXPAND_CASE(no,countcompare);
    EXPAND_CASE(no,limit);
    EXPAND_CASE(no,evaluate_stmt);
    EXPAND_CASE(no,notify);
    EXPAND_CASE(no,parse);
    EXPAND_CASE(no,newparse);
    EXPAND_CASE(no,skip);
    EXPAND_CASE(no,matched);
    EXPAND_CASE(no,matchtext);
    EXPAND_CASE(no,matchlength);
    EXPAND_CASE(no,matchposition);
    EXPAND_CASE(no,pat_select);
    EXPAND_CASE(no,pat_const);
    EXPAND_CASE(no,pat_pattern);
    EXPAND_CASE(no,pat_follow);
    EXPAND_CASE(no,pat_first);
    EXPAND_CASE(no,pat_last);
    EXPAND_CASE(no,pat_repeat);
    EXPAND_CASE(no,pat_instance);
    EXPAND_CASE(no,pat_anychar);
    EXPAND_CASE(no,pat_token);
    EXPAND_CASE(no,pat_imptoken);
    EXPAND_CASE(no,pat_set);
    EXPAND_CASE(no,pat_checkin);
    EXPAND_CASE(no,pat_x_before_y);
    EXPAND_CASE(no,pat_x_after_y);
    EXPAND_CASE(no,xml);
    EXPAND_CASE(no,compound_fetch);
    EXPAND_CASE(no,pat_index);
    EXPAND_CASE(no,pat_beginpattern);
    EXPAND_CASE(no,pat_endpattern);
    EXPAND_CASE(no,pat_checklength);
    EXPAND_CASE(no,topn);
    EXPAND_CASE(no,outputscalar);
    EXPAND_CASE(no,matchunicode);
    EXPAND_CASE(no,pat_validate);
    EXPAND_CASE(no,regex_findset);
    EXPAND_CASE(no,existsgroup);
    EXPAND_CASE(no,pat_use);
    EXPAND_CASE(no,unused13);
    EXPAND_CASE(no,penalty);
    EXPAND_CASE(no,rowdiff);
    EXPAND_CASE(no,wuid);
    EXPAND_CASE(no,featuretype);
    EXPAND_CASE(no,pat_guard);
    EXPAND_CASE(no,xmltext);
    EXPAND_CASE(no,xmlunicode);
    EXPAND_CASE(no,newxmlparse);
    EXPAND_CASE(no,xmlparse);
    EXPAND_CASE(no,xmldecode);
    EXPAND_CASE(no,xmlencode);
    EXPAND_CASE(no,pat_featureparam);
    EXPAND_CASE(no,pat_featureactual);
    EXPAND_CASE(no,pat_featuredef);
    EXPAND_CASE(no,evalonce);
    EXPAND_CASE(no,unused14);
    EXPAND_CASE(no,merge);
    EXPAND_CASE(no,keyeddistribute);
    EXPAND_CASE(no,distributer);
    EXPAND_CASE(no,impure);
    EXPAND_CASE(no,attr_link);
    EXPAND_CASE(no,attr_expr);
    EXPAND_CASE(no,addsets);
    EXPAND_CASE(no,rowvalue);
    EXPAND_CASE(no,newkeyindex);
    EXPAND_CASE(no,pat_case);
    EXPAND_CASE(no,pat_nocase);
    EXPAND_CASE(no,activetable);
    EXPAND_CASE(no,preload);
    EXPAND_CASE(no,createset);
    EXPAND_CASE(no,assertkeyed);
    EXPAND_CASE(no,assertwild);
    EXPAND_CASE(no,recordlist);
    EXPAND_CASE(no,hashmd5);
    EXPAND_CASE(no,soapcall);
    EXPAND_CASE(no,soapcall_ds);
    EXPAND_CASE(no,newsoapcall);
    EXPAND_CASE(no,newsoapcall_ds);
    EXPAND_CASE(no,soapaction_ds);
    EXPAND_CASE(no,newsoapaction_ds);
    EXPAND_CASE(no,temprow);
    EXPAND_CASE(no,activerow);
    EXPAND_CASE(no,catch);
    EXPAND_CASE(no,unused80);
    EXPAND_CASE(no,reference);
    EXPAND_CASE(no,callback);
    EXPAND_CASE(no,keyedlimit);
    EXPAND_CASE(no,keydiff);
    EXPAND_CASE(no,keypatch);
    EXPAND_CASE(no,returnresult);
    EXPAND_CASE(no,id2blob);
    EXPAND_CASE(no,blob2id);
    EXPAND_CASE(no,anon);
    EXPAND_CASE(no,projectrow);
    EXPAND_CASE(no,embedbody);
    EXPAND_CASE(no,sortpartition);
    EXPAND_CASE(no,define);
    EXPAND_CASE(no,globalscope);
    EXPAND_CASE(no,forcelocal);
    EXPAND_CASE(no,typedef);
    EXPAND_CASE(no,matchattr);
    EXPAND_CASE(no,pat_production);
    EXPAND_CASE(no,guard);
    EXPAND_CASE(no,datasetfromrow);
    EXPAND_CASE(no,createrow);
    EXPAND_CASE(no,selfref);
    EXPAND_CASE(no,unicodeorder);
    EXPAND_CASE(no,assertconstant);
    EXPAND_CASE(no,compound_selectnew);
    EXPAND_CASE(no,nothor);
    EXPAND_CASE(no,newrow);
    EXPAND_CASE(no,clustersize);
    EXPAND_CASE(no,call);
    EXPAND_CASE(no,compound_diskread);
    EXPAND_CASE(no,compound_disknormalize);
    EXPAND_CASE(no,compound_diskaggregate);
    EXPAND_CASE(no,compound_diskcount);
    EXPAND_CASE(no,compound_diskgroupaggregate);
    EXPAND_CASE(no,compound_indexread);
    EXPAND_CASE(no,compound_indexnormalize);
    EXPAND_CASE(no,compound_indexaggregate);
    EXPAND_CASE(no,compound_indexcount);
    EXPAND_CASE(no,compound_indexgroupaggregate);
    EXPAND_CASE(no,compound_childread);
    EXPAND_CASE(no,compound_childnormalize);
    EXPAND_CASE(no,compound_childaggregate);
    EXPAND_CASE(no,compound_childcount);
    EXPAND_CASE(no,compound_childgroupaggregate);
    EXPAND_CASE(no,compound_inline);
    EXPAND_CASE(no,getgraphresult);
    EXPAND_CASE(no,setgraphresult);
    EXPAND_CASE(no,assert);
    EXPAND_CASE(no,assert_ds);
    EXPAND_CASE(no,namedactual);
    EXPAND_CASE(no,combine);
    EXPAND_CASE(no,rows);
    EXPAND_CASE(no,rollupgroup);
    EXPAND_CASE(no,regroup);
    EXPAND_CASE(no,combinegroup);
    EXPAND_CASE(no,inlinetable);
    EXPAND_CASE(no,transformlist);
    EXPAND_CASE(no,variance);
    EXPAND_CASE(no,covariance);
    EXPAND_CASE(no,correlation);
    EXPAND_CASE(no,vargroup);
    EXPAND_CASE(no,covargroup);
    EXPAND_CASE(no,corrgroup);
    EXPAND_CASE(no,denormalizegroup);
    EXPAND_CASE(no,xmlproject);
    EXPAND_CASE(no,spillgraphresult);
    EXPAND_CASE(no,enum);
    EXPAND_CASE(no,pat_or);
    EXPAND_CASE(no,loop);
    EXPAND_CASE(no,loopbody);
    EXPAND_CASE(no,cluster);
    EXPAND_CASE(no,forcenolocal);
    EXPAND_CASE(no,allnodes);
    EXPAND_CASE(no,unused6);
    EXPAND_CASE(no,matchrow);
    EXPAND_CASE(no,sequence);
    EXPAND_CASE(no,selfjoin);
    EXPAND_CASE(no,remotescope);
    EXPAND_CASE(no,privatescope);
    EXPAND_CASE(no,virtualscope);
    EXPAND_CASE(no,concretescope);
    EXPAND_CASE(no,purevirtual);
    EXPAND_CASE(no,internalselect);
    EXPAND_CASE(no,delayedselect);
    EXPAND_CASE(no,pure);
    EXPAND_CASE(no,libraryscope);
    EXPAND_CASE(no,libraryscopeinstance);
    EXPAND_CASE(no,libraryinput);
    EXPAND_CASE(no,pseudods);
    EXPAND_CASE(no,process);
    EXPAND_CASE(no,matchutf8);
    EXPAND_CASE(no,thisnode);
    EXPAND_CASE(no,graphloop);
    EXPAND_CASE(no,rowset);
    EXPAND_CASE(no,loopcounter);
    EXPAND_CASE(no,getgraphloopresult);
    EXPAND_CASE(no,setgraphloopresult);
    EXPAND_CASE(no,rowsetindex);
    EXPAND_CASE(no,rowsetrange);
    EXPAND_CASE(no,assertstepped);
    EXPAND_CASE(no,assertsorted);
    EXPAND_CASE(no,assertgrouped);
    EXPAND_CASE(no,assertdistributed);
    EXPAND_CASE(no,mergejoin);
    EXPAND_CASE(no,datasetlist);
    EXPAND_CASE(no,nwayjoin);
    EXPAND_CASE(no,nwaymerge);
    EXPAND_CASE(no,stepped);
    EXPAND_CASE(no,existslist);
    EXPAND_CASE(no,countlist);
    EXPAND_CASE(no,maxlist);
    EXPAND_CASE(no,minlist);
    EXPAND_CASE(no,sumlist);
    EXPAND_CASE(no,getgraphloopresultset);
    EXPAND_CASE(no,forwardscope);
    EXPAND_CASE(no,pat_before_y);
    EXPAND_CASE(no,pat_after_y);
    EXPAND_CASE(no,extractresult);
    EXPAND_CASE(no,attrname);
    EXPAND_CASE(no,nonempty);
    EXPAND_CASE(no,processing);
    EXPAND_CASE(no,filtergroup);
    EXPAND_CASE(no,rangecommon);
    EXPAND_CASE(no,section);
    EXPAND_CASE(no,nobody);
    EXPAND_CASE(no,deserialize);
    EXPAND_CASE(no,serialize);
    EXPAND_CASE(no,eclcrc);
    EXPAND_CASE(no,top);
    EXPAND_CASE(no,uncommoned_comma);
    EXPAND_CASE(no,nameof);
    EXPAND_CASE(no,catchds);
    EXPAND_CASE(no,file_logicalname);
    EXPAND_CASE(no,toxml);
    EXPAND_CASE(no,tojson);
    EXPAND_CASE(no,sectioninput);
    EXPAND_CASE(no,forcegraph);
    EXPAND_CASE(no,eventextra);
    EXPAND_CASE(no,unused81);
    EXPAND_CASE(no,related);
    EXPAND_CASE(no,executewhen);
    EXPAND_CASE(no,definesideeffect);
    EXPAND_CASE(no,callsideeffect);
    EXPAND_CASE(no,fromxml);
    EXPAND_CASE(no,fromjson);
    EXPAND_CASE(no,actionlist);
    EXPAND_CASE(no,preservemeta);
    EXPAND_CASE(no,normalizegroup);
    EXPAND_CASE(no,indirect);
    EXPAND_CASE(no,selectindirect);
    EXPAND_CASE(no,nohoist);
    EXPAND_CASE(no,merge_pending);
    EXPAND_CASE(no,httpcall);
    EXPAND_CASE(no,getenv);
    EXPAND_CASE(no,json);
    EXPAND_CASE(no,matched_injoin);
    }

    return "<unknown>";
}

const char * getTypeIRText(type_t type)
{
    switch(type)
    {
    EXPAND_CASE(type,boolean);
    EXPAND_CASE(type,int);
    EXPAND_CASE(type,real);
    EXPAND_CASE(type,decimal);
    EXPAND_CASE(type,string);
    EXPAND_CASE(type,date);
    EXPAND_CASE(type,biasedswapint);
    EXPAND_CASE(type,swapfilepos);
    EXPAND_CASE(type,bitfield);
    EXPAND_CASE(type,keyedint);
    EXPAND_CASE(type,char);
    EXPAND_CASE(type,enumerated);
    EXPAND_CASE(type,record);
    EXPAND_CASE(type,varstring);
    EXPAND_CASE(type,blob);
    EXPAND_CASE(type,data);
    EXPAND_CASE(type,pointer);
    EXPAND_CASE(type,class);
    EXPAND_CASE(type,array);
    EXPAND_CASE(type,table);
    EXPAND_CASE(type,set);
    EXPAND_CASE(type,row);
    EXPAND_CASE(type,groupedtable);
    EXPAND_CASE(type,void);
    EXPAND_CASE(type,alien);
    case type_swapint: return "swap";
    EXPAND_CASE(type,filepos);
    EXPAND_CASE(type,none);
    EXPAND_CASE(type,packedint);
    EXPAND_CASE(type,qstring);
    EXPAND_CASE(type,unicode);
    EXPAND_CASE(type,any);
    EXPAND_CASE(type,varunicode);
    EXPAND_CASE(type,pattern);
    EXPAND_CASE(type,rule);
    EXPAND_CASE(type,token);
    EXPAND_CASE(type,feature);
    EXPAND_CASE(type,event);
    EXPAND_CASE(type,null);
    EXPAND_CASE(type,scope);
    EXPAND_CASE(type,utf8);
    EXPAND_CASE(type,transform);
    EXPAND_CASE(type,ifblock);
    EXPAND_CASE(type,function);
    EXPAND_CASE(type,sortlist);
    EXPAND_CASE(type,dictionary);
    EXPAND_CASE(type,alias);
    }
    return "<unknown>";
}

// -----------------------------------------------------------------

// The class performing the building is responsible for mapping its internal representation to and from the types
// returned by the builder interface

typedef unsigned id_t;
typedef id_t typeid_t;
typedef id_t exprid_t;
typedef UnsignedArray IdArray;

inline type_t getRequiredTypeCode(node_operator op)
{
    switch (op)
    {
    //These always have the same type
    case no_assign:
    case no_output:
    case no_sequential:
    case no_parallel:
    case no_apply:
    case no_actionlist:
    case no_orderedactionlist:
        return type_void;
    case no_attr:
    case no_attr_expr:
    case no_attr_link:
    case no_service:
        return type_null;

    //These must never have the type
    case no_record:
    case no_privatescope:
    case no_scope:
    case no_virtualscope:
    case no_concretescope:
    case no_remotescope:
    case no_libraryscope:
    case no_type:
    case no_libraryscopeinstance:
        return type_alias; // type is an alias if itself.
    }
    return type_none;
}

struct ConstantBuilderInfo
{
public:
    ConstantBuilderInfo() : tc(type_none), type(0) {}

public:
    type_t tc;
    typeid_t type;

    union
    {
        unsigned __int64 intValue;
        double realValue;
        struct
        {
            size32_t size;
            const void * data;
        } dataValue;
    };
};

struct SimpleTypeBuilderInfo
{
public:
    SimpleTypeBuilderInfo() : length(0), isSigned(false), precision(0), locale(NULL) {}

public:
    size32_t length;
    size32_t precision;
    bool isSigned;
    const char * locale; // locale or code page
};

struct CompoundTypeBuilderInfo
{
public:
    CompoundTypeBuilderInfo() : baseType(0) {}

public:
    id_t baseType;
};

class TypeAnnotationBuilderInfo
{
public:
    TypeAnnotationBuilderInfo() : type(0), otherExpr(0) {}

public:
    typeid_t type;
    exprid_t otherExpr;
};

class ExprBuilderInfo
{
public:
    ExprBuilderInfo() : type(0), name(NULL), id(NULL), sequence(0) {}

    inline void addOperand(exprid_t id) { args.append((unsigned)id); }

public:
    typeid_t type;
    IAtom * name;
    IIdAtom * id;
    unsigned __int64 sequence;
    IdArray args;
    IdArray special;
    IdArray comment;
};

class ExprAnnotationBuilderInfo
{
public:
    ExprAnnotationBuilderInfo() : expr(0), name(NULL), value(0), line(0), col(0), warning(NULL), tree(NULL) {}

public:
    exprid_t expr;
    const char * name;
    IError * warning;
    IPropertyTree * tree;
    unsigned value;
    unsigned line;
    unsigned col;
    IdArray args;
};


interface IEclBuilder
{
public:
    virtual typeid_t addSimpleType(type_t tc, const SimpleTypeBuilderInfo & info) = 0;
    virtual typeid_t addExprType(type_t tc, exprid_t expr) = 0;
    virtual typeid_t addCompoundType(type_t tc, const CompoundTypeBuilderInfo & info) = 0;
    virtual typeid_t addClassType(const char * name) = 0;
    virtual typeid_t addUnknownType(type_t tc) = 0;
    virtual typeid_t addTypeAnnotation(typemod_t kind, const TypeAnnotationBuilderInfo & info) = 0;

    virtual exprid_t addExpr(node_operator op, const ExprBuilderInfo & info) = 0;
    virtual exprid_t addConstantExpr(const ConstantBuilderInfo & info) = 0;
    virtual exprid_t addExprAnnotation(annotate_kind annot, const ExprAnnotationBuilderInfo & info) = 0;

    virtual void addReturn(exprid_t) = 0;
};

//--------------------------------------------------------------------------------------------------------------------
//- Binary
//--------------------------------------------------------------------------------------------------------------------

class IdMapper
{
public:
    void addMapping(id_t src, id_t target)
    {
        while (targetIds.ordinality() < src)
            targetIds.append(0);
        if (targetIds.ordinality() > src)
        {
            assertex(targetIds.item(src) == 0);
            targetIds.replace(target, (unsigned)src);
        }
        else
            targetIds.append(target);
    }
    id_t lookup(id_t src) const
    {
        if (src == 0)
            return 0;
        assertex(targetIds.isItem(src));
        return targetIds.item(src);
    }

protected:
    UnsignedArray targetIds;
};

//Hardly worth having as a base class at the moment - more for documentation
class CIRPlayer
{
public:
    CIRPlayer(IEclBuilder * _target) : target(_target)
    {
    }

protected:
    IEclBuilder * target;
};

class BinaryIRPlayer : public CIRPlayer
{
    enum {
        IntLengthMask = 0x3f,
        SignedMask = 0x80,
    };

    //Needs to be in a binary format class so shared with writer
    enum
    {
        ElementNone,
        ElementType,
        ElementExpr,
        ElementReturn,
    };

public:
    BinaryIRPlayer(ISimpleReadStream * _in, IEclBuilder * _target) : CIRPlayer(_target), in(_in), seq(0)
    {
    }

    void process();

protected:

    bool processTypeOrExpr();
    exprid_t processExpr();
    typeid_t processType();
    void processReturn();

//Processing for specific types.
    typeid_t readType();
    typeid_t readSimpleType(type_t tc);
    typeid_t readTableType();
    exprid_t readExpr();

//Processing for expressions


//Mapping helpers
    void addMapping(typeid_t srcId, typeid_t tgtId) { idMapper.addMapping(srcId, tgtId); }
    exprid_t mapExprToTarget(exprid_t srcId) const { return idMapper.lookup(srcId); }
    typeid_t mapTypeToTarget(typeid_t srcId) const { return idMapper.lookup(srcId); }

protected:
    //Candidates for being wrapped in a class together with in
    template <class X>
    inline void read(X & x) const { in->read(sizeof(x), &x); }
    template <class X>
    void readPacked(X & value) const { read(value); }
    IIdAtom * readSymId();
    IAtom * readName();

    exprid_t readId() const;

private:
    Owned<ISimpleReadStream> in;
    IdMapper idMapper;
    unsigned seq;
};

void BinaryIRPlayer::process()
{
    seq = 1;
    for (;;)
    {
        byte element;
        read(element);
        switch (element)
        {
        case ElementNone:
            return;
        case ElementType:
            processType();
            break;
        case ElementExpr:
            processExpr();
            break;
        case ElementReturn:
            processReturn();
            break;
        }
    }
}

typeid_t BinaryIRPlayer::processType()
{
    typeid_t id = readType();
    addMapping(seq++, id);
    return id;
}

exprid_t BinaryIRPlayer::processExpr()
{
    exprid_t id = readExpr();
    addMapping(seq++, id);
    return id;
}

exprid_t BinaryIRPlayer::readId() const
{
    exprid_t id;
    readPacked(id);
    return mapExprToTarget(id);
}


//Functions for reading types and processing them
typeid_t BinaryIRPlayer::readType()
{
    type_t tc;
    read(tc);
    switch (tc)
    {
    case type_none:
        return 0;
    case type_alias:
        {
            unsigned refid;
            readPacked(refid);
            return mapTypeToTarget(refid);
        };
    case type_int:
        return readSimpleType(tc);
    case type_table:
        return readTableType();
    default:
        return target->addUnknownType(tc);
    }
    throwUnexpected();
}

typeid_t BinaryIRPlayer::readSimpleType(type_t tc)
{
    SimpleTypeBuilderInfo info;
    switch (tc)
    {
    case type_int:
        {
            //MORE: Worry about packing the binary representation later
            //(or should it be done by passing through zip)
            read(info.length);
            read(info.isSigned);
            break;
        }
    case type_data:
        readPacked(info.length);
        break;
    default:
        throwUnexpected();
    }

    return target->addSimpleType(tc, info);
}

typeid_t BinaryIRPlayer::readTableType()
{
    UNIMPLEMENTED;
    return 0;
}

//Functions for reading expressions and processing them
exprid_t BinaryIRPlayer::readExpr()
{
    node_operator op;
    read(op);
    if (op == no_none)
        return 0;

    ExprBuilderInfo info;
    info.type = readId();
    info.id = readSymId();
    info.name = readName();
    for (;;)
    {
        exprid_t id = readId();
        if (!id)
            break;
        info.addOperand(id);
    }
    return target->addExpr(op, info);
}

void BinaryIRPlayer::processReturn()
{
    exprid_t id = readId();
    target->addReturn(id);
}

IIdAtom * BinaryIRPlayer::readSymId()
{
    return NULL;
}

IAtom * BinaryIRPlayer::readName()
{
    return NULL;
}

//MORE: Compress the binary format
//--------------------------------------------------------------------------------------------------------------------
//- Text
//--------------------------------------------------------------------------------------------------------------------

enum
{
    TIRexpandSimpleTypes    = 0x00000001,
    TIRexpandAttributes     = 0x00000002,
    TIRstripAnnotatations   = 0x00000004,
};
class TextIRBuilder : public CInterfaceOf<IEclBuilder>
{
    class Definition
    {
    public:
        Definition(const char * prefix, id_t _id, bool expandInline) : id(_id)
        {
            if (!expandInline)
            {
                StringAttrBuilder s(idText);
                s.append("%").append(prefix).append(id);
            }
        }

        inline bool expandInline() const { return idText.length() == 0; }

    public:
        StringAttr idText;
        id_t id;
    };

public:
    TextIRBuilder(unsigned _options) : options(_options) {}

    virtual typeid_t addSimpleType(type_t tc, const SimpleTypeBuilderInfo & info)
    {
        bool expandInline = (options & TIRexpandSimpleTypes) != 0;
        Definition def("t", nextId(), expandInline);

        startDefinition(def, "type");
        appendTypeText(tc, info);
        finishDefinition(def);
        return def.id;
    }

    virtual typeid_t addExprType(type_t tc, exprid_t expr)
    {
        Definition def("t", nextId(), false);

        startDefinition(def, "type");
        line.append(getTypeIRText(tc)).append("(");
        appendId(expr).append(")");
        finishDefinition(def);

        return def.id;
    }

    virtual typeid_t addCompoundType(type_t tc, const CompoundTypeBuilderInfo & info)
    {
        Definition def("t", nextId(), false);

        startDefinition(def, "type");
        line.append(getTypeIRText(tc)).append("(");
        appendId(info.baseType).append(")");
        finishDefinition(def);

        return def.id;
    }

    virtual typeid_t addClassType(const char * name)
    {
        Definition def("t", nextId(), false);

        startDefinition(def, "type");
        line.append("class:").append(name);
        finishDefinition(def);

        return def.id;
    }


    virtual typeid_t addUnknownType(type_t tc)
    {
        Definition def("t", nextId(), false);

        startDefinition(def, "type");
        line.append("unknown:").append(getTypeIRText(tc));
        finishDefinition(def);

        return def.id;
    }

    virtual typeid_t addTypeAnnotation(typemod_t kind, const TypeAnnotationBuilderInfo & info)
    {
        if (options & TIRstripAnnotatations)
            return info.type;

        Definition def("t", nextId(), false);

        startDefinition(def, "type");
        appendId(info.type).append(" {");
        switch (kind)
        {
        case typemod_const:
            line.append("const");
            break;
        case typemod_ref:
            line.append("reference");
            break;
        case typemod_wrapper:
            line.append("wrapper");
            break;
        case typemod_builder:
            line.append("builder");
            break;
        case typemod_original:
            line.append("original(");
            appendId(info.otherExpr);
            line.append(")");
            break;
        case typemod_member:
            line.append("member");
            break;
        case typemod_serialized:
            line.append("serialized");
            break;
        case typemod_outofline:
            line.append("outofline");
            break;
        case typemod_attr:
            line.append("attr(");
            appendId(info.otherExpr);
            line.append(")");
            break;
        case typemod_indirect:
            line.append("indirect(");
            appendId(info.otherExpr);
            line.append(")");
            break;
        }
        line.append("}");
        finishDefinition(def);

        return def.id;
    }

    virtual exprid_t addExpr(node_operator op, const ExprBuilderInfo & info)
    {
        bool expandInline = false;
        const char * prefix = "e";
        switch (op)
        {
        case no_attr:
        case no_attr_expr:
            expandInline = (options & TIRexpandAttributes) != 0;
            break;
        case no_assign:
            prefix = "as";
            break;
        }
        Definition def(prefix, nextId(), expandInline);

        startDefinition(def, NULL);
        appendExprText(op, info);
        finishDefinition(def);

        return def.id;
    }

    virtual exprid_t addConstantExpr(const ConstantBuilderInfo & info)
    {
        Definition def("c", nextId(), false);

        startDefinition(def, NULL);
        line.append("constant ");
        appendConstantText(info);
        finishDefinition(def);

        return def.id;
    }

    virtual exprid_t addExprAnnotation(annotate_kind annot, const ExprAnnotationBuilderInfo & info)
    {
        if (options & TIRstripAnnotatations)
            return info.expr;

        Definition def("e", nextId(), false);

        startDefinition(def, NULL);
        appendId(info.expr);
        line.append(" {");
        switch (annot)
        {
        case annotate_symbol:
            line.append("symbol ").append(info.name);
            if (info.value & ob_exported)
                line.append(" exported");
            else if (info.value & ob_shared)
                line.append(" shared");
            if (info.value & ob_virtual)
                line.append(" virtual");
            if (info.line)
                line.append("@").append(info.line);
            break;
        case annotate_location:
            line.append("location '").append(info.name);
            line.append("'");
            if (info.line)
            {
                line.append(",").append(info.line);
                if (info.col)
                    line.append(",").append(info.col);
            }
            break;
        case annotate_meta:
            {
                line.append("meta(");
                ForEachItemIn(i, info.args)
                {
                    if (i!= 0)
                        line.append(",");
                    appendId(info.args.item(i));
                }
                line.append(")");
                break;
            }
        case annotate_warning:
            {
                IError * warning = info.warning;
                StringBuffer msg;
                warning->errorMessage(msg);
                const char * filename = warning->getFilename();
                line.append("warning ");
                line.append("(");
                if (filename)
                {
                    line.append("'");
                    appendStringAsCPP(line, strlen(filename), filename, false);
                    line.append("'");
                }
                line.append(",").append(warning->getLine());
                line.append(",").append(warning->getColumn());
                line.append(",").append(warning->errorCode());
                line.append(",'");
                appendStringAsCPP(line, msg.length(), msg.str(), false);
                line.append("'");
                line.append(",").append(warning->errorAudience());
                line.append(",").append((unsigned)warning->getSeverity());
                line.append(")");
                break;
            }
        case annotate_parsemeta:
            line.append("parsemeta");
            break;
        case annotate_javadoc:
            line.append("javadoc ");
            toXML(info.tree, line);
            break;
        default:
            line.append("unknown");
            break;
        }
        line.append("}");
        finishDefinition(def);

        return def.id;
    }

    virtual void addReturn(exprid_t id)
    {
        line.append("return ");
        appendId(id);

        finishLine();
    }

protected:
    inline id_t nextId() const { return ids.ordinality()+1; }

    id_t createId(const char * prefix)
    {
        id_t nextId = ids.ordinality()+1;
        StringBuffer idText;
        idText.append("%").append(prefix).append(nextId);
        ids.append(idText.str());
        return nextId;
    }

    id_t addId(const char * text)
    {
        id_t nextId = ids.ordinality()+1;
        ids.append(text);
        return nextId;
    }

    void appendTypeText(type_t tc, const SimpleTypeBuilderInfo & info)
    {
        const char * irText = getTypeIRText(tc);
        switch (tc)
        {
        case type_boolean:
        case type_date:
        case type_char:
        case type_event:
        case type_null:
        case type_void:
        case type_sortlist:
        case type_any:
            line.append(irText);
            return;
        case type_packedint:
            if (!info.isSigned)
                line.append("u");
            line.append(irText);
            return;
        case type_filepos:
        case type_int:
        case type_swapint:
            {
                if (!info.isSigned)
                    line.append("u");
                line.append(irText);
                line.append(info.length);
                return;
            }
        case type_real:
        case type_data:
        case type_qstring:
            line.append(irText);
            if (info.length != UNKNOWN_LENGTH)
                line.append(info.length);
            return;
        case type_decimal:
        case type_bitfield:
        case type_enumerated:
            return;
        case type_string:
        case type_unicode:
        case type_varstring:
            line.append(irText);
            if (info.length != UNKNOWN_LENGTH)
                line.append(info.length);
            line.append("(").append(info.locale).append(")");
            return;
        case type_utf8:
            line.append(irText);
            if (info.length != UNKNOWN_LENGTH)
                line.append("_").append(info.length);
            line.append("(").append(info.locale).append(")");
            return;
        }
        line.append(irText).append("(").append(info.length).append(",").append(info.precision).append(",").append(info.isSigned).append(")");
    }

    void appendExprText(node_operator op, const ExprBuilderInfo & info)
    {
        line.append(getOperatorIRText(op));
        if (info.id)
            line.append("#").append(str(info.id));
        else if (info.name)
            line.append("#").append(str(info.name));
        if (info.sequence)
            line.append("[seq(").append(info.sequence).append(")]");

        if (info.args.ordinality())
        {
            line.append("(");
            ForEachItemIn(i, info.args)
            {
                if (i)
                    line.append(",");
                appendId(info.args.item(i));
            }
            line.append(")");
        }
        if (info.special.ordinality())
        {
            line.append("[");
            ForEachItemIn(i, info.special)
            {
                if (i)
                    line.append(",");
                appendId(info.special.item(i));
            }
            line.append("]");
        }
        type_t tc = getRequiredTypeCode(op);
        if (tc == type_none)
        {
            line.append(" : ");
            appendId(info.type);
        }
        if (info.comment.ordinality())
        {
            line.append("  // ");
            ForEachItemIn(i, info.comment)
            {
                if (i)
                    line.append(",");
                appendId(info.comment.item(i));
            }
        }

    }

    void appendConstantText(const ConstantBuilderInfo & info)
    {
        switch (info.tc)
        {
        case type_boolean:
            line.append(info.intValue ? "true" : "false");
            break;
        case type_int:
        case type_swapint:
        case type_packedint:
            {
                if (true)//info.isSigned)
                    line.append((__int64)info.intValue);
                else
                    line.append((unsigned __int64)info.intValue);
                break;
            }
        case type_real:
            {
                line.append(info.realValue);
                break;
            }
        case type_decimal:
        case type_string:
        case type_bitfield:
        case type_enumerated:
        case type_varstring:
        case type_data:
        case type_qstring:
            {
                line.append("D");
                appendStringAsQuotedCPP(line, info.dataValue.size, (const char *)info.dataValue.data, false);
                break;
            }
        }

        line.append(" : ");
        appendId(info.type);
    }

    StringBuffer & appendId(id_t id)
    {
        if (id)
            line.append(ids.item((unsigned)id-1));
        else
            line.append("<null>");
        return line;
    }

    void startDefinition(Definition & def, const char * prefix)
    {
        if (!def.expandInline())
        {
            line.append(def.idText).append(" = ");
            if (prefix)
                line.append(prefix).append(" ");
        }
    }

    void finishDefinition(Definition & def)
    {
        assertex(def.id == ids.ordinality()+1);

        if (!def.expandInline())
        {
            ids.append(def.idText);
            finishLine();
        }
        else
            ids.append(line.str());
        line.clear();
    }

    virtual void finishLine() = 0;

protected:
    unsigned options;
    StringBuffer line;
    StringArray ids;
};

class StringTextIRBuilder : public TextIRBuilder
{
public:
    const char * queryText() { return line.str(); }

protected:
    virtual void finishLine()
    {
        if (line.length())
        {
            line.append(";");
            output.append(line);
            line.clear();
        }
    }

protected:
    StringBuffer output;
};

class DblgLogIRBuilder : public TextIRBuilder
{
public:
    DblgLogIRBuilder(unsigned _options) : TextIRBuilder(_options) {}

protected:
    virtual void finishLine()
    {
        if (line.length())
        {
            DBGLOG("%s;", line.str());
            line.clear();
        }
    }
};

class FileIRBuilder : public TextIRBuilder
{
public:
    FileIRBuilder(unsigned _options, FILE * _file) : TextIRBuilder(_options), file(_file) {}

protected:
    virtual void finishLine()
    {
        if (line.length())
        {
            fputs(line.str(), file);
            fputs(";\n", file);
            line.clear();
        }
    }

protected:
    FILE * file;
};

class StringBufferIRBuilder : public TextIRBuilder
{
public:
    StringBufferIRBuilder(StringBuffer & _target, unsigned _options) : TextIRBuilder(_options), target(_target) {}

protected:
    virtual void finishLine()
    {
        if (line.length())
        {
            target.append(line).append(";\n");
            line.clear();
        }
    }

protected:
    StringBuffer & target;
};

class StringArrayIRBuilder : public TextIRBuilder
{
public:
    StringArrayIRBuilder(StringArray & _target, unsigned _options) : TextIRBuilder(_options), target(_target) {}

protected:
    virtual void finishLine()
    {
        if (line.length())
        {
            line.append(";");
            target.append(line.str());
            line.clear();
        }
    }

protected:
    StringArray & target;
};
//--------------------------------------------------------------------------------------------------------------------
//- XML
//--------------------------------------------------------------------------------------------------------------------


//--------------------------------------------------------------------------------------------------------------------


//--------------------------------------------------------------------------------------------------------------------
//- Expression trees
//--------------------------------------------------------------------------------------------------------------------

class ExpressionIRBuilder : public CInterfaceOf<IEclBuilder>
{
public:
    virtual typeid_t addSimpleType(type_t tc, const SimpleTypeBuilderInfo & info)
    {
        return 0;
    }

    virtual typeid_t addExprType(type_t tc, exprid_t expr)
    {
        return 0;
    }
    virtual typeid_t addCompoundType(type_t tc, const CompoundTypeBuilderInfo & info)
    {
        return 0;
    }
    virtual typeid_t addClassType(const char * name)
    {
        return 0;
    }
    virtual typeid_t addUnknownType(type_t tc)
    {
        return saveItem(makeNullType());
    }

    virtual typeid_t addTypeAnnotation(typemod_t kind, const TypeAnnotationBuilderInfo & info)
    {
        return 0;
    }

    virtual exprid_t addExpr(node_operator op, const ExprBuilderInfo & info)
    {
        return 0;
    }

    virtual exprid_t addConstantExpr(const ConstantBuilderInfo & info)
    {
        UNIMPLEMENTED;
        return 0;
    }

    virtual exprid_t addExprAnnotation(annotate_kind annot, const ExprAnnotationBuilderInfo & info)
    {
        UNIMPLEMENTED;
        return 0;
    }

    virtual void addReturn(exprid_t id)
    {
        IHqlExpression * expr = idToExpr(id);
        results.append(*LINK(expr));
    }

    IHqlExpression * getExpression() { return createCompound(results); }

protected:
    id_t saveItem(IInterface * next)
    {
        values.append(*next);
        return values.ordinality();
    }

    IHqlExpression * idToExpr(id_t id)
    {
        if (id == 0)
            return NULL;
        IInterface & cur = values.item(id-1);
        return static_cast<IHqlExpression *>(&cur);
    }

protected:
    IArray values;
    HqlExprArray results;
};

//--------------------------------------------------------------------------------------------------------------------

class ExpressionId : public CInterfaceOf<IInterface>
{
public:
    ExpressionId(id_t _id) : id(_id) {}

public:
    const id_t id;
};

class ExpressionIRPlayer : public CIRPlayer
{
public:
    ExpressionIRPlayer(IEclBuilder * _target) : CIRPlayer(_target), seq(0)
    {
        lockTransformMutex();
    }
    ~ExpressionIRPlayer()
    {
        unlockTransformMutex();
    }

    void play(IHqlExpression * expr);
    void play(ITypeInfo * type);

protected:
    id_t processType(ITypeInfo * type);
    id_t doProcessType(ITypeInfo * expr);

    id_t processExpr(IHqlExpression * expr);
    id_t doProcessExpr(IHqlExpression * expr);
    id_t doProcessConstant(IHqlExpression * expr);
    id_t doProcessAnnotation(IHqlExpression * expr);

protected:
    ICopyArray types;
    UnsignedArray typeIds;
    unsigned seq;
};

void ExpressionIRPlayer::play(IHqlExpression * expr)
{
    id_t id = processExpr(expr);
    target->addReturn(id);
}

void ExpressionIRPlayer::play(ITypeInfo * type)
{
    id_t id = processType(type);
    target->addReturn(id);
}

//----

id_t ExpressionIRPlayer::processType(ITypeInfo * type)
{
    if (!type)
        return 0;

    unsigned match = types.find(*type);
    if (match != NotFound)
        return typeIds.item(match);

    id_t nextId = doProcessType(type);

    types.append(*type);
    typeIds.append(nextId);
    return nextId;
}

id_t ExpressionIRPlayer::doProcessType(ITypeInfo * type)
{
    typemod_t mod = type->queryModifier();
    if (mod == typemod_none)
    {
        type_t tc = type->getTypeCode();
        SimpleTypeBuilderInfo info;

        switch (tc)
        {
        case type_boolean:
        case type_date:
        case type_char:
        case type_event:
        case type_null:
        case type_void:
        case type_filepos:
        case type_sortlist:
        case type_any:
            break;
        case type_keyedint:
        case type_int:
        case type_swapint:
        case type_real:
        case type_decimal:
        case type_packedint:
            info.length = type->getSize();
            info.isSigned = type->isSigned();
            info.precision = type->getPrecision();
            break;
        case type_unicode:
        case type_varunicode:
        case type_utf8:
            info.length = type->getStringLen();
            info.locale = str(type->queryLocale());
            break;;
        case type_string:
        case type_varstring:
            info.length = type->getStringLen();
            info.locale = str(type->queryCharset()->queryName());
            break;
        case type_bitfield:
            return target->addUnknownType(tc);
        case type_record:
        case type_scope:
        case type_alien:
        case type_enumerated:
            return target->addExprType(tc, processExpr(queryExpression(type)));
        case type_data:
        case type_qstring:
            info.length = type->getStringLen();
            break;
        case type_set:
        case type_row:
        case type_pattern:
        case type_rule:
        case type_token:
        case type_transform:
        case type_table:    // more??
        case type_groupedtable:
        case type_dictionary:
        case type_function:
        case type_pointer:
        case type_array:
            {
                CompoundTypeBuilderInfo info;
                info.baseType = processType(type->queryChildType());
                return target->addCompoundType(tc, info);
            }
        case type_feature:
            return target->addUnknownType(tc);
        case type_none:
        case type_ifblock:
        case type_alias:
        case type_blob:
            throwUnexpected();
            break;
        case type_class:
            return target->addClassType(type->queryTypeName());
        default:
            UNIMPLEMENTED;
        }

        return target->addSimpleType(tc, info);
    }
    else
    {
        TypeAnnotationBuilderInfo info;
        info.type = processType(type->queryTypeBase());
        switch (mod)
        {
        case typemod_original:
        case typemod_attr:
        case typemod_indirect:
            {
                IHqlExpression * expr = static_cast<IHqlExpression *>(type->queryModifierExtra());
                info.otherExpr = processExpr(expr);
                break;
            }
        }
        return target->addTypeAnnotation(mod, info);
    }
}

//----

id_t ExpressionIRPlayer::processExpr(IHqlExpression * expr)
{
    if (!expr)
        return 0;

    IInterface * match = expr->queryTransformExtra();
    if (match)
    {
        //Check for recursion
        if (match == expr)
            throwUnexpected();
        return static_cast<ExpressionId *>(match)->id;
    }
    expr->setTransformExtraUnlinked(expr);

    id_t nextId = doProcessExpr(expr);
    expr->setTransformExtraOwned(new ExpressionId(nextId));
    return nextId;
}

id_t ExpressionIRPlayer::doProcessExpr(IHqlExpression * expr)
{
    IHqlExpression * body = expr->queryBody(true);
    if (body != expr)
        return doProcessAnnotation(expr);

    node_operator op = expr->getOperator();
    switch (op)
    {
    case no_constant:
        return doProcessConstant(expr);
    }

    ExprBuilderInfo info;
    switch (op)
    {
    case no_externalcall:
    case no_record:
    case no_left:
    case no_self:
    case no_selfref:
    case no_right:
        break;
    default:
        info.name = expr->queryName();
        break;
    }

    ForEachChild(i, expr)
        info.args.append(processExpr(expr->queryChild(i)));

#ifdef ADD_ACTIVE_SCOPE_AS_COMMENT
    HqlExprCopyArray inScope;
    expr->gatherTablesUsed(NULL, &inScope);
    ForEachItemIn(i, inScope)
    {
        IHqlExpression * cur = &inScope.item(i);
        if (cur == expr) // add 0 to list if is own active selector
            cur = NULL;
        info.comment.append(processExpr(cur));
    }
#endif

    switch (op)
    {
    case no_externalcall:
        info.special.append(processExpr(expr->queryExternalDefinition()));
        break;
    case no_call:
        info.special.append(processExpr(expr->queryBody()->queryFunctionDefinition()));
        break;
    case no_virtualscope:
    case no_concretescope:
        {
            HqlExprArray scopeSymbols;
            expr->queryScope()->getSymbols(scopeSymbols);
            ForEachItemIn(i, scopeSymbols)
                info.special.append(processExpr(&scopeSymbols.item(i)));
            break;
        }
    case no_type:
        {
            IHqlAlienTypeInfo * alienType = queryAlienType(expr->queryType());
            //MORE: Need to output information about members of the scope, but no functions are avaiable to generate it...
            info.special.append(processExpr(alienType->queryLoadFunction()));
            info.special.append(processExpr(alienType->queryLengthFunction()));
            info.special.append(processExpr(alienType->queryStoreFunction()));
            break;
        }
    }

    if (getRequiredTypeCode(op) == type_none)
        info.type = processType(expr->queryType());
    info.sequence = expr->querySequenceExtra();

    return target->addExpr(op, info);
}

id_t ExpressionIRPlayer::doProcessConstant(IHqlExpression * expr)
{
    IValue * value = expr->queryValue();
    assertex(value);
    ITypeInfo * type = expr->queryType();
    ConstantBuilderInfo info;
    info.tc = type->getTypeCode();
    info.type = processType(type);

    switch (info.tc)
    {
    case type_boolean:
        info.intValue = value->getBoolValue();
        break;
    case type_int:
    case type_swapint:
    case type_packedint:
        info.intValue = value->getIntValue();
        break;
    case type_real:
        info.realValue = value->getRealValue();
        break;
    case type_decimal:
    case type_string:
    case type_bitfield:
    case type_enumerated:
    case type_varstring:
    case type_data:
    case type_qstring:
        info.dataValue.size = value->getSize();
        info.dataValue.data = value->queryValue();
        break;
    }

    return target->addConstantExpr(info);
}

id_t ExpressionIRPlayer::doProcessAnnotation(IHqlExpression * expr)
{
    annotate_kind kind = expr->getAnnotationKind();
    IHqlExpression * body = expr->queryBody(true);

    ExprAnnotationBuilderInfo info;
    Owned<IPropertyTree> javadoc;
    info.expr = processExpr(body);
    switch (kind)
    {
    case annotate_symbol:
        {
            IHqlNamedAnnotation * annotation = static_cast<IHqlNamedAnnotation *>(expr->queryAnnotation());
            info.name = str(expr->queryId());
            info.value = annotation->isExported() ? ob_exported : annotation->isShared() ? ob_shared : 0;
            if (annotation->isVirtual())
                info.value |= ob_virtual;
            info.line = expr->getStartLine();
            info.col = expr->getStartColumn();
            break;
        }
    case annotate_location:
        info.name = str(expr->querySourcePath());
        info.line = expr->getStartLine();
        info.col = expr->getStartColumn();
        break;
    case annotate_meta:
        {
            for (unsigned i=0;; i++)
            {
                IHqlExpression * arg = expr->queryAnnotationParameter(i);
                if (!arg)
                    break;
                info.args.append(processExpr(arg));
            }
            break;
        }
    case annotate_warning:
        info.warning = queryAnnotatedWarning(expr);
        break;
    case annotate_parsemeta:
        //This isn't even used.
        break;
    case annotate_javadoc:
        javadoc.setown(expr->getDocumentation());
        info.tree = javadoc;
        break;
    }

    return target->addExprAnnotation(kind, info);
}

// --------------------------------------------------------------- Exported functions

unsigned defaultDumpOptions = TIRexpandSimpleTypes|TIRexpandAttributes;

extern HQL_API void testIR(IHqlExpression * expr)
{
    ExpressionIRBuilder output;
    ExpressionIRPlayer reader(&output);
    reader.play(expr);

    OwnedHqlExpr result = output.getExpression();
    assertex(expr == result);
}

//-- Dump the IR for the expression(s)/type to stdout ----------------------------------------------------------------

static void playIR(IEclBuilder & output, IHqlExpression * expr, const HqlExprArray * exprs, ITypeInfo * type)
{
    ExpressionIRPlayer reader(&output);
    if (expr)
        reader.play(expr);
    if (exprs)
    {
        ForEachItemIn(i, *exprs)
            reader.play(&exprs->item(i));
    }
    if (type)
        reader.play(type);
}

static void dump_ir(IHqlExpression * expr, const HqlExprArray * exprs, ITypeInfo * type)
{
    printf("\nIR Expression Dumper\n====================\n");
    FileIRBuilder output(defaultDumpOptions, stdout);
    playIR(output, expr, exprs, type);
}

extern HQL_API void dump_ir(IHqlExpression * expr)
{
    dump_ir(expr, NULL, NULL);
}

extern HQL_API void dump_ir(const HqlExprArray & exprs)
{
    dump_ir(NULL, &exprs, NULL);
}

extern HQL_API void dump_ir(ITypeInfo * type)
{
    dump_ir(NULL, NULL, type);
}

extern HQL_API void dump_ir(ITypeInfo * type1, ITypeInfo * type2)
{
    FileIRBuilder output(defaultDumpOptions, stdout);
    ExpressionIRPlayer reader(&output);
    reader.play(type1);
    reader.play(type2);
}

extern HQL_API void dump_ir(IHqlExpression * expr1, IHqlExpression * expr2)
{
    FileIRBuilder output(defaultDumpOptions, stdout);
    ExpressionIRPlayer reader(&output);
    reader.play(expr1);
    reader.play(expr2);
}

extern HQL_API void dump_irn(unsigned n, ...)
{
    FileIRBuilder output(defaultDumpOptions, stdout);
    ExpressionIRPlayer reader(&output);
    va_list args;
    va_start(args, n);
    for (unsigned i=0; i < n;i++)
    {
        IInterface * next = va_arg(args, IInterface *);
        IHqlExpression * expr = dynamic_cast<IHqlExpression *>(next);
        ITypeInfo * type = dynamic_cast<ITypeInfo *>(next);
        if (expr)
            reader.play(expr);
        else if (type)
            reader.play(type);
    }
    va_end(args);
}

//-- Dump the IR for the expression(s)/type to DBGLOG ----------------------------------------------------------------

extern HQL_API void dbglogIR(IHqlExpression * expr)
{
    DblgLogIRBuilder output(defaultDumpOptions);
    playIR(output, expr, NULL, NULL);
}

extern HQL_API void dbglogIR(const HqlExprArray & exprs)
{
    DblgLogIRBuilder output(defaultDumpOptions);
    playIR(output, NULL, &exprs, NULL);
}

extern HQL_API void dbglogIR(ITypeInfo * type)
{
    DblgLogIRBuilder output(defaultDumpOptions);
    playIR(output, NULL, NULL, type);
}

extern HQL_API void dbglogIR(unsigned n, ...)
{
    DblgLogIRBuilder output(defaultDumpOptions);
    ExpressionIRPlayer reader(&output);
    va_list args;
    va_start(args, n);
    for (unsigned i=0; i < n;i++)
    {
        IInterface * next = va_arg(args, IInterface *);
        IHqlExpression * expr = dynamic_cast<IHqlExpression *>(next);
        ITypeInfo * type = dynamic_cast<ITypeInfo *>(next);
        if (expr)
            reader.play(expr);
        else if (type)
            reader.play(type);
    }
    va_end(args);
}


extern HQL_API void getIRText(StringBuffer & target, unsigned options, IHqlExpression * expr)
{
    StringBufferIRBuilder output(target, options);
    ExpressionIRPlayer reader(&output);
    reader.play(expr);
}

extern HQL_API void getIRText(StringArray & target, unsigned options, IHqlExpression * expr)
{
    StringArrayIRBuilder output(target, options);
    ExpressionIRPlayer reader(&output);
    reader.play(expr);
}

extern HQL_API void getIRText(StringBuffer & target, unsigned options, unsigned n, ...)
{
    StringBufferIRBuilder output(target, options);
    ExpressionIRPlayer reader(&output);
    va_list args;
    va_start(args, n);
    for (unsigned i=0; i < n;i++)
    {
        IInterface * next = va_arg(args, IInterface *);
        IHqlExpression * expr = dynamic_cast<IHqlExpression *>(next);
        ITypeInfo * type = dynamic_cast<ITypeInfo *>(next);
        if (expr)
            reader.play(expr);
        else if (type)
            reader.play(type);
    }
    va_end(args);
}

static StringBuffer staticDebuggingStringBuffer;
extern HQL_API const char * getIRText(IHqlExpression * expr)
{
    StringBufferIRBuilder output(staticDebuggingStringBuffer.clear(), defaultDumpOptions);
    playIR(output, expr, NULL, NULL);
    return staticDebuggingStringBuffer.str();
}

extern HQL_API const char * getIRText(ITypeInfo * type)
{
    StringBufferIRBuilder output(staticDebuggingStringBuffer.clear(), defaultDumpOptions);
    playIR(output, NULL, NULL, type);
    return staticDebuggingStringBuffer.str();
}

} // end namespace


#ifdef _USE_CPPUNIT
#include "unittests.hpp"

namespace EclIR
{

// These test queries illustrate the kind of output that is expected from the IR generation
static const char * const testQuery1 = 
"r := RECORD\n"
"  unsigned id;\n"
"END;\n"
"\n"
"r t(unsigned value) := TRANSFORM\n"
"  SELF.id := value;\n"
"END;\n"
"\n"
"ds := DATASET([t(10), t(1 + 10)]);\n"
"OUTPUT(ds);\n";

static const char * const expectedIR1 [] = {
"%t1 = type uint8;",
"%e2 = field#id : %t1;",
"%e3 = record(%e2);",
"%t4 = type record(%e3);",
"%t5 = type row(%t4);",
"%e6 = self(%e3) : %t5;",
"%e7 = select(%e6,%e2) : %t1;",
"%c8 = constant 10 : %t1;",
"%as9 = assign(%e7,%c8);",
"%e10 = %as9 {location '',6,3};",
"%e11 = %e3 {symbol r@1};",
"%t12 = type %t4 {original(%e11)};",
"%t13 = type transform(%t12);",
"%e14 = transform(%e10) : %t13;",
"%e15 = %e14 {symbol t@5};",
"%t16 = type int8;",
"%c17 = constant 1 : %t16;",
"%c18 = constant 10 : %t16;",
"%e19 = add(%c17,%c18) : %t16;",
"%e20 = implicitcast(%e19) : %t1;",
"%as21 = assign(%e7,%e20);",
"%e22 = %as21 {location '',6,3};",
"%e23 = transform(%e22) : %t13;",
"%e24 = %e23 {symbol t@5};",
"%t25 = type null;",
"%e26 = transformlist(%e15,%e24) : %t25;",
"%t27 = type table(%t5);",
"%e28 = inlinetable(%e26,%e3) : %t27;",
"%e29 = %e28 {location '',9,7};",
"%e30 = %e29 {symbol ds@9};",
"%e31 = null : <null>;",
"%e32 = selectfields(%e30,%e31) : %t27;",
"%t33 = type int4;",
"%c34 = constant 706706620 : %t33;",
"%e35 = attr#always;",
"%e36 = attr#update(%c34,%e35);",
"%e37 = output(%e32,%e36);",
"%e38 = %e37 {location '',10,1};",
"%e39 = %e38 {location '',10,1};",
"return %e39;",
NULL
};

class EclIRTests : public CppUnit::TestFixture  
{
    CPPUNIT_TEST_SUITE( EclIRTests );
        CPPUNIT_TEST(testExprToText);
    CPPUNIT_TEST_SUITE_END();

public:
    void compareStringArrays(const StringArray & left, const char * const right[], unsigned test)
    {
        for (unsigned i=0;;i++)
        {
            const char * leftLine = left.isItem(i) ? left.item(i) : NULL;
            const char * rightLine = right[i];
            if (!leftLine && !rightLine)
                return;
            if (!rightLine)
            {
                printf("Test %u@%u: Extra item on left: %s", test, i, leftLine);
                ASSERT(false);
            }
            else if (!leftLine)
            {
                printf("Test %u@%u: Extra item on right: %s", test, i, rightLine);
                ASSERT(false);
            }
            else
            {
                if (strcmp(leftLine, rightLine) != 0)
                {
                    printf("Test %u@%u: Line mismatch: [%s],[%s]", test, i, leftLine, rightLine);
                    ASSERT(false);
                }
            }
        }
    }
    void testExprToText()
    {
        OwnedHqlExpr query = parseQuery(testQuery1, NULL);
        StringArray ir;
        getIRText(ir, 0, query);
        compareStringArrays(ir, expectedIR1, __LINE__);
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION( EclIRTests );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( EclIRTests, "EclIRTests" );

} // end namespace
#endif // USE_CPPUNIT
