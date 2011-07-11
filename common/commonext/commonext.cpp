/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

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

#include "platform.h"
#define CHEAP_UCHAR_DEF
#ifdef _WIN32
typedef wchar_t UChar;
#else 
typedef unsigned short UChar;
#endif 
#include "eclhelper.hpp"

#include "jmisc.hpp"

#include "commonext.hpp"

static const char **kindArray;

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    kindArray = (const char **)malloc(TAKlast * sizeof(const char *));
    memset(kindArray, 0, TAKlast * sizeof(const char *));

    kindArray[TAKnone] = "none";
    kindArray[TAKdiskwrite] = "diskwrite" ;
    kindArray[TAKsort] = "sort" ;
    kindArray[TAKdedup] = "dedup" ;
    kindArray[TAKfilter] = "filter" ;
    kindArray[TAKsplit] = "split" ;
    kindArray[TAKproject] = "project" ;
    kindArray[TAKrollup] = "rollup";
    kindArray[TAKiterate] = "iterate";
    kindArray[TAKaggregate] = "aggregate";
    kindArray[TAKhashaggregate] = "hashaggregate";
    kindArray[TAKfirstn] = "firstn";
    kindArray[TAKsample] = "sample";
    kindArray[TAKdegroup] = "degroup";
    kindArray[TAKjoin] = "join";
    kindArray[TAKhashjoin] = "hashjoin";
    kindArray[TAKlookupjoin] = "lookupjoin";
    kindArray[TAKselfjoin] = "selfjoin";
    kindArray[TAKkeyedjoin] = "keyedjoin";
    kindArray[TAKgroup] = "group";
    kindArray[TAKworkunitwrite] = "workunitwrite";
    kindArray[TAKfunnel] = "funnel";
    kindArray[TAKapply] = "apply";
    kindArray[TAKtemptable] = "temptable";
    kindArray[TAKtemprow] = "temprow";
    kindArray[TAKhashdistribute] = "hashdistribute";
    kindArray[TAKhashdedup] = "hashdedup";
    kindArray[TAKnormalize] = "normalize";
    kindArray[TAKremoteresult] = "remoteresult";
    kindArray[TAKpull] = "pull";
    kindArray[TAKdenormalize] = "denormalize";
    kindArray[TAKnormalizechild] = "normalizechild";
    kindArray[TAKchilddataset] = "childdataset";
    kindArray[TAKselectn] = "selectn";
    kindArray[TAKenth] = "enth";
    kindArray[TAKif] = "if";
    kindArray[TAKnull] = "null";
    kindArray[TAKdistribution] = "distribution";
    kindArray[TAKcountproject] = "countproject";
    kindArray[TAKchoosesets] = "choosesets";
    kindArray[TAKpiperead] = "piperead";
    kindArray[TAKpipewrite] = "pipewrite";
    kindArray[TAKcsvwrite] = "csvwrite";
    kindArray[TAKpipethrough] = "pipethrough";
    kindArray[TAKindexwrite] = "indexwrite";
    kindArray[TAKchoosesetsenth] = "choosesetsenth";
    kindArray[TAKchoosesetslast] = "choosesetslast";
    kindArray[TAKfetch] = "fetch";
    kindArray[TAKhashdenormalize] = "hashdenormalize";
    kindArray[TAKworkunitread] = "workunitread";
    kindArray[TAKthroughaggregate] = "throughaggregate";
    kindArray[TAKspill] = "spill";
    kindArray[TAKcase] = "case";
    kindArray[TAKlimit] = "limit";
    kindArray[TAKcsvfetch] = "csvfetch";
    kindArray[TAKxmlwrite] = "xmlwrite";
    kindArray[TAKparse] = "parse";
    kindArray[TAKtopn] = "topn";
    kindArray[TAKmerge] = "merge";
    kindArray[TAKxmlfetch] = "xmlfetch";
    kindArray[TAKxmlparse] = "xmlparse";
    kindArray[TAKkeyeddistribute] = "keyeddistribute";
    kindArray[TAKjoinlight] = "joinlight";
    kindArray[TAKalljoin] = "alljoin";
    kindArray[TAKsoap_rowdataset] = "SOAP_rowdataset";
    kindArray[TAKsoap_rowaction] = "SOAP_rowaction";
    kindArray[TAKsoap_datasetdataset] = "SOAP_datasetdataset";
    kindArray[TAKsoap_datasetaction] = "SOAP_datasetaction";
    kindArray[TAKkeydiff] = "keydiff";
    kindArray[TAKkeypatch] = "keypatch";
    kindArray[TAKkeyeddenormalize] = "keyeddenormalize";
    kindArray[TAKchilditerator] = "Child Dataset";
    kindArray[TAKdatasetresult] = "Dataset Result";
    kindArray[TAKrowresult] = "Row Result";
    kindArray[TAKchildif] = "childif";
    kindArray[TAKpartition] = "partition";
    kindArray[TAKlocalgraph] = "local graph";
    kindArray[TAKifaction] = "if action";
    kindArray[TAKsequential] = "sequential";
    kindArray[TAKparallel] = "parallel";
    kindArray[TAKemptyaction] = "emptyaction";
    kindArray[TAKskiplimit] = "skip_limit";
    kindArray[TAKdiskread] = "diskread";
    kindArray[TAKdisknormalize] = "disknormalize";
    kindArray[TAKdiskaggregate] = "diskaggregate";
    kindArray[TAKdiskcount] = "diskcount";
    kindArray[TAKdiskgroupaggregate] = "diskgroupaggregate";
    kindArray[TAKindexread] = "indexread";
    kindArray[TAKindexnormalize] = "indexnormalize";
    kindArray[TAKindexaggregate] = "indexaggregate";
    kindArray[TAKindexcount] = "indexcount";
    kindArray[TAKindexgroupaggregate] = "indexgroupaggregate";
    kindArray[TAKchildnormalize] = "childnormalize";
    kindArray[TAKchildaggregate] = "childaggregate";
    kindArray[TAKchildgroupaggregate] = "childgroupaggregate";
    kindArray[TAKchildthroughnormalize] = "childthroughnormalize";
    kindArray[TAKcsvread] = "csvread";
    kindArray[TAKxmlread] = "xmlread";
    kindArray[TAKlocalresultread] = "localresultread";
    kindArray[TAKlocalresultwrite] = "localresultwrite";
    kindArray[TAKcombine] = "combine";
    kindArray[TAKregroup] = "regroup";
    kindArray[TAKrollupgroup] = "rollupgroup";
    kindArray[TAKcombinegroup] = "combinegroup";
    kindArray[TAKlookupdenormalize] = "lookupdenormalize";
    kindArray[TAKalldenormalize] = "alldenormalize";
    kindArray[TAKdenormalizegroup] = "denormalizegroup";
    kindArray[TAKhashdenormalizegroup] = "hashdenormalizegroup";
    kindArray[TAKlookupdenormalizegroup] = "lookupdenormalizegroup";
    kindArray[TAKkeyeddenormalizegroup] = "keyeddenormalizegroup";
    kindArray[TAKalldenormalizegroup] = "alldenormalizegroup";
    kindArray[TAKlocalresultspill] = "localresultspill";
    kindArray[TAKsimpleaction] = "simpleaction";
    kindArray[TAKloopcount] = "loop";
    kindArray[TAKlooprow] = "loop";
    kindArray[TAKloopdataset] = "loop";
    kindArray[TAKchildcase] = "childcase";
    kindArray[TAKremotegraph] = "remote";
    kindArray[TAKlibrarycall] = "librarycall";
    kindArray[TAKrawiterator] = "Child Dataset";
    kindArray[TAKlocalstreamread] = "localstreamread";
    kindArray[TAKprocess] = "process";
    kindArray[TAKgraphloop] ="graph";
    kindArray[TAKparallelgraphloop] = "graph";
    kindArray[TAKgraphloopresultread] = "graphloopread";
    kindArray[TAKgraphloopresultwrite] = "graphloopwrite";
    kindArray[TAKgrouped] = "grouped";
    kindArray[TAKsorted] = "sorted";
    kindArray[TAKdistributed] = "distributed";
    kindArray[TAKnwayjoin] = "nwayjoin";
    kindArray[TAKnwaymerge] = "nwaymerge";
    kindArray[TAKnwaymergejoin] = "nwaymergejoin";
    kindArray[TAKnwayinput] = "nwayinput";
    kindArray[TAKnwaygraphloopresultread] = "nwaygraphloopread";
    kindArray[TAKnwayselect] = "nwayselect";
    kindArray[TAKnonempty] = "nonempty";
    kindArray[TAKcreaterowlimit] = "createrow_limit";
    kindArray[TAKexistsaggregate] = "existsaggregate";
    kindArray[TAKcountaggregate] = "countaggregate";
    kindArray[TAKprefetchproject] = "prefetchproject";
    kindArray[TAKprefetchcountproject] = "prefetchcountproject";
    kindArray[TAKfiltergroup] = "filtergroup";
    kindArray[TAKmemoryspillread] = "memoryspillread";
    kindArray[TAKmemoryspillwrite] = "memoryspillwrite";
    kindArray[TAKmemoryspillsplit] = "memoryspillsplit";
    kindArray[TAKsection] = "section";
    kindArray[TAKlinkedrawiterator] = "linkedrawiterator";
    kindArray[TAKnormalizelinkedchild] = "normalizelinkedchild";
    kindArray[TAKfilterproject] = "filterproject";
    kindArray[TAKcatch] = "catch";
    kindArray[TAKskipcatch] = "skip_catch";
    kindArray[TAKcreaterowcatch] = "createrow_catch";
    kindArray[TAKsectioninput] = "sectioninput";
    kindArray[TAKindexgroupexists] = "indexgroupexists";
    kindArray[TAKindexgroupcount] = "indexgroupcount";
    kindArray[TAKhashdistributemerge] = "hashdistributemerge";
    kindArray[TAKselfjoinlight] = "selfjoinlight";
    kindArray[TAKwhen] = "when";
    kindArray[TAKhttp_rowdataset] = "http";
    kindArray[TAKstreamediterator] = "streamediterator";
    kindArray[TAKexternalsource] = "externalsource";
    kindArray[TAKexternalsink] = "externalsink";
    kindArray[TAKexternalprocess] = "externalprocess";

//Non standard
    kindArray[TAKcountindex] = "countindex";
    kindArray[TAKcountdisk] = "countdisk";
    kindArray[TAKsubgraph] = "subgraph";

    return true;
}

MODULE_EXIT()
{
    free(kindArray);
}

const char *activityKindStr(ThorActivityKind kind) 
{
    const char *ret = kind<TAKlast?kindArray[kind]:NULL;
    if (ret) return ret;
    static char num[32];
    itoa(kind,num,10);
    return num;
}

