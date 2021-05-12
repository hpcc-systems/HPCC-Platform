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

#include "daclient.hpp"
#include "dadfs.hpp"
#include "rmtfile.hpp"
#include "daadmin.hpp"

using namespace daadmin;

#define DEFAULT_DALICONNECT_TIMEOUT 5 // seconds

void doLog(bool noError, StringBuffer &out)
{
    if (noError)
        PROGLOG("%s", out.str());
    else
        UERRLOG("%s", out.str());
}

void usage(const char *exe)
{
  printf("Usage:\n");
  printf("  %s [<daliserver-ip>] <command> { <option> }\n", exe);              
  printf("\n");
  printf("Data store commands:\n");
  printf("  export <branchxpath> <destfile>\n");
  printf("  import <branchxpath> <srcfile>\n");
  printf("  importadd <branchxpath> <srcfile>\n");
  printf("  delete <branchxpath> [nobackup] -- delete branch, 'nobackup' option suppresses writing copy of existing branch\n");
  printf("  set <xpath> <value>        -- set single value\n");
  printf("  get <xpath>                -- get single value\n");
  printf("  bget <xpath> <dest-file>   -- binary property\n");
  printf("  xget <xpath>               -- (multi-value tail can have commas)\n");
  printf("  wget <xpath>               -- (gets all matching xpath)\n");
  printf("  add <xpath> [<value>]      -- adds new xpath node with optional value\n");
  printf("  delv <xpath>               -- deletes value\n");
  printf("  count <xpath>              -- counts xpath matches\n");
  printf("\n");
  printf("Logical File meta information commands:\n");
  printf("  dfsfile <logicalname>          -- get meta information for file\n");
  printf("  dfsmeta <logicalname> <storage> -- get new meta information for file\n");
  printf("  setdfspartattr <logicalname> <part> <attribute> [<value>] -- set attribute of a file part to value, or delete the attribute if not provided\n");
  printf("  dfspart <logicalname> <part>   -- get meta information for part num\n");
  printf("  dfscheck                       -- verify dfs file information is valid\n");
  printf("  dfscsv <logicalnamemask>       -- get csv info. for files matching mask\n");
  printf("  dfsgroup <logicalgroupname> [filename] -- get IPs for logical group (aka cluster). Written to optional filename if provided\n");
  printf("  clusternodes <clustername> [filename] -- get IPs for cluster group. Written to optional filename if provided\n");
  printf("  dfsls [<logicalname>] [options]-- get list of files within a scope (options=lrs)\n");
  printf("  dfsmap <logicalname>           -- get part files (primary and replicates)\n");
  printf("  dfsexists <logicalname>        -- sets return value to 0 if file exists\n");
  printf("  dfsparents <logicalname>       -- list superfiles containing file\n");
  printf("  dfsunlink <logicalname>        -- unlinks file from all super parents\n");
  printf("  dfsverify <logicalname>        -- verifies parts exist, returns 0 if ok\n");
  printf("  setprotect <logicalname> <id>  -- overwrite protects logical file\n");
  printf("  unprotect <logicalname> <id>   -- unprotect (if id=* then clear all)\n");
  printf("  listprotect <logicalnamemask>  <id-mask> -- list protected files\n");
  printf("  checksuperfile <superfilename> [fix=true|false] -- check superfile links consistent and optionally fix\n");
  printf("  checksubfile <subfilename>     -- check subfile links to parent consistent\n");
  printf("  listexpires <logicalnamemask>  -- lists logical files with expiry value\n");
  printf("  listrelationships <primary> <secondary>\n");
  printf("  dfsperm <logicalname>           -- returns LDAP permission for file\n");
  printf("  dfscompratio <logicalname>      -- returns compression ratio of file\n");
  printf("  dfsscopes <mask>                -- lists logical scopes (mask = * for all)\n");
  printf("  cleanscopes                     -- remove empty scopes\n");
  printf("  normalizefilenames [<logicalnamemask>] -- normalize existing logical filenames that match, e.g. .::.::scope::.::name -> scope::name\n");
  printf("  dfsreplication <clustermask> <logicalnamemask> <redundancy-count> [dryrun] -- set redundancy for files matching mask, on specified clusters only\n");
  printf("  holdlock <logicalfile> <read|write> -- hold a lock to the logical-file until a key is pressed");
  printf("\n");
  printf("Workunit commands:\n");
  printf("  listworkunits [<prop>=<val> [<lower> [<upper>]]] -- list workunits that match prop=val in workunit name range lower to upper\n");
  printf("  listmatches <connection xpath> [<match xpath>=<val> [<property xpaths>]] -- <property xpaths> is comma separated list of xpaths\n");
  printf("  workunittimings <WUID>\n");
  printf("\n");
  printf("Other dali server and misc commands:\n");
  printf("  serverlist <mask>               -- list server IPs (mask optional)\n");
  printf("  clusterlist <mask>              -- list clusters   (mask optional)\n");
  printf("  auditlog <fromdate> <todate> <match>\n");
  printf("  coalesce                        -- force transaction coalesce\n");
  printf("  mpping <server-ip>              -- time MP connect\n");
  printf("  daliping [ <num> ]              -- time dali server connect\n");
  printf("  getxref <destxmlfile>           -- get all XREF information\n");
  printf("  dalilocks [ <ip-pattern> ] [ files ] -- get all locked files/xpaths\n");
  printf("  unlock <xpath or logicalfile> <[path|file]> --  unlocks either matching xpath(s) or matching logical file(s), can contain wildcards\n");
  printf("  validatestore [fix=<true|false>]\n"
         "                [verbose=<true|false>]\n"
         "                [deletefiles=<true|false>]-- perform some checks on dali meta data an optionally fix or remove redundant info \n");
  printf("  workunit <workunit> [true]      -- dump workunit xml, if 2nd parameter equals true, will also include progress data\n");
  printf("  wuidcompress <wildcard> <type>  --  scan workunits that match <wildcard> and compress resources of <type>\n");
  printf("  wuiddecompress <wildcard> <type> --  scan workunits that match <wildcard> and decompress resources of <type>\n");
  printf("  xmlsize <filename> [<percentage>] --  analyse size usage in xml file, display individual items above 'percentage' \n");
  printf("  migratefiles <src-group> <target-group> [<filemask>] [dryrun] [createmaps] [listonly] [verbose]\n");
  printf("  translatetoxpath logicalfile [File|SuperFile|Scope]\n");
  printf("  cleanglobalwuid [dryrun] [noreconstruct]\n");
  printf("\n");
  printf("Common options\n");
  printf("  server=<dali-server-ip>         -- server ip\n");
  printf("                                  -- can be 1st param if numeric ip (or '.')\n");
  printf("  user=<username>                 -- for file operations\n");
  printf("  password=<password>             -- for file operations\n");
  printf("  logfile=<filename>              -- filename blank for no log\n");
  printf("  rawlog=0|1                      -- if raw omits timestamps etc\n");
  printf("  timeout=<seconds>               -- set dali connect timeout\n");
}

#define CHECKPARAMS(mn,mx) { if ((np<mn)||(np>mx)) throw MakeStringException(-1,"%s: incorrect number of parameters",cmd); }

static constexpr const char * defaultYaml = R"!!(
version: "1.0"
daliadmin:
  name: daliadmin
)!!";

int main(int argc, const char* argv[])
{
    int ret = 0;
    InitModuleObjects();
    EnableSEHtoExceptionMapping();
    setDaliServixSocketCaching(true);
    if (argc<2) {
        usage(argv[0]);
        return -1;
    }

    Owned<IPropertyTree> globals = loadConfiguration(defaultYaml, argv, "daliadmin", "DALIADMIN", "daliadmin.xml", nullptr, nullptr, false);
    Owned<IProperties> props = createProperties("daliadmin.ini");
    StringArray params;
    SocketEndpoint ep;
    StringBuffer tmps;
    for (int i=1;i<argc;i++) {
        const char *param = argv[i];
        if ((memcmp(param,"server=",7)==0)||
            (memcmp(param,"logfile=",8)==0)||
            (memcmp(param,"rawlog=",7)==0)||
            (memcmp(param,"user=",5)==0)||
            (memcmp(param,"password=",9)==0) ||
            (memcmp(param,"fix=",4)==0) ||
            (memcmp(param,"verbose=",8)==0) ||
            (memcmp(param,"deletefiles=",12)==0) ||
            (memcmp(param,"timeout=",8)==0))
            props->loadProp(param);
        else if ((i==1)&&(isdigit(*param)||(*param=='.'))&&ep.set(((*param=='.')&&param[1])?(param+1):param,DALI_SERVER_PORT))
            props->setProp("server",ep.getUrlStr(tmps.clear()).str());
        else {
            if ((strieq(param,"help")) || (strieq(param,"-help")) || (strieq(param,"--help"))) {
                usage(argv[0]);
                return -1;
            }
            params.append(param);
        }
    }
    if (!params.ordinality()) {
        usage(argv[0]);
        return -1;
    }

    try {
        StringBuffer logname;
        StringBuffer aliasname;
        bool rawlog = props->getPropBool("rawlog");
        Owned<ILogMsgHandler> fileMsgHandler;
        if (props->getProp("logfile",logname)) {
            if (logname.length()) {
                fileMsgHandler.setown(getFileLogMsgHandler(logname.str(), NULL, rawlog?MSGFIELD_prefix:MSGFIELD_STANDARD, false, false, true));
                queryLogMsgManager()->addMonitorOwn(fileMsgHandler.getClear(), getCategoryLogMsgFilter(MSGAUD_all, MSGCLS_all, TopDetail));
            }
        }
        // set stdout 
        attachStandardHandleLogMsgMonitor(stdout,0,MSGAUD_all,MSGCLS_all&~(MSGCLS_disaster|MSGCLS_error|MSGCLS_warning));
        Owned<ILogMsgFilter> filter = getCategoryLogMsgFilter(MSGAUD_user, MSGCLS_error|MSGCLS_warning);
        queryLogMsgManager()->changeMonitorFilter(queryStderrLogMsgHandler(), filter);
        queryStderrLogMsgHandler()->setMessageFields(MSGFIELD_prefix);
    }
    catch (IException *e) {
        pexception("daliadmin",e);
        e->Release();
        ret = 255;
    }
    unsigned daliconnectelapsed;
    StringBuffer daliserv;
    if (!ret) {
        const char *cmd = params.item(0);
        unsigned np = params.ordinality()-1;

        if (!props->getProp("server",daliserv.clear()))
        {
            // external commands
            try
            {
                if (strieq(cmd,"xmlsize"))
                {
                    CHECKPARAMS(1,2);
                    xmlSize(params.item(1), np>1?atof(params.item(2)):1.0);
                }
                else if (strieq(cmd,"translatetoxpath"))
                {
                    CHECKPARAMS(1,2);
                    DfsXmlBranchKind branchType;
                    if (np>1)
                    {
                        const char *typeStr = params.item(2);
                        branchType = queryDfsXmlBranchType(typeStr);
                    }
                    else
                        branchType = DXB_File;
                    translateToXpath(params.item(1), branchType);
                }
                else
                {
                    UERRLOG("Unknown command %s",cmd);
                    ret = 255;
                }
            }
            catch (IException *e)
            {
                EXCLOG(e,"daliadmin");
                e->Release();
                ret = 255;
            }
            return ret;
        }
        else
        {
            try {
                SocketEndpoint ep(daliserv.str(),DALI_SERVER_PORT);
                SocketEndpointArray epa;
                epa.append(ep);
                Owned<IGroup> group = createIGroup(epa);
                unsigned start = msTick();
                initClientProcess(group, DCR_DaliAdmin);
                daliconnectelapsed = msTick()-start;
            }
            catch (IException *e) {
                EXCLOG(e,"daliadmin initClientProcess");
                e->Release();
                ret = 254;
            }
            if (!ret) {
                try {
                    Owned<IUserDescriptor> userDesc;
                    if (props->getProp("user",tmps.clear())) {
                        userDesc.setown(createUserDescriptor());
                        StringBuffer ps;
                        props->getProp("password",ps);
                        userDesc->set(tmps.str(),ps.str());
                        queryDistributedFileDirectory().setDefaultUser(userDesc);
                    }
                    StringBuffer out;
                    setDaliConnectTimeoutMs(1000 * props->getPropInt("timeout", DEFAULT_DALICONNECT_TIMEOUT));
                    if (strieq(cmd,"export")) {
                        CHECKPARAMS(2,2);
                        exportToFile(params.item(1),params.item(2));
                    }
                    else if (strieq(cmd,"import")) {
                        CHECKPARAMS(2,2);
                        doLog(importFromFile(params.item(1),params.item(2),false,out),out);
                    }
                    else if (strieq(cmd,"importadd")) {
                        CHECKPARAMS(2,2);
                        doLog(importFromFile(params.item(1),params.item(2),true,out),out);
                    }
                    else if (strieq(cmd,"delete")) {
                        CHECKPARAMS(1,2);
                        bool backup = np<2 || !strieq("nobackup", params.item(2));
                        doLog(erase(params.item(1),backup,out),out);
                    }
                    else if (strieq(cmd,"set")) {
                        CHECKPARAMS(2,2);
                        setValue(params.item(1),params.item(2),out);
                        PROGLOG("Changed %s from '%s' to '%s'",params.item(1),out.str(),params.item(2));
                    }
                    else if (strieq(cmd,"get")) {
                        CHECKPARAMS(1,1);
                        getValue(params.item(1),out);
                        PROGLOG("Value of %s is: '%s'",params.item(1),out.str());
                    }
                    else if (strieq(cmd,"bget")) {
                        CHECKPARAMS(2,2);
                        bget(params.item(1),params.item(2));
                    }
                    else if (strieq(cmd,"wget")) {
                        CHECKPARAMS(1,1);
                        wget(params.item(1));
                    }
                    else if (strieq(cmd,"xget")) {
                        CHECKPARAMS(1,1);
                        wget(params.item(1));
                    }
                    else if (strieq(cmd,"add")) {
                        CHECKPARAMS(1,2);
                        doLog(add(params.item(1),(np>1) ? params.item(2) : nullptr,out),out);
                    }
                    else if (strieq(cmd,"delv")) {
                        CHECKPARAMS(1,1);
                        delv(params.item(1));
                    }
                    else if (strieq(cmd,"count")) {
                        CHECKPARAMS(1,1);
                        PROGLOG("Count of %s is: %d",params.item(1),count(params.item(1)));
                    }
                    else if (strieq(cmd,"dfsfile")) {
                        CHECKPARAMS(1,1);
                        doLog(dfsfile(params.item(1),userDesc,out),out);
                    }
                    else if (strieq(cmd,"dfsmeta")) {
                        CHECKPARAMS(1,3);
                        bool includeStorage = (np < 2) || strToBool(params.item(2));
                        dfsmeta(params.item(1),userDesc,includeStorage);
                    }
                    else if (strieq(cmd,"dfspart")) {
                        CHECKPARAMS(2,2);
                        doLog(dfspart(params.item(1),userDesc,atoi(params.item(2)),out),out);                        
                    }
                    else if (strieq(cmd,"setdfspartattr")) {
                        CHECKPARAMS(3,4);
                        setdfspartattr(params.item(1),atoi(params.item(2)),params.item(3),np>3?params.item(4):nullptr,userDesc,out);
                        PROGLOG("%s", out.str());
                    }
                    else if (strieq(cmd,"dfscheck")) {
                        CHECKPARAMS(0,0);
                        doLog(dfsCheck(out),out);
                    }
                    else if (strieq(cmd,"dfscsv")) {
                        CHECKPARAMS(1,1);
                        dfscsv(params.item(1),userDesc,out);
                        PROGLOG("%s",out.str());
                    }
                    else if (strieq(cmd,"dfsgroup")) {
                        CHECKPARAMS(1,2);
                        dfsGroup(params.item(1),(np>1)?params.item(2):NULL);
                    }
                    else if (strieq(cmd,"clusternodes")) {
                        CHECKPARAMS(1,2);
                        ret = clusterGroup(params.item(1),(np>1)?params.item(2):NULL);
                    }
                    else if (strieq(cmd,"dfsls")) {
                        CHECKPARAMS(0,2);
                        doLog(dfsLs((np>0)?params.item(1):nullptr,(np>1)?params.item(2):nullptr,out),out);
                    }
                    else if (strieq(cmd,"dfsmap")) {
                        CHECKPARAMS(1,1);
                        doLog(dfsmap(params.item(1),userDesc,out),out);
                    }
                    else if (strieq(cmd,"dfsexists") || strieq(cmd,"dfsexist")) {
                        // NB: "dfsexist" typo', kept for backward compatibility only (<7.12)
                        CHECKPARAMS(1,1);
                        ret = dfsexists(params.item(1),userDesc);
                    }
                    else if (strieq(cmd,"dfsparents")) {
                        CHECKPARAMS(1,1);
                        dfsparents(params.item(1),userDesc,out);
                        PROGLOG("%s",out.str());
                    }
                    else if (strieq(cmd,"dfsunlink")) {
                        CHECKPARAMS(1,1);
                        dfsunlink(params.item(1),userDesc);
                    }
                    else if (strieq(cmd,"dfsverify")) {
                        CHECKPARAMS(1,1);
                        ret = dfsverify(params.item(1),NULL,userDesc);
                    }
                    else if (strieq(cmd,"setprotect")) {
                        CHECKPARAMS(2,2);
                        setprotect(params.item(1),params.item(2),userDesc);
                    }
                    else if (strieq(cmd,"unprotect")) {
                        CHECKPARAMS(2,2);
                        unprotect(params.item(1),params.item(2),userDesc);
                    }
                    else if (strieq(cmd,"listprotect")) {
                        CHECKPARAMS(0,2);
                        listprotect((np>1)?params.item(1):"*",(np>2)?params.item(2):"*");

                    }
                    else if (strieq(cmd,"checksuperfile")) {
                        CHECKPARAMS(1,1);
                        bool fix = props->getPropBool("fix");
                        checksuperfile(params.item(1),fix);
                    }
                    else if (strieq(cmd,"checksubfile")) {
                        CHECKPARAMS(1,1);
                        checksubfile(params.item(1));
                    }
                    else if (strieq(cmd,"listexpires")) {
                        CHECKPARAMS(0,1);
                        listexpires((np>1)?params.item(1):"*",userDesc);
                    }
                    else if (strieq(cmd,"listrelationships")) {
                        CHECKPARAMS(2,2);
                        listrelationships(params.item(1),params.item(2));
                    }
                    else if (strieq(cmd,"dfsperm")) {
                        if (!userDesc.get())
                            throw MakeStringException(-1,"dfsperm requires username to be set (user=)");
                        CHECKPARAMS(1,1);
                        ret = dfsperm(params.item(1),userDesc);
                    }
                    else if (strieq(cmd,"dfscompratio")) {
                        CHECKPARAMS(1,1);
                        dfscompratio(params.item(1),userDesc);
                    }
                    else if (strieq(cmd,"dfsscopes")) {
                        CHECKPARAMS(0,1);
                        dfsscopes((np>0)?params.item(1):"*",userDesc);
                    }
                    else if (strieq(cmd,"cleanscopes")) {
                        CHECKPARAMS(0,0);
                        cleanscopes(userDesc);
                    }
                    else if (strieq(cmd,"normalizefilenames")) {
                        CHECKPARAMS(0,1);
                        normalizeFileNames(userDesc, np>0 ? params.item(1) : nullptr);
                    }
                    else if (strieq(cmd,"listworkunits")) {
                        CHECKPARAMS(0,3);
                        listworkunits((np>0)?params.item(1):NULL,(np>1)?params.item(2):NULL,(np>2)?params.item(3):NULL);
                    }
                    else if (strieq(cmd,"listmatches")) {
                        CHECKPARAMS(0,3);
                        listmatches((np>0)?params.item(1):NULL,(np>1)?params.item(2):NULL,(np>2)?params.item(3):NULL);
                    }
                    else if (strieq(cmd,"workunittimings")) {
                        CHECKPARAMS(1,1);
                        workunittimings(params.item(1));
                    }
                    else if (strieq(cmd,"serverlist")) {
                        CHECKPARAMS(1,1);
                        serverlist(params.item(1));
                    }
                    else if (strieq(cmd,"clusterlist")) {
                        CHECKPARAMS(1,1);
                        clusterlist(params.item(1));
                    }
                    else if (strieq(cmd,"auditlog")) {
                        CHECKPARAMS(2,3);
                        auditlog(params.item(1),params.item(2),(np>2)?params.item(3):NULL);
                    }
                    else if (strieq(cmd,"coalesce")) {
                        CHECKPARAMS(0,0);
                        coalesce();
                    }
                    else if (strieq(cmd,"mpping")) {
                        CHECKPARAMS(1,1);
                        mpping(params.item(1));
                    }
                    else if (strieq(cmd,"daliping")) {
                        CHECKPARAMS(0,1);
                        daliping(daliserv.str(),daliconnectelapsed,(np>0)?atoi(params.item(1)):1);
                    }
                    else if (strieq(cmd,"getxref")) {
                        CHECKPARAMS(1,1);
                        getxref(params.item(1));
                    }
                    else if (strieq(cmd,"dalilocks")) {
                        CHECKPARAMS(0,2);
                        bool filesonly = false;
                        if (np&&(strieq(params.item(np),"files"))) {
                            filesonly = true;
                            np--;
                        }
                        dalilocks(np>0?params.item(1):NULL,filesonly);
                    }
                    else if (strieq(cmd,"unlock")) {
                        CHECKPARAMS(2,2);
                        const char *fileOrPath = params.item(2);
                        if (strieq("file", fileOrPath))
                            unlock(params.item(1), true);
                        else if (strieq("path", fileOrPath))
                            unlock(params.item(1), false);
                        else
                            throw MakeStringException(0, "unknown type [ %s ], must be 'file' or 'path'", fileOrPath);
                    }
                    else if (strieq(cmd,"validateStore")) {
                        CHECKPARAMS(0,2);
                        bool fix = props->getPropBool("fix");
                        bool verbose = props->getPropBool("verbose");
                        bool deleteFiles = props->getPropBool("deletefiles");
                        validateStore(fix, deleteFiles, verbose);
                    }
                    else if (strieq(cmd, "workunit")) {
                        CHECKPARAMS(1,2);
                        bool includeProgress=false;
                        if (np>1)
                            includeProgress = strToBool(params.item(2));
                        dumpWorkunit(params.item(1), includeProgress);
                    }
                    else if (strieq(cmd,"wuidCompress")) {
                        CHECKPARAMS(2,2);
                        wuidCompress(params.item(1), params.item(2), true);
                    }
                    else if (strieq(cmd,"wuidDecompress")) {
                        CHECKPARAMS(2,2);
                        wuidCompress(params.item(1), params.item(2), false);
                    }
                    else if (strieq(cmd,"dfsreplication")) {
                        CHECKPARAMS(3,4);
                        bool dryRun = np>3 && strieq("dryrun", params.item(4));
                        dfsreplication(params.item(1), params.item(2), atoi(params.item(3)), dryRun);
                    }
                    else if (strieq(cmd,"holdlock")) {
                        CHECKPARAMS(2,2);
                        holdlock(params.item(1), params.item(2), userDesc);
                    }
                    else if (strieq(cmd, "progress")) {
                        CHECKPARAMS(2,2);
                        dumpProgress(params.item(1), params.item(2));
                    }
                    else if (strieq(cmd, "migratefiles"))
                    {
                        CHECKPARAMS(2, 7);
                        const char *srcGroup = params.item(1);
                        const char *dstGroup = params.item(2);
                        const char *filemask = "*";
                        StringBuffer options;
                        if (params.isItem(3))
                        {
                            filemask = params.item(3);
                            unsigned arg=4;
                            StringArray optArray;
                            while (arg<params.ordinality())
                                optArray.append(params.item(arg++));
                            optArray.getString(options, ",");
                        }
                        migrateFiles(srcGroup, dstGroup, filemask, options);
                    }
                    else if (stricmp(cmd, "wuattr") == 0) {
                        CHECKPARAMS(1, 2);
                        if (params.ordinality() > 2)
                            dumpWorkunitAttr(params.item(1), params.item(2));
                        else
                            dumpWorkunitAttr(params.item(1), nullptr);
                    }
                    else if (strieq(cmd, "cleanglobalwuid"))
                    {
                        CHECKPARAMS(0, 2);
                        bool dryrun = false;
                        bool reconstruct = true;
                        for (unsigned i=1; i<params.ordinality(); i++)
                        {
                            const char *param = params.item(i);
                            if (strieq("dryrun", param))
                                dryrun = true;
                            else if (strieq("noreconstruct", param))
                                reconstruct = false;
                        }
                        removeOrphanedGlobalVariables(dryrun, reconstruct);
                    }
                    else
                        UERRLOG("Unknown command %s",cmd);
                }
                catch (IException *e)
                {
                    EXCLOG(e,"daliadmin");
                    e->Release();
                    ret = 255;
                }
                closedownClientProcess();
            }
        }
    }
    setDaliServixSocketCaching(false);
    setNodeCaching(false);
    releaseAtoms();
    fflush(stdout);
    fflush(stderr);
    return ret;
}