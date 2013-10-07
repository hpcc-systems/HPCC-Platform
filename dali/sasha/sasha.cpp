#include "platform.h"
#include <stdio.h>
#include "jlib.hpp"
#include "jio.hpp"
#include "jmisc.hpp"
#include "jfile.hpp"
#include "jsuperhash.hpp"
#include "mpbase.hpp"
#include "mpcomm.hpp"
#include "portlist.h"
#ifdef _WIN32
#include <conio.h>
#else
#define _getch getchar
#define _putch putchar
#endif

#include "sacmd.hpp"

bool restoreWU(const char * sashaserver,const char *wuid)
{
    Owned<ISashaCommand> cmd = createSashaCommand();
    cmd->setAction(SCA_RESTORE);
    cmd->addId(wuid);
    SocketEndpoint ep(sashaserver);
    Owned<INode> node = createINode(ep);
    if (!cmd->send(node,1*60*1000)) {
        ERRLOG("Could not connect to Sasha server at %s",sashaserver);
        return false;
    }
    if (cmd->numIds()==0) {
        PROGLOG("Could not restore %s",wuid);
        return true;
    }
    StringBuffer reply;
    cmd->getId(0,reply);
    PROGLOG("%s",reply.str());
    return true;
}

bool listWUs(const char * sashaserver,const char *wuid)
{
    Owned<ISashaCommand> cmd = createSashaCommand();
    cmd->setAction(SCA_LIST);       // get WU info
    cmd->setArchived(true);         // include archived WUs
    cmd->setAfter("200401010000");      
    cmd->setBefore("200402010000");             // jan 2004
    cmd->setOwner("nigel");         
    cmd->setState("failed");
    SocketEndpoint ep(sashaserver);
    Owned<INode> node = createINode(ep);
    if (!cmd->send(node,1*60*1000)) {
        ERRLOG("Could not connect to Sasha server at %s",sashaserver);
        return false;
    }
    unsigned n = cmd->numIds();
    StringBuffer s;
    for (unsigned i=0;i<n;i++) {
        cmd->getId(i,s.clear());
        printf("%s\n",s.str());
    }
    return true;
}


//#define TRACE

#define SDS_CONNECT_TIMEOUT  (1000*60*60*2)     // better than infinite

void usage()
{
    printf("SASHA usage:\n");
    printf("  sasha server=<sasha-server-ip> action=VERSION\n");
    printf("  sasha server=<sasha-server-ip> action=LIST    <wu-specifier>\n");
    printf("  sasha server=<sasha-server-ip> action=GET     <wu-specifier>\n");
    printf("  sasha server=<sasha-server-ip> action=RESTORE <wu-specifier>\n");
    printf("  sasha server=<sasha-server-ip> action=ARCHIVE <wu-specifier>\n");
    printf("  sasha server=<sasha-server-ip> action=BACKUP  <wu-specifier>\n");
    printf("  sasha server=<sasha-server-ip> action=XREF    cluster=<clustername>\n");
    printf("  sasha server=<sasha-server-ip> action=LISTDT  <wu-specifier>\n");
    printf("  sasha server=<sasha-server-ip> action=STOP\n");
    printf("where:\n");
    printf("  <wu-specifier> is one or more of:\n");
    printf("        wuid=<WU-name>       --  can contain ? and or *\n");
    printf("        owner=<username>\n");
    printf("        cluster=<clustername>\n");
    printf("        jobname=<job-name>\n");
    printf("        state=<WU-state>\n");
    printf("        before=YYYYMMDD\n");
    printf("        after=YYYYMMDD\n");
    printf("        dfu=1                -- lookup DFU WUs rather than ECL WUs\n");
    printf("        output=<output-list> -- comma separated list of:\n");
    printf("                             -- owner,cluster,jobname,state\n");
    printf("        limit=<number>       -- maximum number of results\n");
    printf("        archived=1           -- lookup in archived (default)\n");
    printf("        online=1             -- lookup in online WUs\n");
    printf("        outfile=<filename>   -- filename for output text\n");
    printf("        xslfile=<filename>   -- XSL transform for GET results\n");
    printf("examples:\n");
    printf("    sasha server=10.150.10.75 action=archive wuid=W20040514-123412\n");
    printf("    sasha server=10.150.10.75 action=list owner=nhicks\n");
    printf("    sasha server=10.150.10.75 action=list after=20040101\n");                
    printf("    sasha server=10.150.10.75 action=list state=failed output=owner,jobname\n");                
    printf("    sasha server=10.150.10.75 action=restore W20040514-123412\n");
    printf("    sasha server=10.150.10.75 action=list dfu=1 online=1 owner=nhicks\n");
    exit(1);
}

static Owned<IFileIOStream> outfile;
static bool extractTimings=true;

bool getResponse()
{
    int ch;
    do
    {
        ch = toupper(ch = _getch());
    } while (ch != 'Y' && ch != 'N');
    printf("%c\n",ch);
    return ch=='Y' ? true : false;
}

bool confirm(const char * msg)
{
    printf("%s",msg);
    return getResponse();
}

static const char *getNum(const char *s,unsigned &num)
{
    while (*s&&!isdigit(*s))
        s++;
    num = 0;
    while (isdigit(*s)) {
        num = num*10+*s-'0';
        s++;
    }
    return s;
}


void DumpWorkunitTimings(IPropertyTree *wu)
{
    Owned<IFile> file;
    Owned<IFileIO> fileio;
    offset_t filepos = 0;
    const char *basename = "DaAudit.";
    StringBuffer curfilename;
    StringBuffer wuid;
    wu->getName(wuid);
    StringBuffer query;
    StringBuffer sdate;
    StringBuffer name;
    CDateTime dt;
    StringBuffer line;
    const char *submitid = wu->queryProp("@submitID");
    if (!submitid)
        submitid = "";
    Owned<IPropertyTreeIterator> iter = wu->getElements("Timings/Timing");
    ForEach(*iter) {
        if (iter->query().getProp("@name",name.clear())) {
            if ((name.length()>11)&&(memcmp("Graph graph",name.str(),11)==0)) {
                unsigned gn;
                const char *s = getNum(name.str(),gn);
                unsigned sn;
                s = getNum(s,sn);
                if (gn&&sn) {
                    query.clear().appendf("TimeStamps/TimeStamp[@application=\"Thor - graph%d\"]/Started[1]",gn);
                    if (wu->getProp(query.str(),sdate.clear())) {
                        dt.setString(sdate.str());
                        unsigned year;
                        unsigned month;
                        unsigned day;
                        dt.getDate(year,month,day);
                        StringBuffer logname(basename);
                        logname.appendf("%04d_%02d_%02d.log",year,month,day);
//                      printf("**%s\n",logname.str());
                        if (strcmp(logname.str(),curfilename.str())!=0) {
                            fileio.clear();
                            file.setown(createIFile(logname.str()));
                            if (!file) 
                                throw MakeStringException(-1,"Could not create file %s",logname.str());
                            fileio.setown(file->open(IFOwrite));
                            if (!fileio) 
                                throw MakeStringException(-1,"Could not open file %s",logname.str());
                            filepos = fileio->size();
                            curfilename.clear().append(logname);
                        }
                        dt.getDateString(line.clear());
                        line.append(' ');
                        dt.getTimeString(line);
                        line.append(" ,Timing,").append(wuid.str()).append(',').append(submitid).append(',');
                        iter->query().getProp("@duration",line);
                        line.append(',');
                        if (iter->query().getProp("@count",line))
                            line.append(',');
                        line.append("ThorGraph").append(',').append(gn).append(',').append(sn).append('\n');
                        fileio->write(filepos,line.length(),line.str());
                        filepos += line.length();
                    }
                }
            }
        }
    }
}

ISashaCommand *createCommand(unsigned argc, char* argv[], SocketEndpoint &serverep)
{
    bool needloadwu=false;
    bool iswild=false;
    if (argc<3)
        return NULL;
    Owned<ISashaCommand> cmd = createSashaCommand();
    unsigned i = 1;
    StringBuffer xcmd;
    while (i<argc) {
        const char *arg = argv[i++];
        const char *tail;
        if ((tail=strchr(arg,'='))==NULL) {
            if ((stricmp(arg,"list")==0)||
                (stricmp(arg,"listdt")==0)||
                (stricmp(arg,"backup")==0)||
                (stricmp(arg,"xref")==0)||
                (stricmp(arg,"archive")==0)||
                (stricmp(arg,"stop")==0))
                arg = xcmd.clear().append("action=").append(arg);
            else if ((strchr(arg,'*')!=NULL)||(strchr(arg,'?')!=NULL)|| 
                     ((toupper(arg[0])=='W')&&(arg[1]=='2'))||
                     ((toupper(arg[0])=='D')&&(arg[1]=='2')))
                arg = xcmd.clear().append("wuid=").append(arg);
            else if (stricmp(arg,"dfu")==0)
                arg = xcmd.clear().append("dfu=1");
            else if (stricmp(arg,"online")==0)
                arg = xcmd.clear().append("online=1");
            else if (stricmp(arg,"archived")==0)
                arg = xcmd.clear().append("archived=1");
            else {
                SocketEndpoint ep;
                if (isdigit(arg[0])) 
                    ep.set(arg,DEFAULT_SASHA_PORT);             
                if (ep.isNull()) {
                    ERRLOG("parameter '%s' not recognized",arg);
                    return NULL;
                }
                serverep = ep;
            }
            tail=strchr(arg,'=');
            if (!tail)
                continue;
        }
        StringBuffer head(tail-arg,arg);
        tail++;
        if (stricmp(head.str(),"action")==0) {
            if (stricmp(tail,"list")==0)
                cmd->setAction(SCA_LIST);
            else if (stricmp(tail,"listdt")==0)
                cmd->setAction(SCA_LISTDT);
            else if (stricmp(tail,"get")==0) {
                cmd->setAction(SCA_GET);
                needloadwu = true;
            }
            else if (stricmp(tail,"restore")==0)
                cmd->setAction(SCA_RESTORE);
            else if (stricmp(tail,"backup")==0)
                cmd->setAction(SCA_BACKUP);
            else if (stricmp(tail,"xref")==0)
                cmd->setAction(SCA_XREF);
            else if (stricmp(tail,"archive")==0)
                cmd->setAction(SCA_ARCHIVE);
            else if (stricmp(tail,"stop")==0) {
                cmd->setAction(SCA_STOP);
                return cmd.getClear();
            }
            else if (stricmp(tail,"version")==0) {
                cmd->setAction(SCA_GETVERSION);
                return cmd.getClear();
            }
            else if (stricmp(tail,"coalesce_suspend")==0) {
                cmd->setAction(SCA_COALESCE_SUSPEND);
                return cmd.getClear();
            }
            else if (stricmp(tail,"coalesce_resume")==0) {
                cmd->setAction(SCA_COALESCE_RESUME);
                return cmd.getClear();
            }
            else {
                ERRLOG("action '%s' not recognized",tail);
                return NULL;
            }
        }
        else if (stricmp(head.str(),"wuid")==0) {
            if (!*tail)
                tail = "*";
            if ((strchr(tail,'*')!=NULL)||(strchr(tail,'?')!=NULL)) 
                iswild = true;
            cmd->addId(tail);
        }
        else if (stricmp(head.str(),"server")==0) {
            SocketEndpoint ep(tail,DEFAULT_SASHA_PORT);             
            if (ep.isNull()) {
                ERRLOG("server '%s' not resolved",tail);
                return NULL;
            }
            serverep = ep;
        }
        else if (stricmp(head.str(),"outfile")==0) {
            Owned<IFile> f = createIFile(tail);
            if (f) {
                Owned<IFileIO> fio = f->open(IFOcreate);
                if (fio)
                    outfile.setown(createIOStream(fio));
            }
        }
        else if (stricmp(head.str(),"xslfile")==0) {
            Owned<IFile> f = createIFile(tail);
            if (f) {
                Owned<IFileIO> fio = f->open(IFOread);
                if (fio) {
                    size32_t xsllen = (size32_t)fio->size();
                    char *buf = new char[xsllen+1];
                    xsllen = fio->read(0,xsllen,buf);
                    buf[xsllen] = 0;
                    cmd->setXslt(buf);
                    delete [] buf;
                }
            }
        }
        else if (stricmp(head.str(),"after")==0)
            cmd->setAfter(tail);
        else if (stricmp(head.str(),"before")==0)
            cmd->setBefore(tail);
        else if (stricmp(head.str(),"start")==0)
            cmd->setStart(atoi(tail));
        else if (stricmp(head.str(),"limit")==0)
            cmd->setLimit(atoi(tail));
        else if (stricmp(head.str(),"dfu")==0)
            cmd->setDFU(strToBool(tail));
        else if (stricmp(head.str(),"online")==0)
            cmd->setOnline(strToBool(tail));
        else if (stricmp(head.str(),"archived")==0)
            cmd->setArchived(strToBool(tail));
        else {
            needloadwu = true;
            if (stricmp(head.str(),"owner")==0) 
                cmd->setOwner(tail);
            else if (stricmp(head.str(),"state")==0)
                cmd->setState(tail);
            else if (stricmp(head.str(),"cluster")==0)
                cmd->setCluster(tail);
            else if (stricmp(head.str(),"jobname")==0)
                cmd->setJobName(tail);
            else if (stricmp(head.str(),"output")==0)
                cmd->setOutputFormat(tail);
            else {
                ERRLOG("attribute '%s' not recognized",head.str());
                return NULL;
            }
        }
    }
    if (cmd->numIds()==0) {
        iswild = true;
        cmd->addId("*");
    }
    if (iswild&&needloadwu&&!confirm("This command may take some time - ok to continue? (Y/N)"))
        return NULL;
    if (serverep.isNull()) {
        ERRLOG("no server specified");
        return NULL;
    }
    if (cmd->getAction()==SCA_null) {
        ERRLOG("no action specified");
        return NULL;
    }
    return cmd.getClear();
}

bool getVersion(INode *node)
{
    Owned<ISashaCommand> cmd = createSashaCommand();
    cmd->setAction(SCA_GETVERSION);
    StringBuffer ips;
    node->endpoint().getIpText(ips);
    if (!cmd->send(node,1*60*1000)) {
        ERRLOG("Could not connect to Sasha server on %s",ips.str());
        return false;
    }
    StringBuffer id;
    if (cmd->getId(0,id)) {
        PROGLOG("Sasha server[%s]: Version %s",ips.str(),id.str());
        return true;
    }
    ERRLOG("Sasha server[%s]: Protocol error",ips.str());
    return false;
}

struct ReleaseAtomBlock { ~ReleaseAtomBlock() { releaseAtoms(); } };
int main(int argc, char* argv[])
{   
    ReleaseAtomBlock rABlock;
    InitModuleObjects();
    if (argc<3) {
        usage();
        return 0;
    }

    EnableSEHtoExceptionMapping();
    Thread::setDefaultStackSize(0x10000);
    startMPServer(0);
    attachStandardFileLogMsgMonitor("sasha.log", NULL, MSGFIELD_STANDARD, MSGAUD_all, MSGCLS_all, TopDetail, false, true);
    queryStderrLogMsgHandler()->setMessageFields(MSGFIELD_prefix);
    try {
        SocketEndpoint ep;
        Owned<ISashaCommand> cmd = createCommand(argc,argv,ep);
        if (cmd.get()) {
            SashaCommandAction action = cmd->getAction();
            Owned<INode> node = createINode(ep);
            if (getVersion(node)&&(action!=SCA_GETVERSION)) {
                if (action==SCA_RESTORE) {
                    cmd->setAction(SCA_LIST);
                    cmd->setArchived(true);
                    cmd->setOnline(false);
                }
                else if (action==SCA_ARCHIVE) {
                    cmd->setAction(SCA_LIST);
                    cmd->setArchived(false);
                    cmd->setOnline(true);
                }
                else if (action==SCA_BACKUP) {
                    cmd->setAction(SCA_LIST);
                    cmd->setArchived(false);
                    cmd->setOnline(true);
                    cmd->setDFU(false);     // can only backup WUs currently
                }
                if (cmd->send(node)) {
                    if ((cmd->getAction()==SCA_LIST)||(cmd->getAction()==SCA_GET)||(action==SCA_LISTDT)) {
                        cmd->setAction(action); // restore orig action
                        cmd->setArchived(false);
                        cmd->setOnline(false);
                        unsigned n = cmd->numIds();
                        StringBuffer s;
                        if ((action==SCA_LIST)||(action==SCA_GET)||(action==SCA_LISTDT)) {
                            for (unsigned i=0;i<n;i++) {
                                cmd->getId(i,s.clear());
                                if (action==SCA_LISTDT) {
                                    StringBuffer dts;
                                    CDateTime dt;
                                    cmd->getDT(dt,i);
                                    dt.getString(dts);
                                    PROGLOG("%s,%s",s.str(),dts.str());
                                    if (outfile) {
                                        outfile->write(s.length(),s.str());
                                        outfile->write(1,",");
                                        outfile->write(dts.length(),dts.str());
                                        outfile->write(1,"\n");
                                    }
                                }
                                else {
                                    PROGLOG("%s",s.str());
                                    if (outfile) {
                                        outfile->write(s.length(),s.str());
                                        outfile->write(1,"\n");
                                    }
                                }
                                if (action==SCA_GET) {
                                    StringBuffer res;
                                    if (cmd->getResult(i,res)) {
                                        if (extractTimings) {
                                            Owned<IPropertyTree> pt = createPTreeFromXMLString(res.str());
                                            if (pt) 
                                                DumpWorkunitTimings(pt);
                                        }
                                        else {
                                            PROGLOG("----------------");
                                            PROGLOG("%s",res.str());
                                            PROGLOG("================");
                                            if (outfile) {
                                                outfile->write(17,"----------------\n");
                                                outfile->write(res.length(),res.str());
                                                outfile->write(1,"\n");
                                                outfile->write(17,"================\n");
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        if ((action==SCA_LIST)||(action==SCA_GET)||(n==0)||(action==SCA_LISTDT))
                            PROGLOG("%d WUID%s returned",n,(n==1)?"":"s");
                        else {
                            bool ok = true;
                            StringBuffer msg;
                            if (n>1) {
                                msg.append(n).append(" workunits will be ");
                                if (action==SCA_RESTORE)
                                    msg.append("restored");
                                else
                                    msg.append("archived");
                                msg.append(", Continue (Y/N)");
                                ok = confirm(msg.str());
                            }
                            if (ok) {
                                if (cmd->send(node)) {
                                    n = cmd->numIds();;
                                    for (unsigned i=0;i<n;i++) {
                                        if ((action==SCA_ARCHIVE)||(action==SCA_RESTORE)||(action==SCA_BACKUP)) {
                                            cmd->getId(i,s.clear());
                                            PROGLOG("%s",s.str());
                                            if (outfile) {
                                                outfile->write(s.length(),s.str());
                                                outfile->write(1,"\n");
                                            }
                                        }
                                    }
                                    if (action==SCA_RESTORE)
                                        PROGLOG("Restore complete");
                                    else
                                        PROGLOG("Archive complete");
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    catch (IException *e) {
        EXCLOG(e, "SASHA");
        e->Release();
    }
    outfile.clear();
    stopMPServer();

    return 0;
}

