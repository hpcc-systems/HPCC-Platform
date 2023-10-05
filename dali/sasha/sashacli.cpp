#include "platform.h"
#include <stdio.h>
#include "jlib.hpp"
#include "jio.hpp"
#include "jmisc.hpp"
#include "jfile.hpp"
#include "mpbase.hpp"
#ifdef _WIN32
#include <conio.h>
#else
#define _getch getchar
#endif

#include "sacmd.hpp"
#include "sashacli.hpp"

//static bool extractTimings=true; //This line should be removed because there is no place where the extractTimings is set to false.

static bool getResponse()
{
    int ch;
    do
    {
        ch = toupper(ch = _getch());
    } while (ch != 'Y' && ch != 'N');
    printf("%c\n",ch);
    return ch=='Y' ? true : false;
}

static bool confirm(const char * msg)
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

static void DumpWorkunitTimings(IPropertyTree *wu)
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

static void getVersion(INode *node, StringBuffer &outBuffer, bool viaESP)
{
    Owned<ISashaCommand> cmd = createSashaCommand();
    cmd->setAction(SCA_GETVERSION);
    StringBuffer host;
    node->endpoint().getHostText(host);
    if (!cmd->send(node,1*60*1000)) {
        if (viaESP)
            makeStringExceptionV(-1,"Could not connect to Sasha server on %s",host.str());
        OERRLOG("Could not connect to Sasha server on %s",host.str());
        return;
    }
    StringBuffer id;
    if (cmd->getId(0,id)) {
        if (viaESP)
            outBuffer.appendf("Sasha server[%s]: Version %s\n",host.str(),id.str());
        else
            PROGLOG("Sasha server[%s]: Version %s",host.str(),id.str());
        return;
    }
    if (viaESP)
        makeStringExceptionV(-1,"Sasha server[%s]: Protocol error",host.str());
    IERRLOG("Sasha server[%s]: Protocol error",host.str());
    return;
}

extern SASHACLI_API void runSashaCommand(SocketEndpoint ep, ISashaCommand *cmd, IFileIOStream* outfile, StringBuffer &outBuffer, bool viaESP)
{
    Owned<INode> node = createINode(ep);
    getVersion(node,outBuffer,viaESP);

    SashaCommandAction action = cmd->getAction();
    if (action==SCA_GETVERSION)
        return;

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

    if (!cmd->send(node))
    {
        StringBuffer host;
        if (viaESP)
            makeStringExceptionV(-1,"Could not connect to Sasha server at %s",ep.getHostText(host).str());
        return;
    }

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
                    if (viaESP)
                        outBuffer.append(s).append(",").append(dts).append("\n");
                    else
                        PROGLOG("%s,%s",s.str(),dts.str());
                    if (outfile) {
                        outfile->write(s.length(),s.str());
                        outfile->write(1,",");
                        outfile->write(dts.length(),dts.str());
                        outfile->write(1,"\n");
                    }
                }
                else {
                    if (viaESP)
                        outBuffer.append(s).append("\n");
                    else
                        PROGLOG("%s",s.str());
                    if (outfile) {
                        outfile->write(s.length(),s.str());
                        outfile->write(1,"\n");
                    }
                }
                if (action==SCA_GET) {
                    StringBuffer res;
                    if (cmd->getResult(i,res)) {
                        Owned<IPropertyTree> pt = createPTreeFromXMLString(res.str());
                        if (pt)
                            DumpWorkunitTimings(pt);
                        /* //this code block is replaced by the last 3 lines because the extractTimings is always true (see another note).
                        if (extractTimings) {
                            Owned<IPropertyTree> pt = createPTreeFromXMLString(res.str());
                            if (pt)
                                DumpWorkunitTimings(pt);  //Is the Timings/Timing still available (not in my WUs)?
                        }
                        else {
                            PROGLOG("----------------");
                            PROGLOG("%s",res.str());
                            PROGLOG("================");
                            if (viaESP) {
                                outBuffer.append("----------------\n").append(res);
                                outBuffer.append("\n================\n");
                            }
                            if (outfile) {
                                outfile->write(17,"----------------\n");
                                outfile->write(res.length(),res.str());
                                outfile->write(1,"\n");
                                outfile->write(17,"================\n");
                            }
                        }*/
                    }
                }
            }
        }
        if ((action==SCA_LIST)||(action==SCA_GET)||(n==0)||(action==SCA_LISTDT))
        {
            if (viaESP)
                outBuffer.appendf("%d WUID%s returned\n",n,(n==1)?"":"s");
            else
                PROGLOG("%d WUID%s returned",n,(n==1)?"":"s");
            return;
        }

        if (!viaESP&&(n>1)) {
            StringBuffer msg;
            msg.append(n).append(" workunits will be ");
            if (action==SCA_RESTORE)
                msg.append("restored");
            else
                msg.append("archived");
            msg.append(", Continue (Y/N)");
            if (!confirm(msg.str()))
                return;
        }

        if (!cmd->send(node))
        {
            StringBuffer host;
            if (viaESP)
                makeStringExceptionV(-1,"Could not connect to Sasha server at %s",ep.getHostText(host).str());
            return;
        }

        n = cmd->numIds();;
        for (unsigned i=0;i<n;i++) {
            if ((action==SCA_ARCHIVE)||(action==SCA_RESTORE)||(action==SCA_BACKUP)) {
                cmd->getId(i,s.clear());
                if (viaESP)
                    outBuffer.append(s).append("\n");
                else
                    PROGLOG("%s",s.str());
                if (outfile) {
                    outfile->write(s.length(),s.str());
                    outfile->write(1,"\n");
                }
            }
        }
        if (action==SCA_RESTORE)
        {
            if (viaESP)
                outBuffer.append("Restore complete\n");
            else
                PROGLOG("Restore complete");
        }
        else
        {
            if (viaESP)
                outBuffer.append("Archive complete\n");
            else
                PROGLOG("Archive complete");
        }
    }
}
