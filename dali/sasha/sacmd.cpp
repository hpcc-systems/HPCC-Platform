#include "platform.h"
#include "jlib.hpp"
#include "jmisc.hpp"
#include "jptree.hpp"
#include "jsuperhash.hpp"
#include "jtime.hpp"
#include "mpbase.hpp"
#include "mpcomm.hpp"
#define NO_XLS
#ifndef NO_XLS
#include "xslprocessor.hpp"
#endif

#include "sacmd.hpp"

#define MAX_RESULT_SIZE (0x100000*10)

class CSashaCommand: public CInterface, implements ISashaCommand
{
    SashaCommandAction action;
    StringAttrArray ids;
    CDateTime *dts;
    unsigned numdts;

    StringAttr after;
    StringAttr before;
    StringAttr state;
    StringAttr owner;
    StringAttr cluster;
    StringAttr jobname;
    StringAttr outputformat;
    StringAttr priority;
    StringAttr fileread;
    StringAttr filewritten;
    StringAttr roxiecluster;
    StringAttr eclcontains;
    StringAttr dfucmdname;
#ifndef NO_XLS
    Owned<IXslProcessor> xslproc;
    Owned<IXslTransform> xsltrans;
#endif
    StringAttr xslt;
    StringAttrArray results;
    unsigned resultsize;
    bool resultoverflow;
    bool online;
    bool archived;
    bool dfu;
    bool wuservices;
    int numberOfIdsMatching; //default to -1 (for input: if >0, ask for number OfIdsMatching; for output:if >=0, this is the number OfIdsMatching)
    unsigned start;
    unsigned limit;
    CMessageBuffer msgbuf;  // used for reply
    MemoryBuffer wusbuf;

    const char *retstr(StringAttr &ret)
    {
        const char *s = ret.get();
        return s?s:"";
    }


public:
    IMPLEMENT_IINTERFACE;

    CSashaCommand()
    {
        action = SCA_null;
        online = false;
        archived = false;
        dfu = false;
        start = 0;
        limit = 0x7fffffff;
        resultoverflow = false;
        resultsize = 0;
        wuservices = false;
        dts = NULL;
        numdts = 0;
        numberOfIdsMatching = -1;
    }

    CSashaCommand(MemoryBuffer &mb)
    {
        deserialize(mb);
        free(dts);
    }

    void deserialize(MemoryBuffer &mb)
    {
        // clear optional
#ifndef NO_XLS
        xslproc.clear();
        xsltrans.clear();
        xslt.clear();
#endif
        resultoverflow = false;
        wuservices = false;
        unsigned version;
        mb.read(version);
        unsigned v;
        mb.read(v);
        action = (SashaCommandAction)v;
        if (action == SCA_LIST_WITH_MATCHING_COUNT)
            mb.read(numberOfIdsMatching);
        unsigned n;
        mb.read(n);
        while (n--) {
            StringAttrItem *s = new StringAttrItem;
            mb.read(s->text);
            ids.append(*s);
        }
        mb.read(after);
        mb.read(before);
        mb.read(state);
        mb.read(owner);
        mb.read(cluster);
        mb.read(jobname);
        mb.read(outputformat);
        mb.read(online);
        mb.read(archived);
        mb.read(dfu);
        mb.read(start);
        mb.read(limit);
        if (mb.remaining()>sizeof(unsigned)) { // new format (trailing WUs)
            mb.read(resultoverflow);
            mb.read(n);
            while (n--) {
                StringAttrItem *s = new StringAttrItem;
                mb.read(s->text);
                results.append(*s);
            }
            bool isxslt;
            mb.read(isxslt);
            if (isxslt)
                mb.read(xslt);
            if (mb.remaining()>sizeof(bool)) { // new WorkUnitServices format (trailing WUs)
                mb.read(wuservices);
                if (wuservices) {
                    mb.read(priority);
                    mb.read(fileread);
                    mb.read(filewritten);
                    mb.read(roxiecluster);
                    mb.read(eclcontains);
                }
                if (mb.remaining()>=sizeof(numdts)) { // new listdts info
                    mb.read(numdts);
                    free(dts);
                    dts = NULL;
                    if (numdts) {
                        dts = (CDateTime *)calloc(numdts,sizeof(CDateTime));
                        for (unsigned i=0;i<numdts;i++)
                            dts[i].deserialize(mb);
                    }
                }
            }
        }
    }

    void serialize(MemoryBuffer &mb)
    {
        unsigned version=1;
        mb.append(version);
        mb.append((unsigned)action);
        if (action == SCA_LIST_WITH_MATCHING_COUNT)
            mb.append(numberOfIdsMatching);
        unsigned n = ids.ordinality();
        mb.append(n);
        unsigned i;
        for (i=0;i<n;i++) 
            mb.append(ids.item(i).text);
        mb.append(after);
        mb.append(before);
        mb.append(state);
        mb.append(owner);
        mb.append(cluster);
        mb.append(jobname);
        mb.append(outputformat);
        mb.append(online);
        mb.append(archived);
        mb.append(dfu);
        mb.append(start);
        mb.append(limit);
        mb.append(resultoverflow);
        n = results.ordinality();
        mb.append(n);
        for (i=0;i<n;i++) 
            mb.append(results.item(i).text);
        bool isxslt = xslt&&xslt.length();
        mb.append(isxslt);
        if (isxslt) 
            mb.append(xslt);
        mb.append(wuservices);
        if (wuservices) {
            mb.append(priority);
            mb.append(fileread);
            mb.append(filewritten);
            mb.append(roxiecluster);
            mb.append(eclcontains);
        }
        mb.append(numdts);
        for (i=0;i<numdts;i++) 
            dts[i].serialize(mb);

    }

    SashaCommandAction getAction()
    {
        return action;
    }

    void setAction(SashaCommandAction val)
    {
        action = val;
    }

    void addId(const char *id)
    {
        ids.append(*new StringAttrItem(id));
    }

    void clearIds()
    {
        ids.kill();
    }

    unsigned numIds() 
    { 
        return ids.ordinality(); 
    }

    
    bool getId(unsigned i, StringBuffer &id)
    {
        if (i>=numIds())
            return false;
        id.append(ids.item(i).text);
        return true;
    }

    void addDT(CDateTime &dt)
    {
        if ((numdts+16)/16!=(numdts+15)/16)
            if (dts)
                dts = (CDateTime *)realloc(dts,(numdts+16)*sizeof(CDateTime));
            else
                dts = (CDateTime *)malloc((numdts+16)*sizeof(CDateTime));
        memset(dts+numdts,0,sizeof(CDateTime));
        dts[numdts++].set(dt);
    }

    void getDT(CDateTime &dt,unsigned i)
    {
        if (i<numdts)
            dt.set(dts[i]);
        else
            dt.clear();
    }


    const char *queryId(unsigned i)
    {
        if (i>=numIds())
            return "";
        return retstr(ids.item(i).text);
    }

    const char *queryAfter()
    {
        return retstr(after);
    }

    void setAfter(const char *val)
    {
        after.set(val);
    }

    const char *queryBefore()
    {
        return retstr(before);
    }

    void setBefore(const char *val)
    {
        before.set(val);
    }

    const char *queryState()
    {
        return retstr(state);
    }

    void setState(const char *val)
    {
        state.set(val);
    }

    const char *queryOwner()
    {
        return retstr(owner);
    }

    void setJobName(const char *val)
    {
        jobname.set(val);
    }

    const char *queryJobName()
    {
        return retstr(jobname);
    }

    void setOwner(const char *val)
    {
        owner.set(val);
    }

    const char *queryCluster()
    {
        return retstr(cluster);
    }

    void setCluster(const char *val)
    {
        cluster.set(val);
    }
    
    bool getWUSmode()
    {
        return wuservices;
    }

    void setWUSmode(bool val) 
    {
        wuservices = val;
    }

    void setPriority(const char *val)
    {
        priority.set(val);
    }

    const char *queryPriority()
    {
        return retstr(priority);
    }

    void setFileRead(const char *val)
    {
        fileread.set(val);
    }

    const char *queryFileRead()
    {
        return retstr(fileread);
    }

    void setFileWritten(const char *val)
    {
        filewritten.set(val);
    }

    const char *queryFileWritten()
    {
        return retstr(filewritten);
    }

    void setRoxieCluster(const char *val)
    {
        roxiecluster.set(val);
    }

    const char *queryRoxieCluster()
    {
        return retstr(roxiecluster);
    }

    void setEclContains(const char *val)
    {
        eclcontains.set(val);
    }

    const char *queryEclContains()
    {
        return retstr(eclcontains);
    }

    const char *queryOutputFormat()
    {
        return retstr(outputformat);
    }

    void setOutputFormat(const char *val)
    {
        outputformat.set(val);
    }

    const char *queryDfuCmdName()
    {
        return retstr(dfucmdname);
    }

    void setDfuCmdName(const char *val)
    {
        dfucmdname.set(val);
    }

    bool getOnline()
    {
        return online;
    }
    void setOnline(bool val)
    {
        online = val;
    }

    bool getArchived()
    {
        return !online||archived;
    }
    void setArchived(bool val)
    {
        archived = val;
    }

    virtual unsigned getStart()
    {
        return start;
    }

    void setStart(unsigned val)
    {
        start = val;
    }

    virtual unsigned getLimit()
    {
        return limit;
    }
    virtual void setLimit(unsigned val)
    {
        limit = val;
    }

    bool addResult(const char *res)
    {
        if (resultoverflow)
            return false;
        StringBuffer t;
        size32_t reslen = strlen(res);
        if (xslt&&xslt.length()) {
#ifndef NO_XLS
            if (!xslproc)
                xslproc.setown(getXslProcessor());
            if (!xsltrans&&xslproc) {
                xsltrans.setown(xslproc->createXslTransform());
                if (!xsltrans)
                    return false;
                xsltrans->setXslSource(xslt.get(), xslt.length());
            }
            xsltrans->setXmlSource(res, reslen);
            try {
                xsltrans->transform(t);
                res = t.str();
                reslen = t.length();
            }
            catch (IException *e) {
                EXCLOG(e,"CSashaCommand::addResult");
                e->Release();
                return false;
            }
#endif
        }
        if (resultsize&&(resultsize+reslen>MAX_RESULT_SIZE)) 
            resultoverflow = true;
        else {
            results.append(*new StringAttrItem(res,reslen));
            resultsize += reslen;
        }
        return !resultoverflow;
    }

    void clearResults()
    {
        results.kill();
        resultoverflow = false;
        resultsize = 0;
        wusbuf.clear();
    }

    unsigned numResults() 
    { 
        return results.ordinality(); 
    }
    
    bool getResult(unsigned i, StringBuffer &res)
    {
        if (i>=numResults())
            return false;
        res.append(results.item(i).text);
        return true;
    }

    bool resultsOverflowed()
    {
        return resultoverflow;
    }

    void setXslt(const char *_xslt)
    {
#ifdef NO_XLS
        UNIMPLEMENTED;
#else
        xslproc.clear();
        xsltrans.clear();
        xslt.set(_xslt);
#endif
    }
    
    bool getXslt(StringBuffer &_xslt)
    {
        if (!xslt)
            return false;
        _xslt.append(xslt);
        return true;
    }

    bool getDFU()
    {
        return dfu;
    }

    void setDFU(bool val)
    {
        dfu = val;
    }

    bool send(INode *node,unsigned timeout)
    {
        unsigned retries = 3;
        loop {
            try {
                CMessageBuffer mb;
                serialize(mb);
                if (queryWorldCommunicator().sendRecv(mb,node,MPTAG_SASHA_REQUEST,timeout?timeout:12*60*60*1000)) { // could take a long time!
                    clearIds();
                    clearResults();
                    if (action==SCA_WORKUNIT_SERVICES_GET) {
                        mb.swapWith(wusbuf);
                    }
                    else {
                        if (action==SCA_LIST_WITH_MATCHING_COUNT) {
                            if (mb.length()-mb.getPos()>=sizeof(int)) {
                                int numberOfIdsMatchingReq = -1;
                                mb.read(numberOfIdsMatchingReq);
                                setNumberOfIdsMatching(numberOfIdsMatchingReq);
                            }
                        }
                        unsigned n=0;
                        unsigned i;
                        if (mb.length()-mb.getPos()>=sizeof(unsigned)) {
                            mb.read(n);
                            for (i=0;i<n;i++) {
                                StringAttr s;
                                mb.read(s);
                                addId(s.get());
                            }
                            if (mb.length()-mb.getPos()>=sizeof(unsigned)+sizeof(bool)) {
                                mb.read(resultoverflow);
                                mb.read(n);
                                for (i=0;i<n;i++) {
                                    StringAttr res;
                                    mb.read(res);
                                    size32_t reslen = res.length();
                                    results.append(*new StringAttrItem(res,reslen));
                                    resultsize += reslen;
                                }
                                if (mb.length()-mb.getPos()>=sizeof(unsigned)) {
                                    mb.read(numdts);
                                    free(dts);
                                    dts = NULL;
                                    if (numdts) {
                                        dts = (CDateTime *)calloc(numdts,sizeof(CDateTime));
                                        for (i=0;i<numdts;i++)
                                            dts[i].deserialize(mb);
                                    }
                                }

                            }
                        }
                    }
                    return true;
                }   
                else
                    break;
            }
            catch (IException *e) {
                if ((--retries==0)||(action==SCA_STOP))
                    throw;
                EXCLOG(e,"CSashaCommand send");
                ::Release(e);
            }
            try { // shouldn't really be necessary but make sure socket really closed
                queryWorldCommunicator().disconnect(node);
            }
            catch (IException *e) {
                EXCLOG(e,"CSashaCommand disconnect");
                ::Release(e);
            }
        }; 
        return false;
    }

    bool accept(unsigned timeout)
    {
        msgbuf.clear();
        if (queryWorldCommunicator().recv(msgbuf,NULL,MPTAG_SASHA_REQUEST,NULL,timeout?timeout:(5*60*1000))&&msgbuf.length()) {
            deserialize(msgbuf);
            msgbuf.clear();     
            return true;
        }
        return false;
    }

    void cancelaccept()
    {
        queryWorldCommunicator().cancel(NULL,MPTAG_SASHA_REQUEST);
    }


    bool reply(bool clearBuf)
    {
        if (clearBuf)
            msgbuf.clear();
        unsigned n = ids.ordinality();
        msgbuf.append(n);
        unsigned i;
        for (i=0;i<n;i++)
            msgbuf.append(ids.item(i).text);
        msgbuf.append(resultoverflow);
        n = results.ordinality();
        msgbuf.append(n);
        for (i=0;i<n;i++)
            msgbuf.append(results.item(i).text);
        msgbuf.append(numdts);
        for (i=0;i<numdts;i++)
            dts[i].serialize(msgbuf);
        bool ret = queryWorldCommunicator().reply(msgbuf,1000*5*60);
        msgbuf.clear();
        return ret;
    }

    bool IDSWithMatchingNumberReply()
    {
        msgbuf.clear();
        msgbuf.append(numberOfIdsMatching);
        return reply(false);
    }

    bool WUSreply()
    {
        msgbuf.swapWith(wusbuf);
        bool ret = queryWorldCommunicator().reply(msgbuf,1000*5*60);
        msgbuf.clear();
        return ret;
    }


    byte getWUSresult(MemoryBuffer &mb)
    {
        byte ret = 0;
        wusbuf.swapWith(mb);    
        if (mb.length()==1) {
            mb.read(ret);
            mb.clear();
        }
        return ret;
    }

    virtual void setWUSresult(MemoryBuffer &mb)
    {
        mb.swapWith(wusbuf);
    }

    int getNumberOfIdsMatching()
    {
        return numberOfIdsMatching;
    }

    void setNumberOfIdsMatching(int val)
    {
        numberOfIdsMatching = val;
    }

};




ISashaCommand *createSashaCommand()
{
    return new CSashaCommand;
}


#if 0

void sashaExample(INode *sashaserver) 
{
    Owned<ISashaCommand> cmd = createSashaCommand();        // create command
    cmd->setAction(SCA_GET);                                // get WU info
    cmd->setOnline(true);                                       // include online WUs
    cmd->setArchived(true);                                     // include archived WUs
    cmd->setAfter("200401010000");      
    cmd->setBefore("200402010000");                         // jan 2004
    cmd->setOwner("nigel");         
    cmd->setState("failed");
    cmd->setXslt(XSLTTEXT);
    if (cmd->send(sashaserver)) {
        if (cmd->resultsOverflowed())
            printf("Results overflowed");
        else {
            unsigned n = cmd->numResults();
            for (unsigned i=0;i<n;i++) {
                StringBuffer res;
                cmd->getResult(i,res);
                printf("%d: %s\n",i,res.str());                 // print transformed results
            }
        }
    }
}


#endif
