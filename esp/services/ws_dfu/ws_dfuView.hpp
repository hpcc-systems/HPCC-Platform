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

#ifndef _ESP_WsDfuView_HPP__
#define _ESP_WsDfuView_HPP__


#ifdef _WIN32
    #define FILEVIEW_API __declspec(dllimport)
#else
    #define FILEVIEW_API
#endif


#include "fileview.hpp"
#include "jstring.hpp"

const int DefaultViewLeaseTimeout = (10 /* minutes */ * 60 /* seconds/minute */);


class CStringVal : public IStringVal
{
public:
    CStringVal()
    {
        s.clear();
    }
    virtual const char * str() const
    {
        return s.str();
    }
    virtual void set(const char * val)
    {
        s.clear();
        s.append(val);
        s.trim();
        s.clip();
    }
    virtual void clear()
    {
        s.clear();
    }
    virtual void setLen(const char * val, unsigned length)
    {
        s.clear();
        if (val != NULL)
        {
            s.append(val, 0, length - 1);
            s.trim();
            s.clip();
        }
    }
    virtual unsigned length() const
    {
        return s.length();
    }

private:
    StringBuffer s;
};

class LVToken
{
public:

    LVToken(unsigned id, unsigned rowsPerPage, bool randSupport=false, const char *lfname=NULL, const char *queue=NULL, const char *cluster=NULL)
        : id_(id), rowsPerPage_(rowsPerPage), randSupport_(randSupport), lfname_(lfname), queue_(queue), cluster_(cluster)
    {
    }
    
    LVToken(const char *token)
    {
        StringBuffer temp;
        JBASE64_Decode(token, temp);
        MemoryBuffer mb(temp.length(), temp.toCharArray());

        Owned<IProperties> props(createProperties());
        props->deserialize(mb);

        id_=props->getPropInt("id");
        rowsPerPage_=props->getPropInt("rpp");
        randSupport_=(props->getPropInt("rnd")!=0);

        props->getProp("ln", lfname_);
        props->getProp("q", queue_);
        props->getProp("cl", cluster_);
    }

    StringBuffer &toStringBuffer(StringBuffer &token)
    {
        token.clear();

        Owned<IProperties> props(createProperties());
        if (props)
        {
            props->setProp("id", id_);
            props->setProp("rpp", rowsPerPage_);
            props->setProp("rnd", randSupport_);

            if (lfname_.length())
                props->setProp("ln", lfname_.str());
            if (queue_.length())
                props->setProp("q", queue_.str());
            if (cluster_.length())
                props->setProp("cl", cluster_.str());

            MemoryBuffer mb;
            props->serialize(mb);

            JBASE64_Encode(mb.toByteArray(), mb.length(), token);
        }

        return token;
    }

    unsigned getId(){return id_;}
    unsigned getRowsPerPage(){return rowsPerPage_;}
    bool  supportsRandomAccess(){return randSupport_;}

    const char *getLogicalName(){return lfname_.str();}
    const char *getQueue(){return queue_.str();}
    const char *getCluster(){return cluster_.str();}

private:
        unsigned id_;
        unsigned rowsPerPage_;
        bool randSupport_;

        StringBuffer lfname_;
        StringBuffer queue_;
        StringBuffer cluster_;
};


class CLogicalView
{
public:
    CLogicalView(IResultSetCursor *trsc, const char * label, unsigned rowsPerPage);
    ~CLogicalView();

    unsigned getId(){return id_;}
    void updateLastAccessTime(){lastaccess_=time(NULL);}
    bool hasExpired(time_t timeout){return (time(NULL)-lastaccess_ > timeout);}

    unsigned getRowsPerPage(){return rowsPerPage_;}
    const char * getXsd();
    void getPage(StringBuffer &page, StringBuffer &state, bool backFlag=false);

//  IResultSetCursor *getResultSet(){return rs_;}
private:
    CLogicalView();

    static unsigned idgen_;

    unsigned id_;
    time_t lastaccess_;
    unsigned rowsPerPage_;
    IResultSetCursor *rs_;
    const IResultSetMetaData *rsmd_;
    StringBuffer label_;
    StringBuffer xsd_;
};

MAKEPointerArray(CLogicalView, ArrayOfViews);


class LVManager : public Thread
{
public:
    LVManager()
    {
//      rsFactory_.setown(getResultSetFactory());
    }

    void setLeaseTimeout(int timeout)
    {
        leaseTimeout_=timeout;
    }

    ArrayOfViews & views(){return views_;}

    CLogicalView *createLogicalView(StringBuffer &token, const char * logicalFile, const char * cluster=NULL, unsigned rowsPerPage=50);
    bool deleteLogicalView(const StringBuffer &token);
    CLogicalView *queryLogicalView(const StringBuffer &token);

private:
    int run()
    {
        while (!isAlive())
        {
            sleep(10);
            
            ForEachItemIn(idx, views_)
            {
                CLogicalView &view = views_.item(idx);
                
                if (view.hasExpired(leaseTimeout_))
                {
                    views_.remove(idx);
                    delete &view;
                }
            }
        }
        return 0;
    }

    //view token
    //base64(id # random # logicalname # queue # cluster # pagerows)

    //view-state
    //base64(pagerows # currentpage)

    const StringBuffer &createLVToken(StringBuffer &strToken, unsigned id, unsigned rowsPerPage, bool randSupport=false, const char *lfname=NULL, const char *queue=NULL, const char *cluster=NULL)
    {
        return strToken.append(LVToken(id, rowsPerPage, randSupport, lfname, queue, cluster).toStringBuffer(strToken));
    }

    unsigned idFromToken(const StringBuffer &token)
    {
        StringBuffer out;
        
        JBASE64_Decode(token.str(), out);

        Owned<IProperties> props(createProperties(out.str()));

        return props->getPropInt("id");
    }




private:
    ArrayOfViews views_;
    int leaseTimeout_;

    Owned<IResultSetFactory> rsFactory_;
};


#endif //_ESP_WsDfuView_HPP__
