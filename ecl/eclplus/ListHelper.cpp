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
#include "jlib.hpp"
#include "ListHelper.ipp"

#define NUMCOLUMNS 4

static StringAttr columns[NUMCOLUMNS] = {"WUID", "OWNER", "JOBNAME", "STATUS"};

ListHelper::ListHelper(IProperties * _globals, IFormatType * _format) : globals(_globals), format(_format), wuclient(createWorkunitsClient(_globals))
{
}

ListHelper::~ListHelper()
{
}

static int compareWUs(IInterface * const *ll, IInterface * const *rr)
{
    IConstECLWorkunit *l = (IConstECLWorkunit *) *ll;
    IConstECLWorkunit *r = (IConstECLWorkunit *) *rr;
    const char* ln = l->getWuid();
    const char* rn = r->getWuid();
    return strcmp(ln, rn);
}

bool ListHelper::doit(FILE * fp)
{
    StringBuffer s;

    // Add the header
    if(format && format->displayNamesHeader())
    {
        for(unsigned i = 0; i < NUMCOLUMNS; i++)
        {
            if(i)
                s.append(format->getValueSeparator());

            s.append(format->getDataDelimiter())
             .append(columns[i].get())
             .append(format->getDataDelimiter());
        }
        s.append(format->getRecordSeparator());
    }

    // If a WUID was specified go for it, otherwise go for the owner
    if(globals->hasProp("WUID"))
    {
        Owned<IClientWUInfoRequest> req = wuclient->createWUInfoRequest();
        req->setWuid(globals->queryProp("WUID"));
        Owned<IClientWUInfoResponse> resp = wuclient->WUInfo(req);
        if(!resp)
            return false;
        const IMultiException* excep = &resp->getExceptions();
        if(excep != NULL && excep->ordinality() > 0)
        {
            StringBuffer msg;
            excep->errorMessage(msg);
            printf("%s\n", msg.str());
            return false;
        }

        IConstECLWorkunit* wu = &resp->getWorkunit();
        if(wu)
        {
            doFormat(s, wu);
            // Print the results
            if (fp != NULL)
            {
                fprintf(fp, "%s", s.str());
            }
        }
        else
            return false;
    }
    else
    {
        Owned<IClientWUQueryRequest> req = wuclient->createWUQueryRequest();
        req->setOwner(globals->queryProp("OWNER"));

        Owned<IClientWUQueryResponse> resp = wuclient->WUQuery(req);
        if(!resp)
            return false;
        const IMultiException* excep = &resp->getExceptions();
        if(excep != NULL && excep->ordinality() > 0)
        {
            StringBuffer msg;
            excep->errorMessage(msg);
            printf("%s\n", msg.str());
            return false;
        }
        IArrayOf<IConstECLWorkunit>& wus = resp->getWorkunits();
        wus.sort(compareWUs);
        ForEachItemIn(idx, wus)
        {
            doFormat(s, &wus.item(idx));

            // Print the results
            if (fp != NULL)
            {
                fprintf(fp, "%s", s.str());
                s.clear();
            }
        }
    }
    return true;
}

StringBuffer &ListHelper::doFormat(StringBuffer &s, IConstECLWorkunit * w)
{
    if(!format)
        format.set(new DefaultFormatType());

    if (format->embedNames())
    {
        s.append(columns[0].get())
         .append(format->getNameValueSeparator());
    }
    SCMStringBuffer sbuf;
    s.append(format->getDataDelimiter())
     .append(w->getWuid())
     .append(format->getDataDelimiter())
     .append(format->getValueSeparator());

    if (format->embedNames())
    {
        s.append(columns[1].get())
         .append(format->getNameValueSeparator());
    }
    sbuf.clear();
    s.append(format->getDataDelimiter())
     .append(w->getOwner())
     .append(format->getDataDelimiter())
     .append(format->getValueSeparator());

    if (format->embedNames())
    {
        s.append(columns[2].get())
         .append(format->getNameValueSeparator());
    }
    sbuf.clear();
    s.append(format->getDataDelimiter())
     .append(w->getJobname())
     .append(format->getDataDelimiter())
     .append(format->getValueSeparator());

    if (format->embedNames())
    {
        s.append(columns[3].get())
         .append(format->getNameValueSeparator());
    }
    sbuf.clear();
    s.append(format->getDataDelimiter())
     .append(w->getState())
     .append(format->getDataDelimiter())
     .append(format->getRecordSeparator()); 

    return s;
}

