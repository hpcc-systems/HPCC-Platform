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
#include "jlib.hpp"
#include "ListHelper.ipp"

#define NUMCOLUMNS 4

static CBuildVersion _bv("$HeadURL: https://svn.br.seisint.com/ecl/trunk/ecl/eclplus/ListHelper.cpp $ $Id: ListHelper.cpp 62376 2011-02-04 21:59:58Z sort $");

static StringAttr columns[NUMCOLUMNS] = {"WUID", "OWNER", "JOBNAME", "STATUS"};

ListHelper::ListHelper(IProperties * _globals, IFormatType * _format) : globals(_globals), format(_format), wuclient(createWorkunitsClient(_globals))
{
}

ListHelper::~ListHelper()
{
}

static int compareWUs(IInterface **ll, IInterface **rr)
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

