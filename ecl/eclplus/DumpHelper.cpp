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
#include "DumpHelper.ipp"

static CBuildVersion _bv("$HeadURL: https://svn.br.seisint.com/ecl/trunk/ecl/eclplus/DumpHelper.cpp $ $Id: DumpHelper.cpp 64954 2011-05-27 14:50:31Z jprichard $");

DumpHelper::DumpHelper(IProperties * _globals, IFormatType * _format) : globals(_globals), format(_format), wuclient(createWorkunitsClient(_globals))
{
}

DumpHelper::~DumpHelper()
{
}

bool DumpHelper::doit(FILE * fp)
{
    SCMStringBuffer xml;

    // If we were given a workunit dump that one workunit, otherwise moan
    if (globals->hasProp("WUID"))
    {
        const char* wuid = globals->queryProp("WUID");
        const char *whichPath = globals->queryProp("prop");
        if (whichPath)
        {
            if (stricmp(whichPath, "ecl")==0)
            {                   
                Owned<IClientWUInfoRequest> inforeq = wuclient->createWUInfoRequest();
                inforeq->setWuid(wuid);
                Owned<IClientWUInfoResponse> inforesp = wuclient->WUInfo(inforeq);
                if(!inforesp || &inforesp->getWorkunit() == NULL)
                {
                    printf("Workunit %s not found\n", wuid);
                    return false;
                }

                IConstECLWorkunit* wu = &inforesp->getWorkunit();
                IConstECLQuery* query = &wu->getQuery();
                if(query)
                    xml.set(query->getText());
            }
            else
            {
                printf("Unrecognized parameter prop=%s", whichPath);
                return false;
            }
        }
        else
        {
            Owned<IClientWULogFileRequest> req = wuclient->createWUFileRequest();
            req->setWuid(wuid);
            req->setType("XML");
            Owned<IClientWULogFileResponse> resp = wuclient->WUFile(req);
            if(!resp)
            {
                printf("Workunit %s not found\n", wuid);
                return false;
            }
            const IMultiException* excep = &resp->getExceptions();
            if(excep != NULL && excep->ordinality())
            {
                unsigned i = 0;
                while (i < excep->ordinality())
                {
                    StringBuffer msg;
                    excep->item(i).errorMessage(msg);
                    unsigned code = excep->item(i).errorCode();
                    printf("<Error><code>%d</code><message>%s</message></Error>\n", code, msg.str());
                }
                return false;
            }

            const MemoryBuffer & xmlmem = resp->getThefile();
            StringBuffer xmlbuf;
            xmlbuf.append(xmlmem.length(), xmlmem.toByteArray());
            xml.set(xmlbuf.str());
        }
        // Print the results
        if (fp != NULL)
        {
            fprintf(fp, "%s", xml.str());
        }
        return true;
    }

    return false;
}


GraphHelper::GraphHelper(IProperties * _globals, IFormatType * _format) : globals(_globals), format(_format), wuclient(createWorkunitsClient(_globals))
{
}

GraphHelper::~GraphHelper()
{
}

bool GraphHelper::doit(FILE * fp)
{
    if (globals->hasProp("WUID"))
    {
        const char* wuid = globals->queryProp("WUID");
        Owned<IClientWUProcessGraphRequest> req = wuclient->createWUProcessGraphRequest();
        StringBuffer fname(wuid);
        req->setWuid(fname.str());

        StringBuffer graph;
        if(globals->hasProp("graph"))
            graph.append(globals->queryProp("graph"));

        req->setName(graph.str());
        Owned<IClientWUProcessGraphResponse> resp = wuclient->WUProcessGraph(req);
        const IMultiException* excep = &resp->getExceptions();
        if(excep != NULL && excep->ordinality() > 0)
        {
            StringBuffer msg;
            excep->errorMessage(msg);
            printf("%s\n", msg.str());
            return false;
        }

            StringBuffer graphbuf;
            graphbuf.append(resp->getTheGraph());
            fprintf(fp, "%s", graphbuf.str());
            return true;
        
        return false;
    }
    else 
    {
        printf("Please specify the WUID\n");
        return false;
    }
}


