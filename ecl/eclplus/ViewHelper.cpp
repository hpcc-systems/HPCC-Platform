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
#include "jexcept.hpp"
#include "jmisc.hpp"
#include "ViewHelper.ipp"

#define DEFAULT_PAGESIZE 500

SCMStringBuffer resultName;

ViewHelper::ViewHelper(IProperties * _globals, IFormatType * _format) : globals(_globals), format(_format), wuclient(createWorkunitsClient(_globals))
{
}

ViewHelper::~ViewHelper()
{
}

bool ViewHelper::doit(FILE * fp)
{
    if (globals->hasProp("WUID"))
    {
        const char* wuid = globals->queryProp("WUID");

        Owned<IClientWUInfoRequest> req = wuclient->createWUInfoRequest();
        req->setWuid(wuid);
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

        IConstECLWorkunit* w = &resp->getWorkunit();
        if (w && fp)
        {
            bool xml = true;
            const char* fmt = globals->queryProp("format");
            if(fmt && (stricmp(fmt, "bin") == 0 || stricmp(fmt, "binary") == 0))
                xml = false;
            IArrayOf<IConstECLException>& exceptions = w->getExceptions();
            ForEachItemIn(ind, exceptions)
            {
                IConstECLException* excep = &exceptions.item(ind);
                if(!excep)
                    continue;

                bool skip = false;
                const char* severity = excep->getSeverity();
                if (severity != NULL && stricmp(severity, "Warning") == 0 && globals->getPropBool("noWarnings", false))
                    skip = true;
                if (severity != NULL && stricmp(severity, "Info") == 0 && globals->getPropBool("noInfo", false))
                    skip = true;
                if (severity != NULL && stricmp(severity, "Error") == 0 && globals->getPropBool("noErrors", false))
                    skip = true;
                if (!skip)
                {
                    int lineno = excep->getLineNo();
                    const char* source = excep->getSource();
                    const char* msg = excep->getMessage();
                    unsigned code = excep->getCode();
                    if (lineno && source != NULL)
                    {
                        if (xml)
                            fprintf(fp, "<%s><source>%s</source><line>%d</line><code>%d</code><message>%s</message></%s>\n", severity, source, lineno, code, msg, severity);
                        else
                            fprintf(fp, "%s: %s(%d) %s\n", severity, source, lineno, msg);
                    }
                    else if(source != NULL)
                    {
                        if (xml)
                            fprintf(fp, "<%s><source>%s</source><code>%d</code><message>%s</message></%s>\n", severity, source, code, msg, severity);
                        else
                            fprintf(fp, "%s: %s %s\n", severity, source, msg);
                    }
                    else
                    {
                        if (xml)
                            fprintf(fp, "<%s><code>%d</code><message>%s</message></%s>\n", severity, code, msg, severity);
                        else
                            fprintf(fp, "%s: %s\n", severity, msg);
                    }
                }
            }

            if (w->getStateID() == WUStateAborted)
            {
                fprintf(fp, "Aborted\n");
                return true;
            }
            if (w->getStateID() == WUStateFailed)
                return false;

            int queryid = 0;

            IArrayOf<IConstECLResult>& results = w->getResults();
            ForEachItemIn(res_ind, results)
            {
                IConstECLResult* result = &results.item(res_ind);
                
                if(!result)
                    continue;
                
                const char* value = result->getValue();
                if(value != NULL && stricmp(value, "[undefined]") == 0)
                    continue;

                if(format)
                    format->printHeader(fp, result->getName());

                const char* rfname = result->getFileName();
                if(!(rfname && *rfname  && (globals->getPropInt("viewFileResults") ==0)))
                {
                    int pagesize = 0;
                    if(globals->hasProp("pagesize"))
                        pagesize = atoi(globals->queryProp("pagesize"));
                    if(pagesize == 0)
                        pagesize = DEFAULT_PAGESIZE;
                    int curpos = 0;
                    int count = 0;
                    int total = 0;
                    do
                    {
                        Owned<IClientWUResultBinRequest> res_req = wuclient->createWUResultBinRequest();
                        res_req->setStart(curpos);
                        res_req->setWuid(wuid);
                        res_req->setSequence(result->getSequence());
                        res_req->setCount(pagesize);
                        if (xml)
                            res_req->setFormat("xml");
                        else
                            res_req->setFormat("raw");
                        Owned<IClientWUResultBinResponse> res_resp = wuclient->WUResultBin(res_req);

                        const IMultiException* excep = &res_resp->getExceptions();
                        if(excep != NULL && excep->ordinality() > 0)
                        {
                            StringBuffer errmsg;
                            excep->errorMessage(errmsg);
                            printf("%s\n", errmsg.str());
                            continue;
                        }

                        const MemoryBuffer& resultbuf = res_resp->getResult();
                        count = res_resp->getCount();
                        total = res_resp->getTotal();
                        
                        if(format)
                        {
                            format->setStartRowNumber(curpos);
                            format->printBody(fp, resultbuf.length(), (char*)resultbuf.toByteArray());
                        }
                        else
                        {
                            // This should never happen
                            fprintf(fp, "%s", resultbuf.toByteArray());
                        }
                        
                        curpos += count;
                    }
                    while (count > 0 && curpos < total - 1);
                }
                
                if(format)
                    format->printFooter(fp);
            }
        }
    }
    return true;
}



