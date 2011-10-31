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
#include <algorithm>
#include "jlib.hpp"
#include "jmisc.hpp"
#include "QueryHelper.ipp"
#include "ViewHelper.ipp"

QueryHelper::QueryHelper(IProperties * _globals, IFormatType * _format) : globals(_globals), format(_format), wuclient(createWorkunitsClient(_globals))
{
}

QueryHelper::~QueryHelper()
{
}

bool QueryHelper::doit(FILE * fp)
{
    Owned<IClientWUCreateRequest> creq = wuclient->createWUCreateRequest();
    Owned<IClientWUCreateResponse> cresp = wuclient->WUCreate(creq);
    const IMultiException* excep = &cresp->getExceptions();
    if(excep != NULL && excep->ordinality() > 0)
    {
        StringBuffer msg;
        excep->errorMessage(msg);
        printf("%s\n", msg.str());
        return false;
    }

    IConstECLWorkunit* wu = &cresp->getWorkunit();
    if(!wu)
    {
        printf("can't create workunit\n");
        return false;
    }
    
    Owned<IClientWUUpdateRequest> ureq = wuclient->createWUUpdateRequest();
    ureq->setWuid(wu->getWuid());

    // Make a workUnit
    StringBuffer jobname;
    if(globals->hasProp("jobname"))
        jobname.append(globals->queryProp("jobname"));

    StringBuffer ecl;
    if (globals->getProp("ecl", ecl))
    {
        if (ecl.length() && ecl.charAt(0)=='@')
        {
            StringBuffer filename(ecl.str()+1);
            ecl.clear().loadFile(filename);
            if (jobname.length() == 0)
                splitFilename(filename, NULL, NULL, &jobname, NULL);
        }
        ureq->setQueryText(ecl.str());
    }
    else if (globals->hasProp("main"))
        ureq->setQueryMainDefinition(globals->queryProp("main"));
    else if (globals->hasProp("attr"))
        ureq->setQueryText(globals->queryProp("attr"));

    if (globals->getPropInt("compileOnly", 0)!=0)
        ureq->setAction(WUActionCompile);
    if (jobname.length())
        ureq->setJobname(jobname);

    IArrayOf<IEspDebugValue> dvals;
    IArrayOf<IEspApplicationValue> avals;
    StringBuffer xmlParams;

    Owned<IPropertyIterator> it = globals->getIterator();
    bool xmlSeen = false;
    ForEach(*it)
    {
        const char * key = it->getPropKey();
        if (key && strlen(key)>1)
        {
            if(key[0] == '-')
            {
                if (key[1] == 'f')
                {
                    Owned<IEspDebugValue> dval = createDebugValue();
                    dval->setName(&key[2]);
                    dval->setValue(globals->queryProp(key));
                    dvals.append(*dval.getLink());
                }
                //All other options are ignored.
            }
            else if(key[0] == '_')
            {
                Owned<IEspApplicationValue> aval = createApplicationValue();
                aval->setApplication("eclplus");
                aval->setName(&key[1]);
                aval->setValue(globals->queryProp(key));
                avals.append(*aval.getLink());
            }
            else if(key[0] == '/')
            {
                if (xmlSeen)
                    throw MakeStringException(0, "query option must not be used with stored or /, and cannot appear more than once");
                // The / form is expected to be used for scalars, so xmlEncode is appropriate.
                // To pass sets or datasets, use the xml= version
                xmlParams.appendf("<%s>", &key[1]);
                encodeXML(globals->queryProp(key), xmlParams);
                xmlParams.appendf("</%s>", &key[1]);
            }
            else if(stricmp(key, "stored")==0)
            {
                if (xmlSeen)
                    throw MakeStringException(0, "query option must not be used with stored or /, and cannot appear more than once");
                const char *xml = globals->queryProp(key);
                try
                {
                    Owned<IPropertyTree> checkValid = createPTreeFromXMLString(xml);
                }
                catch (IException *E)
                {
                    StringBuffer msg;
                    E->errorMessage(msg);
                    E->Release();
                    throw MakeStringException(0, "Invalid xml: %s", msg.str());
                }
                xmlParams.append(xml);
            }
            else if(stricmp(key, "query")==0)
            {
                if (xmlSeen || xmlParams.length())
                    throw MakeStringException(0, "query option must not be used with stored or /, and cannot appear more than once");
                xmlSeen = true;
                StringBuffer xml;
                if (!globals->getProp(key, xml))
                    throw MakeStringException(0, "Invalid value for query= parameter");
                if (xml.length() && xml.charAt(0)=='@')
                {
                    StringBuffer filename(xml.str()+1);
                    xml.clear().loadFile(filename);
                }
                try
                {
                    Owned<IPropertyTree> checkValid = createPTreeFromXMLString(xml);
                }
                catch (IException *E)
                {
                    StringBuffer msg;
                    E->errorMessage(msg);
                    E->Release();
                    throw MakeStringException(0, "Invalid xml: %s", msg.str());
                }
                xmlParams.append(xml);
            }
        }
    }
    if(dvals.length() > 0)
        ureq->setDebugValues(dvals);
    if(avals.length() > 0)
        ureq->setApplicationValues(avals);
    if (xmlParams.length())
    {
        if (!xmlSeen)
        {
            xmlParams.insert(0, "<Query>");
            xmlParams.append("</Query>");
        }
        ureq->setXmlParams(xmlParams);
    }

    Owned<IClientWUUpdateResponse> uresp = wuclient->WUUpdate(ureq);
    const IMultiException* uexcep = &uresp->getExceptions();
    if(uexcep != NULL && uexcep->ordinality() > 0)
    {
        StringBuffer msg;
        uexcep->errorMessage(msg);
        printf("%s\n", msg.str());
        return false;
    }

    // Execute it
    return doSubmitWorkUnit(fp, wu->getWuid(), globals->queryProp("cluster"));
}


bool QueryHelper::doSubmitWorkUnit(FILE * fp, const char * wuid, const char* cluster)
{
    Owned<IClientWUSubmitRequest> req = wuclient->createWUSubmitRequest();
    req->setWuid(wuid);
    req->setCluster(cluster);
    req->setNotifyCluster(true);
    if(globals->hasProp("snapshot"))
        req->setSnapshot(globals->queryProp("snapshot"));

    Owned<IClientWUSubmitResponse> resp = wuclient->WUSubmit(req);
    if(!resp)
    {
        printf("error submitting workunit %s\n", wuid);
        return false;
    }
    else
    {
        const IMultiException* excep = &resp->getExceptions();
        if(excep != NULL && excep->ordinality() > 0)
        {
            StringBuffer msg;
            excep->errorMessage(msg);
            printf("%s\n", msg.str());
            return false;
        }
        printf("Workunit %s submitted\n", wuid);
    }

    // Wait for the results (or not)
    bool infinite = true;
    int timeout = -1;
    if(globals->hasProp("timeout"))
    {
        infinite = false;
        timeout = globals->getPropInt("timeout", 0)*1000;
    }
    bool compileOnly = globals->getPropInt("compileOnly", 0)!=0;
    bool returnOnWaitState = globals->getPropInt("returnOnWait", 0)!=0;
    if(timeout > 0 || infinite)
    {
        if (compileOnly)
        {
            Owned<IClientWUWaitRequest> req = wuclient->createWUWaitCompiledRequest();
            req->setWuid(wuid);
            req->setWait(timeout);
            req->setReturnOnWait(returnOnWaitState);
            Owned<IClientWUWaitResponse> resp = wuclient->WUWaitCompiled(req);
            WUState s = (WUState)resp->getStateID();
            return s == WUStateCompiled;
        }
        else
        {
            WUState s;

            int initial_wait = 30;
            int polling_period = 30;
            int waited = 0;
            int actual_wait = -1;
            while(1)
            {
                if(actual_wait != 0)
                {
                    // Initially, wait initial_wait
                    actual_wait = infinite?initial_wait*1000: std::min(timeout, initial_wait*1000);
                }

                Owned<IClientWUWaitRequest> req = wuclient->createWUWaitCompleteRequest();
                req->setWuid(wuid);
                req->setWait(actual_wait);
                Owned<IClientWUWaitResponse> resp = wuclient->WUWaitComplete(req);
                const IMultiException* excep = &resp->getExceptions();
                if(excep != NULL && excep->ordinality() > 0)
                {
                    StringBuffer msg;
                    excep->errorMessage(msg);
                    printf("%s\n", msg.str());
                    return false;
                }

                s = (WUState)resp->getStateID();
                if (s != WUStateUnknown)
                    break;

                if(actual_wait != 0)
                {
                    waited += actual_wait;
                    actual_wait = 0;
                    if (!infinite && (waited >= timeout))
                        break;
                }
                else
                {
                    if (!infinite && (waited >= timeout))
                        break;
                    int time_to_wait = infinite?polling_period*1000: std::min(timeout - waited, polling_period*1000);
                    sleep(time_to_wait/1000);
                    waited += time_to_wait;
                }

            }

            if (s != WUStateUnknown)
            {
                globals->setProp("wuid", wuid);
                if(format)
                    LINK(format);
                Owned<IEclPlusHelper> viewer = new ViewHelper(LINK(globals), format);
                viewer->doit(fp);
            }
            return ((s == WUStateCompleted) || (returnOnWaitState && (s == WUStateWait)));
        }
    }
    else
    {       
        StringBuffer s;
        s.append("Submitted WUID ").append(wuid);
        fprintf(fp, "%s", s.str());
        return true;
    }
}


bool RerunHelper::doit(FILE * fp)
{   
    if(!globals->hasProp("wuid"))
    {
        printf("No wuid specified");
        return false;
    }
    if(!globals->hasProp("cluster"))
    {
        printf("No cluster name specified");
        return false;
    }

    StringBuffer wuid, cluster;
    globals->getProp("WUID", wuid);
    globals->getProp("cluster", cluster);
    // Execute it
    return doSubmitWorkUnit(fp, wuid.str(), cluster.str());
}

