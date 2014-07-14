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
#include "eclplus.hpp"
#include "formattypes.ipp"
#include "ListHelper.ipp"
#include "DeleteHelper.ipp"
#include "DumpHelper.ipp"
#include "ViewHelper.ipp"
#include "QueryHelper.ipp"
#include "ws_workunits.hpp"
#include "bindutil.hpp"

IClientWsWorkunits * createWorkunitsClient(IProperties * _globals)
{
    Owned<IClientWsWorkunits> wuclient = createWsWorkunitsClient();
    
    const char* eclwatch = _globals->queryProp("server");
    if(eclwatch == NULL)
        throw MakeStringException(0, "Server url not defined");
    
    StringBuffer url;
    if(Utils::strncasecmp(eclwatch, "http://", 7) != 0 && Utils::strncasecmp(eclwatch, "https://", 8) != 0)
        url.append("http://");
    url.append(eclwatch);
    if(strchr(url.str() + 7, ':') == NULL)
        url.append(":8010/");
    if(url.charAt(url.length() - 1) != '/')
        url.append("/");
    url.append("WsWorkUnits");
    wuclient->addServiceUrl(url.str());
    const char* username = _globals->queryProp("user");
    if (!username)
        username = _globals->queryProp("owner");
    const char* password = _globals->queryProp("password");
    if(username != NULL)
        wuclient->setUsernameToken(username, password, NULL);

    return LINK(wuclient.get());
}

IFormatType * createFormatter(IProperties * globals)
{
    // Create a formatter
    IFormatType * format;
    if(globals->hasProp("format"))
    {
        const char * fmt = globals->queryProp("format");
        
        if(!fmt || !*fmt || !stricmp(fmt, "default"))
        {
            format = new DefaultFormatType();
        }
        else if(!stricmp(fmt, "csvh"))
        {
            format = new CSVHFormatType();
        }
        else if(!stricmp(fmt, "csv"))
        {
            format = new CSVFormatType();
        }
        else if(!stricmp(fmt, "runecl"))
        {
            format = new RuneclFormatType();
        }
        else if(!stricmp(fmt, "xml"))
        {
            format = new XmlFormatType();
        }
        else if(!stricmp(fmt, "bin") || !stricmp(fmt, "binary"))
        {
            format = new BinFormatType;
        }
        else
        {
            throw MakeStringException(-1, "format %s not supported", fmt);
        }
    }
    else
    {
        format = new DefaultFormatType();
    }
    return format;
}

IEclPlusHelper * createEclPlusHelper(IProperties * globals)
{
    // Check to see what kind of helper to return !
    IFormatType * format = createFormatter(globals);
    IEclPlusHelper * helper = NULL;
    if(globals->hasProp("action"))
    {
        const char * action = globals->queryProp("action");
        if(!stricmp(action, "list"))
        {
            helper = new ListHelper(LINK(globals), format);
        }
        // Now re-enable delete
        else if(!stricmp(action, "delete"))
        {
            helper = new DeleteHelper(LINK(globals), format);
        }
        else if(!stricmp(action, "dump"))
        {
            helper = new DumpHelper(LINK(globals), format);
        }
        else if(!stricmp(action, "graph"))
        {
            helper = new GraphHelper(LINK(globals), format);
        }
        else if(!stricmp(action, "view"))
        {
            helper = new ViewHelper(LINK(globals), format);
        }
        else if(!stricmp(action, "query"))
        {
            helper = new QueryHelper(LINK(globals), format);
        }
        else if(!stricmp(action, "abort"))
        {
            helper = new AbortHelper(LINK(globals), format);
        }
        else if(!stricmp(action, "rerun"))
        {
            helper = new RerunHelper(LINK(globals), format);
        }
        else
        {
            ::Release(format);
            throw MakeStringException(-1, "unknown action");
        }
    }
    else
        ::Release(format);
    return helper;
}



