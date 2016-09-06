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

#include "authmap.ipp"

int CAuthMap::add(const char* path, ISecResourceList* resourceList)
{
    if(!path || !*path)
    {
        DBGLOG("can't add to CAuthMap, path is NULL");
        return -1;
    }

    StringBuffer s;
    DBGLOG("Adding authentication information for %s: %s", path, resourceList?resourceList->toString(s).str():"");

    m_paths.append(path);
    m_resourcelists.append(*new CSecResourceListHolder(resourceList));
    return 0;
}

ISecResourceList* CAuthMap::queryResourceList(const char* path)
{
    if(!path || !*path)
        return NULL;

    int pathlen = strlen(path);

    const char* curmatch = NULL;
    ISecResourceList* curlist = NULL;
    ForEachItemIn(x, m_paths)
    {
        const char* curpath = (char*)m_paths.item(x);
        if(!curpath || !*curpath)
            continue;
        
        int curlen = strlen(curpath);
        if(pathlen >= curlen && strncmp(curpath, path, strlen(curpath)) == 0 && (path[curlen - 1] == '/' || path[curlen] == '/' || path[curlen] == '\0'))
        {
            if(curmatch == NULL || strlen(curmatch) < strlen(curpath))
            {
                curmatch = curpath;
                curlist = m_resourcelists.item(x).list();
                //Keep comparing, because it need to find the longest fit.
            }
        }
    }

    return curlist;
}

ISecResourceList* CAuthMap::getResourceList(const char* path)
{
    if(!path || !*path)
        return NULL;

    if(strcmp(path, "*") == 0)
    {
        ISecResourceList* dest = NULL;
        ForEachItemIn(x, m_resourcelists)
        {
            ISecResourceList* rlist = m_resourcelists.item(x).list();
            if(!dest)
                dest = rlist->clone();
            else
                rlist->copyTo(*dest);
        }
        return dest;
    }

    ISecResourceList* rlist = queryResourceList(path);
    if(rlist)
        rlist = rlist->clone();

    return rlist;
}

bool CAuthMap::shouldAuth(const char* path)
{
    if(!path || !*path)
        return false;

    int pathlen = strlen(path);

    ForEachItemIn(x, m_paths)
    {
        const char* curpath = (char*)m_paths.item(x);
        if(!curpath || !*curpath)
            continue;
        int curlen = strlen(curpath);
        if(pathlen >= curlen && strncmp(curpath, path, strlen(curpath)) == 0 && (path[curlen - 1] == '/' || path[curlen] == '/' || path[curlen] == '\0'))
        {
            // Can return because it only need to find one match, not the longest.
            return true;
        }
    }

    return false;
}

bool CAuthMap::addToBackend()
{
    if(m_secmgr == NULL)
        return false;

    bool ok = true;
    ForEachItemIn(x, m_resourcelists)
    {
        ISecResourceList* curlist = (ISecResourceList*)m_resourcelists.item(x).list();
        if(curlist == NULL)
            continue;
        ISecUser* usr = NULL;
        bool ret = m_secmgr->addResources(*usr, curlist);
        ok = ok && ret;
    }

    return ok;
}

unsigned str2perm(const char* permstr)
{
    unsigned perm;
    if(permstr == NULL)
    {
        PROGLOG("permission string is NULL, using default");
        perm = DEFAULT_REQUIRED_ACCESS;
    }
    else if(stricmp(permstr, "None") == 0)
    {
        perm = SecAccess_None;
    }
    else if(stricmp(permstr, "Access") == 0)
    {
        perm = SecAccess_Access;
    }
    else if(stricmp(permstr, "Read") == 0)
    {
        perm = SecAccess_Read;
    }
    else if(stricmp(permstr, "Write") == 0)
    {
        perm = SecAccess_Write;
    }
    else if(stricmp(permstr, "Full") == 0)
    {
        perm = SecAccess_Full;
    }
    else
    {
        PROGLOG("using default required access permission");
        perm = DEFAULT_REQUIRED_ACCESS;
    }
    return perm;
}

const char* resTypeDesc(SecResourceType type)
{
    switch(type)
    {
    case RT_DEFAULT: return "Default";
    case RT_MODULE: return "Module";
    case RT_SERVICE: return "Service";
    case RT_FILE_SCOPE: return "FileScope";
    case RT_WORKUNIT_SCOPE: return "Workunit_Scope";
    case RT_SUDOERS: return "Sudoers";
    case RT_TRIAL: return "Trial";
    case RT_VIEW_SCOPE: return "View";
    default: return "<unknown>";
    }
}       

