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

#ifndef _AUTHMAP_IPP__
#define _AUTHMAP_IPP__

#include "jliball.hpp"
#include "seclib.hpp"

SecAccessFlags str2perm(const char* permstr);

class CSecResourceListHolder : public CInterface, implements IInterface
{
private:
    ISecResourceList* m_list;
public:
    IMPLEMENT_IINTERFACE;
    CSecResourceListHolder(ISecResourceList* list) : m_list(list) {}
    ISecResourceList* list() {return m_list;}
};

// Here we're using 2 arrays instead of a hashmap due to the need 
// to find the best matching path prefix.
class CAuthMap : public CInterface, implements IAuthMap
{
private:
    StringArray m_paths;
    IArrayOf<CSecResourceListHolder> m_resourcelists;
    ISecManager* m_secmgr;

public:
    IMPLEMENT_IINTERFACE;

    CAuthMap(ISecManager* secmgr) {m_secmgr = secmgr;};
    virtual ~CAuthMap()
    {
        ForEachItemIn(x, m_resourcelists)
        {
            ISecResourceList* rlist = m_resourcelists.item(x).list();
            if (rlist)
                rlist->Release();
        }
    }
    int add(const char* path, ISecResourceList* resourceList);
    bool shouldAuth(const char* path);
    ISecResourceList* queryResourceList(const char* path);
    ISecResourceList* getResourceList(const char* path);
    bool addToBackend();
};

#endif
