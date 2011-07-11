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

#ifndef _AUTHMAP_IPP__
#define _AUTHMAP_IPP__

#include "jliball.hpp"
#include "seclib.hpp"

unsigned str2perm(const char* permstr);

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
    int add(const char* path, ISecResourceList* resourceList);
    bool shouldAuth(const char* path);
    ISecResourceList* queryResourceList(const char* path);
    ISecResourceList* getResourceList(const char* path);
    bool addToBackend();
};

#endif
