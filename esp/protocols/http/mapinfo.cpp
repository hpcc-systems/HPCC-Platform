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

#pragma warning(disable:4786)

#include "mapinfo.hpp"
#include "esp.hpp"
#include <string>
#include <map>

using namespace std;

//---------------------------------------------------------------
// class CNullMapInfo

class CNullMapInfo : public CInterface, implements IMapInfo
{
public:
    IMPLEMENT_IINTERFACE;
    virtual double getMinVersion(const char* structName, const char* field) { return -1; }
    virtual double getMaxVersion(const char* structName, const char* field) { return -1; }
    virtual double getDeprVersion(const char* structName, const char* field) { return -1; }
    virtual const char* getOptional(const char* structName, const char* field) { return NULL; }
    virtual void addDeprVersion(const char* structName, const char* field, double version) { }
    virtual void addMaxVersion(const char* structName, const char* field, double version) { }
    virtual void addMinVersion(const char* structName, const char* field, double version) { } 
    virtual void addOptional(const char* structName, const char* field, const char* option) { }
    virtual void toString(class StringBuffer& s) {  s.append("Null map info:"); }
};

//---------------------------------------------------------------
// class CMapInfo

class CMapInfo : public CInterface, implements IMapInfo
{
protected:
    typedef map<string, double, less<string> > VersionMap;
    typedef map<string, string,less<string> > OptionMap;

    VersionMap m_minvers;
    VersionMap m_maxvers;
    VersionMap m_deprvers;
    OptionMap  m_optionals;

public:
    IMPLEMENT_IINTERFACE;

    CMapInfo() { }

    virtual double getMinVersion(const char* structName, const char* field);
    virtual double getMaxVersion(const char* structName, const char* field);
    virtual double getDeprVersion(const char* structName, const char* field);
    virtual const char* getOptional(const char* structName, const char* field);

    virtual void addDeprVersion(const char* structName, const char* field, double version);
    virtual void addMaxVersion(const char* structName, const char* field, double version);
    virtual void addMinVersion(const char* structName, const char* field, double version);
    virtual void addOptional(const char* structName, const char* field, const char* option);

    virtual void toString(StringBuffer& s);
};

double CMapInfo::getMinVersion(const char* structName, const char* field) 
{
    string tag = string(structName)+":"+field;
    VersionMap::iterator it = m_minvers.find(tag);
    if (it!=m_minvers.end())
        return (*it).second;
    
    return -1;
}

double CMapInfo::getMaxVersion(const char* structName, const char* field) 
{
    string tag = string(structName)+":"+field;
    VersionMap::iterator it = m_maxvers.find(tag);
    if (it!=m_maxvers.end())
        return (*it).second;
    return -1;
}

double CMapInfo::getDeprVersion(const char* structName, const char* field) 
{
    string tag = string(structName)+":"+field;
    VersionMap::iterator it = m_deprvers.find(tag);
    if (it!=m_deprvers.end())
        return (*it).second;
    return -1;
}

const char* CMapInfo::getOptional(const char* structName, const char* field)
{
    string tag = string(structName)+":"+field;
    OptionMap::iterator it = m_optionals.find(tag);
    return (it==m_optionals.end()) ? NULL : (*it).second.c_str();
}

void CMapInfo::addMaxVersion(const char* structName, const char* field, double version) 
{
    string tag = string(structName)+":"+field;
    m_maxvers.insert(pair<string,double>(tag,version));
}

void CMapInfo::addDeprVersion(const char* structName, const char* field, double version) 
{
    string tag = string(structName)+":"+field;
    m_deprvers.insert(pair<string,double>(tag,version));
}

void CMapInfo::addMinVersion(const char* structName, const char* field, double version) 
{
    string tag = string(structName)+":"+field;
    m_minvers.insert(pair<string,double>(tag,version));
}

void CMapInfo::addOptional(const char* structName, const char* field, const char* option) 
{
    string tag = string(structName)+":"+field;
    m_optionals.insert(pair<string,string>(tag,option));
}

void CMapInfo::toString(StringBuffer& s) 
{
    VersionMap::const_iterator it;
    s.append("=== MinVer ===\n");
    for (it = m_minvers.begin(); it!=m_minvers.end();it++)
        s.appendf("%s: %g\n", it->first.c_str(), it->second);
    s.append("=== MaxVer ===\n");
    for (it = m_maxvers.begin(); it!=m_maxvers.end();it++)
        s.appendf("%s: %g\n", it->first.c_str(), it->second);

    s.append("=== DeprVer ===\n");
    for (it = m_deprvers.begin(); it!=m_deprvers.end();it++)
        s.appendf("%s: %g\n", it->first.c_str(), it->second);

    s.append("=== Optional ===\n");
    for (OptionMap::const_iterator it2 = m_optionals.begin(); it2!=m_optionals.end();it2++)
        s.appendf("%s: %s\n", it2->first.c_str(), it2->second.c_str());
}

//---------------------------------------------------------------
// factories

IMapInfo* createMapInfo() { return new CMapInfo(); }
IMapInfo* createNullMapInfo() { return new CNullMapInfo(); }

// end
//---------------------------------------------------------------
