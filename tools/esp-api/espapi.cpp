/*##############################################################################

    Copyright (C) 2024 HPCC Systems®.

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
#include <cstdlib>
#include <cstring>
#include <iostream>
#include "jfile.hpp"
#include "jprop.hpp"
#include "jscm.hpp"
#include "jstring.hpp"
#include <ostream>
#include <string>
#include <stdio.h>
#include "espapi.hpp"
#include "jutil.hpp"
#include <utility>
using namespace std;
class ConcreteEsdlDefReporter : public EsdlDefReporter
{
protected:
    void reportSelf(Flags flags, const char* component, const char* level, const char* msg) const override
    {
        printf("[%s] [%s] %s\n", component, level, msg);
    }
};

IEsdlDefReporter* createConcreteEsdlDefReporter()
{
    return new ConcreteEsdlDefReporter();
}

void EspDef::addFilesToDefinition(vector<Owned<IFile>> files)
{
    for(Owned<IFile>& file: files)
    {
        if(file->exists() )
        {
            if(file->isFile()==fileBool::foundYes )
            {
                if(file->size() > 0 )
                {
                    esdlDef->addDefinitionsFromFile( file->queryFilename() );
                }
                else
                {
                    cerr << "ESDL definition file source " << file->queryFilename() << " is empty" << endl;
                }

            }
            else
            {
                cerr << "ESDL definition file source " <<  file->queryFilename() << " is not a file" << endl;
            }
        }
        else
        {
            cerr << "ESDL definition file source " << file->queryFilename() << " does not exist" << endl;
        }
    }
}

EspDef::EspDef()
{
    esdlDef.setown(createEsdlDefinition(nullptr, createConcreteEsdlDefReporter));
}

void EspDef::getFiles(vector<Owned<IFile>> &files)
{
    const IProperties &temp =  queryEnvironmentConf();
    string path = temp.queryProp("path");
    path += "/componentfiles/esdl_files/";
    getFiles(files, path.c_str());
}

void EspDef::getFiles(std::vector<Owned<IFile>> &files, const char* path)
{
    const char * mask = "*" ".xml";
    Owned<IFile> esdlDir = createIFile(path);

    Owned<IDirectoryIterator> esdlFiles = esdlDir->directoryFiles(mask, false, false);
    ForEach(*esdlFiles)
    {
        IFile &thisPlugin = esdlFiles->query();
        files.push_back(LINK(&thisPlugin));
    }
}

inline bool EspDef::isStructInTrack(const std::string& reqRes)
{
    return std::find(structTrack.begin(), structTrack.end(), reqRes) != structTrack.end();
}

void EspDef::describeType(const char* reqRes, int indent, std::ostream& out, bool recursive)
{
    IEsdlDefStruct* myStruct = esdlDef->queryStruct(reqRes);
    if(!myStruct)
    {
        return;
    }

    if(isStructInTrack(reqRes))
    {
        return;
    }

    string indentChars(indent * 4, ' ');
    if(!recursive)
    {
        out << indentChars << myStruct->queryName() << endl;
    }

    Owned<IEsdlDefObjectIterator> structChildren = myStruct->getChildren();
    ForEach(*structChildren)
    {
        IEsdlDefObject &structChildrenQuery = structChildren->query();
        Owned<IPropertyIterator> structChildrenQueryProps = structChildrenQuery.getProps();
        vector<pair<const char*, const char*>> propVec;
        const char* type = nullptr;
        const char* name = nullptr;
        const char* typeSuffix = "";
        bool callRecursive = false;

        ForEach(*structChildrenQueryProps)
        {
            const char* propKey = structChildrenQueryProps->getPropKey();
            const char* propValue = structChildrenQueryProps->queryPropValue();
            if(streq(propKey, "complex_type"))
            {
                type = propValue;
                callRecursive = true;
            }
            else if(streq(propKey, "enum_type"))
            {
                type = propValue;
                typeSuffix = "(enum)";
            }
            else if(streq(propKey, "name"))
            {
                name = propValue;
            }
            else if(streq(propKey, "type"))
            {
                if(structChildrenQuery.getEsdlType() == EsdlTypeArray)
                {
                    callRecursive = true;
                    typeSuffix = "[]";
                }
                type = propValue;
            }
            else
            {
                propVec.emplace_back(propKey,propValue);
            }
        }
        if(type == nullptr)
        {
            type = "<unknown>";
        }
        if(name == nullptr)
        {
            name = "<unknown>";
        }

        out << indentChars << type << typeSuffix << " " << name;

        if(!propVec.empty())
        {
            out << " [" << propVec[0].first << " (" << propVec[0].second << ")";
            for(int i=1; i<propVec.size(); i++)
            {
                out << ", ";
                out  << propVec[i].first << " (" << propVec[i].second << ")";
            }
            out << "]";
        }

        out << endl;
        if(callRecursive)
        {
            structTrack.push_back(reqRes);
            describeType(type, indent+1, out, true);
            structTrack.pop_back();
        }
    }
}

bool EspDef::checkValidService(const char* serviceName)
{
    for(const char* &service : allServicesList)
    {
        if(streq(service, serviceName))
        {
            return true;
        }
    }
    return false;
}

bool EspDef::checkValidMethod(const char* methodName, const char* serviceName)
{
    IEsdlDefService *esdlServ = esdlDef->queryService(serviceName);
    if (!esdlServ)
    {
        cerr << "Unknown Service " << serviceName << endl;
        return false;
    }

    Owned<IEsdlDefMethodIterator> methodIter = esdlServ->getMethods();
    ForEach(*methodIter)
    {
        IEsdlDefMethod &tempMethod = methodIter->query();
        if (streq(tempMethod.queryName(), methodName))
        {
            return true;
        }
    }
    return false;
}

int EspDef::loadAllServices()
{
    Owned<IEsdlDefServiceIterator> serviceIter = esdlDef->getServiceIterator();
    if(!serviceIter->first())
    {
        cerr << "No services loaded into the esdl object" << endl;
        return 1;
    }
    ForEach(*serviceIter)
    {
        IEsdlDefService &currentService = serviceIter->query();
        const char* serviceName = currentService.queryName();
        allServicesList.push_back(serviceName);
    }
    return 0;
}

void EspDef::describeAllServices(ostream &out)
{
    Owned<IEsdlDefServiceIterator> serviceIter = esdlDef->getServiceIterator();
    if(!serviceIter->first())
    {
        cerr << "No services loaded into the esdl object" << endl;
        return;
    }
    ForEach(*serviceIter)
    {
        IEsdlDefService &currentService = serviceIter->query();
        const char* serviceName = currentService.queryName();
        out << serviceName;
        Owned<IPropertyIterator> propIter = currentService.getProps();
        bool first = true;
        ForEach(*propIter)
        {
            if(streq(propIter->getPropKey(), "name"))
            {
                continue;
            }
            if(first)
            {
                out << " [";
                first = false;
            }
            else
            {
                out << ", ";
            }
            out << propIter->getPropKey() << " (" << propIter->queryPropValue() << ")";
        }
        if(!first)
        {
            out << "]" << endl;
        }
    }
    return;
}

void EspDef::loadAllMethods(const char* serviceName)
{
    IEsdlDefService *esdlServ = esdlDef->queryService(serviceName);

    if (!esdlServ)
    {
        cerr << "No Service " << serviceName << endl;
        return;
    }

    Owned<IEsdlDefMethodIterator> methodIter = esdlServ->getMethods();
    ForEach(*methodIter)
    {
        IEsdlDefMethod &tempMethod = methodIter->query();
        allMethodsList.push_back(tempMethod.queryMethodName());
    }
    return;
}

void EspDef::describeAllMethods(const char* serviceName, ostream &out)
{
    IEsdlDefService *esdlServ = esdlDef->queryService(serviceName);

    if(!esdlServ)
    {
        cerr << "No Service " << serviceName << endl;
        return;
    }
    out << esdlServ->queryName();
    Owned<IPropertyIterator> propIter = esdlServ->getProps();
    bool first = true;
    ForEach(*propIter)
    {
        if(streq(propIter->getPropKey(), "name"))
        {
            continue;
        }
        if(first)
        {
            out << " [";
            first = false;
        }
        else
        {
            out << ", ";
        }
        out << propIter->getPropKey() << " (" << propIter->queryPropValue() << ")";
    }
    if(!first)
    {
        out << "]" << endl;
    }

    Owned<IEsdlDefMethodIterator> methodIter = esdlServ->getMethods();
    ForEach(*methodIter)
    {
        IEsdlDefMethod &tempMethod = methodIter->query();
        out << "    " << tempMethod.queryMethodName() << " ";
        Owned<IPropertyIterator> propIter = tempMethod.getProps();
        out << "[";
        bool first = true;
        ForEach(*propIter)
        {
            if(streq(propIter->getPropKey(), "name"))
            {
                continue;
            }
            if(!first)
            {
                out << ", ";
            }
            out << propIter->getPropKey() << " (" << propIter->queryPropValue() << ")";
            first = false;
        }
        out << "]" << endl;
    }
    return;
}

void EspDef::printAllServices()
{
    for(const char* &serv:allServicesList)
    {
        cout << serv << endl;
    }
}

void EspDef::printAllMethods()
{
    for(const char* &method:allMethodsList)
    {
        cout << method << endl;
    }
}



void EspDef::describe(const char* serviceName, const char* methodName)
{
    describe(serviceName, methodName, cout);
}

void EspDef::describe(const char* serviceName, const char* methodName, std::ostream& out){
    IEsdlDefService *esdlServ = esdlDef->queryService(serviceName);
    if(!esdlServ)
    {
        out << "Invalid Service " << serviceName << endl;
        return;
    }
    Owned<IEsdlDefMethodIterator> methodIter = esdlServ->getMethods();
    IEsdlDefMethod *methodObj = esdlServ->queryMethodByName(methodName);
    if(methodObj)
    {
        describeType(methodObj->queryRequestType(), 0, out, false);
        describeType(methodObj->queryResponseType(), 0, out, false);
    }
    else
    {
        out << "Invalid Method " << methodName << endl;
        return;
    }
}
