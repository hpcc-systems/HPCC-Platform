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

#ifndef _WS_ECL_WUINFO_HPP__
#define _WS_ECL_WUINFO_HPP__

#include "workunit.hpp"
#include "wuwebview.hpp"

class WsEclWuInfo : public CInterface
{
private:
    Owned<IConstWorkUnit> wu;
    Owned<IPropertyTree> paraminfo;
    Owned<IPropertyTree> xsds;
    StringBuffer schemacache;

    StringAttr wuid;
public:
    StringAttr username;
    StringAttr password;
    StringAttr qsetname;
    StringAttr queryname;
    
public:
    IMPLEMENT_IINTERFACE;

    WsEclWuInfo(const char *wuid, const char *qset, const char *qname, const char *user, const char *pw);

    IPropertyTreeIterator *getInputSchemas();
    IPropertyTreeIterator *getResultSchemas();

    const char *ensureWuid();
    const char *queryWuid(){return wuid.get();}
    IConstWorkUnit *ensureWorkUnit();
    void getSchemas(StringBuffer &schemas);
    void getOutputSchema(StringBuffer &schema, const char *name);
    void getInputSchema(StringBuffer &schema, const char *name);
    void getSchemaFromResult(StringBuffer &schema, IConstWUResult &res);
    void updateSchemaCache();

    IPropertyTree *queryParamInfo();

    bool getWsResource(const char *name, StringBuffer &out);

private:
    void addInputSchemas(StringBuffer &schemas, IConstWUResultIterator *results, const char *tag="Input");
    void addOutputSchemas(StringBuffer &schemas, IConstWUResultIterator *results, const char *tag="Result");

};

#endif //_WS_ECL_WUINFO_HPP__
