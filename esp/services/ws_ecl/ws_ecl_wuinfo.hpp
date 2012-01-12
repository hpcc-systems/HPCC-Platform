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

#ifndef _WS_ECL_WUINFO_HPP__
#define _WS_ECL_WUINFO_HPP__

#include "workunit.hpp"
#include "wuwebview.hpp"

class WsEclWuInfo : public CInterface
{
public:
    Owned<IConstWorkUnit> wu;
    Owned<IPropertyTree> paraminfo;
    Owned<IPropertyTree> xsds;
    StringBuffer schemacache;

    StringAttr wuid;
    StringAttr username;
    StringAttr password;
    StringAttr qsetname;
    StringAttr queryname;
    
public:
    IMPLEMENT_IINTERFACE;

    WsEclWuInfo(const char *wuid, const char *qset, const char *qname, const char *user, const char *pw);

    IPropertyTreeIterator *getInputSchemas();
    IPropertyTreeIterator *getResultSchemas();

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
