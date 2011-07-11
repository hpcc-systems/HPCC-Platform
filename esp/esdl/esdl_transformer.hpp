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

#include "jliball.hpp"
#include "esp.hpp"
#include "soapesp.hpp"
#include "ws_ecl_client.hpp"

typedef  enum EsdlProcessMode_
{
    EsdlRequestMode,
    EsdlResponseMode
} EsdlProcessMode;

interface IEsdlMethodInfo : extends IInterface
{
    virtual const char *queryMethodName()=0;
    virtual const char *queryRequestType()=0;
    virtual const char *queryResponseType()=0;
};

interface IEsdlTransformer : extends IInterface
{
    virtual IEsdlMethodInfo *queryMethodInfo(const char *method)=0;

    virtual int process(IEspContext &ctx, EsdlProcessMode mode, const char *method, StringBuffer &xmlout, const char *xmlin)=0;
    virtual int process(IEspContext &ctx, EsdlProcessMode mode, const char *method, IClientWsEclRequest &clReq, IEspStruct& r)=0;
};

IEsdlTransformer *createEsdlXFormerFromXMLString(const char *xml);
IEsdlTransformer *createEsdlXFormerFromXMLFile(const char *filename, StringArray &types);

IEsdlTransformer *createEsdlXFormerFromXMLFiles(StringArray &files, StringArray &types);
