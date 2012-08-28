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
