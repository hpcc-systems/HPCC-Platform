/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems®.

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

#ifndef __ESDL_TRANSFORMER_HPP__
#define __ESDL_TRANSFORMER_HPP__

#include "esp.hpp"
#include "soapesp.hpp"
#include "ws_ecl_client.hpp"
#include "esdl_def.hpp"

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

typedef void REQUEST_HOOK (IEspContext& ctx, IEspStruct& req, StringBuffer& query, void* parm);

#define ESDL_TRANS_SOAP_IN            0x0001
#define ESDL_TRANS_SOAP_OUT            0x0002
#define ESDL_TRANS_ROW_IN            0x0004
#define ESDL_TRANS_ROW_OUT            0x0008

#define ESDL_TRANS_TRIM                0x0010
#define ESDL_TRANS_NO_DEFAULTS        0x0020

#define ESDL_TRANS_INPUT_XMLTAG        0x0100
#define ESDL_TRANS_OUTPUT_XMLTAG    0x0200
#define ESDL_TRANS_OUTPUT_ROOT        0x0400

#define ESDL_TRANS_START_AT_ROOT    0x1000

interface IXmlWriterExt;
interface IEsdlTransformer : extends IInterface
{
    virtual IEsdlMethodInfo *queryMethodInfo(const char* service, const char *method)=0;
    virtual int process(IEspContext &ctx, EsdlProcessMode mode, const char* service, const char *method, StringBuffer &xmlout, const char *xmlin, unsigned int flags=0, const char *ns=NULL, const char *schema_location=NULL)=0;
    virtual int process(IEspContext &ctx, EsdlProcessMode mode, const char* service, const char *method, IPropertyTree &in, IXmlWriterExt * writer, unsigned int flags, const char *ns=NULL)=0;
    virtual int processElement(IEspContext &ctx, const char* service, const char *parentStructName, IXmlWriterExt * writer, const char *xmlin)=0;
    virtual void processHPCCResult(IEspContext &ctx, IEsdlDefMethod &mthdef, const char *xml, IXmlWriterExt * writer, StringBuffer &logdata, unsigned int flags = 0, const char *ns=NULL, const char *schema_location=NULL)=0;
};

esdl_decl IEsdlTransformer *createEsdlXFormerFromXMLFiles(StringArray &files, StringArray &types);

esdl_decl IEsdlTransformer *createEsdlXFormerFromXMLFilesV2(StringArray &files);

esdl_decl IEsdlTransformer *createEsdlXFormer(IEsdlDefinition* def);


#endif
