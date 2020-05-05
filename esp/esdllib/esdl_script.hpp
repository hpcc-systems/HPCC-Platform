/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2020 HPCC Systems®.

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

#ifndef ESDL_SCRIPT_HPP_
#define ESDL_SCRIPT_HPP_

#ifdef ESDLLIB_EXPORTS
 #define esdl_decl DECL_EXPORT
#else
 #define esdl_decl
#endif

#include "jlib.hpp"
#include "jstring.hpp"
#include "jptree.hpp"
#include "jlog.hpp"
#include "esp.hpp"

#include "esdl_def.hpp"

#include <map>
#include <mutex>
#include <thread>
#include <initializer_list>

#include "tokenserialization.hpp"
#include "xpathprocessor.hpp"

#define ESDL_SCRIPT_Error                         5700
#define ESDL_SCRIPT_MissingOperationAttr          5710
#define ESDL_SCRIPT_UnknownOperation              5720

interface IEsdlCustomTransform : extends IInterface
{
    virtual void processTransform(IEspContext * context, IPropertyTree *tgtcfg, IEsdlDefService &srvdef, IEsdlDefMethod &mthdef, StringBuffer & content, IPropertyTree * bindingCfg) = 0;
    virtual void processTransform(IEspContext * context, IPropertyTree *tgtcfg, const char *service, const char *method, const char* reqtype, StringBuffer & content, IPropertyTree * bindingCfg) = 0;
    virtual void appendEsdlURIPrefixes(StringArray &prefixes) = 0;
    virtual void toDBGLog() = 0;
};


esdl_decl void processServiceAndMethodTransforms(std::initializer_list<IEsdlCustomTransform *> const &transforms, IEspContext * context, IPropertyTree *tgtcfg, IEsdlDefService &srvdef, IEsdlDefMethod &mthdef, StringBuffer & content, IPropertyTree * bindingCfg);
esdl_decl void processServiceAndMethodTransforms(std::initializer_list<IEsdlCustomTransform *> const &transforms, IEspContext * context, IPropertyTree *tgtcfg, const char *service, const char *method, const char* reqtype, StringBuffer & content, IPropertyTree * bindingCfg);

esdl_decl IEsdlCustomTransform *createEsdlCustomTransform(IPropertyTree &customRequestTransform, const char *ns_prefix);

esdl_decl void registerEsdlXPathExtensions(IXpathContext *xpathCtx, IEspContext *espCtx, const StringArray &prefixes);

#endif /* ESDL_SCRIPT_HPP_ */
