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
    virtual void processTransform(IEsdlScriptContext * context, const char *srcSection, const char *tgtSection) = 0;
    virtual void processTransformImpl(IEsdlScriptContext * scriptContext, const char *srcSection, const char *tgtSection, IXpathContext *xpathContext, const char *target) = 0;
    virtual void appendPrefixes(StringArray &prefixes) = 0;
    virtual void toDBGLog() = 0;
};

interface IEsdlTransformSet : extends IInterface
{
    virtual void processTransformImpl(IEsdlScriptContext * scriptContext, const char *srcSection, const char *tgtSection, IXpathContext *xpathContext, const char *target) = 0;
    virtual void appendPrefixes(StringArray &prefixes) = 0;
    virtual aindex_t length() = 0;
};

inline bool isEmptyTransformSet(IEsdlTransformSet *set)
{
    if (!set)
        return true;
    return (set->length()==0);
}

#define ESDLScriptEntryPoint_Legacy "CustomRequestTransform"
#define ESDLScriptEntryPoint_BackendRequest "BackendRequest"
#define ESDLScriptEntryPoint_BackendResponse "BackendResponse"
#define ESDLScriptEntryPoint_PreLogging "PreLogging"

interface IEsdlTransformEntryPointMap : extends IInterface
{
    virtual IEsdlTransformSet *queryEntryPoint(const char *name) = 0;
    virtual void removeEntryPoint(const char *name) = 0;
};

interface IEsdlTransformMethodMap : extends IInterface
{
    virtual IEsdlTransformEntryPointMap *queryMethod(const char *method) = 0;
    virtual IEsdlTransformSet *queryMethodEntryPoint(const char *method, const char *name) = 0;
    virtual void removeMethod(const char *method) = 0;
    virtual void addMethodTransforms(const char *method, const char *script, bool &foundNonLegacyTransforms) = 0;
};

esdl_decl IEsdlTransformMethodMap *createEsdlTransformMethodMap();

esdl_decl IEsdlCustomTransform *createEsdlCustomTransform(const char *scriptXml, const char *ns_prefix);

esdl_decl void processServiceAndMethodTransforms(IEsdlScriptContext * scriptCtx, std::initializer_list<IEsdlTransformSet *> const &transforms, const char *srcSection, const char *tgtSection);
esdl_decl void registerEsdlXPathExtensions(IXpathContext *xpathCtx, IEsdlScriptContext *scriptCtx, const StringArray &prefixes);

#endif /* ESDL_SCRIPT_HPP_ */
