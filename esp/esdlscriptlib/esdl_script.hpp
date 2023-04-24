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

#ifdef ESDLSCRIPTLIB_EXPORTS
 #define esdlscript_decl DECL_EXPORT
#else
 #define esdlscript_decl
#endif

#include "jlib.hpp"
#include "jstring.hpp"
#include "jptree.hpp"
#include "jlog.hpp"
#include "esp.hpp"

//#include "esdl_def.hpp"

#include "datamaskingengine.hpp"
#include "tracer.h"

#include <map>
#include <mutex>
#include <thread>
#include <initializer_list>

#include "tokenserialization.hpp"
#include "xpathprocessor.hpp"

#define ESDL_SCRIPT_Error                         5700
#define ESDL_SCRIPT_MissingOperationAttr          5710
#define ESDL_SCRIPT_InvalidOperationAttr          5711
#define ESDL_SCRIPT_UnknownOperation              5720

#define ESDL_SCRIPT_Warning                       5800

interface IEsdlFunctionRegister;

/**
 * @brief Script-specific context associated with a sectional document model.
 *
 * As a convenience, the context presents itself as the model instance with which it is used. An
 * implementation must either be assigned or create a model instance for use.
 */
interface IEsdlScriptContext : extends ISectionalXmlDocModel
{
    virtual IEspContext* queryEspContext() const = 0;
    virtual IEsdlFunctionRegister* queryFunctionRegister() const = 0;
    virtual void setTraceToStdout(bool val) = 0;
    virtual bool getTraceToStdout() const = 0;
    virtual void setTestMode(bool val) = 0; //enable features that help with unit testing but should never be used in production
    virtual bool getTestMode() const = 0;
    virtual ITracer& tracerRef() const = 0;

    /**
     * @brief Attempt to enable data masking.
     *
     * Data masking cannot be disabled once enabled.
     *
     * @param domainId
     * @param version
     * @return true  the requested masking is enabled
     * @return false the requested masking is not enabled, possibly because other masking is
     *               already enabled
     */
    virtual bool enableMasking(const char* domainId, uint8_t version) = 0;

    /**
     * @brief Is data masking enabled?
     *
     * @return true  enabled
     * @return false disabled
     */
    virtual bool maskingEnabled() const = 0;

    /**
     * @brief Return a new reference to the current data masking context.
     *
     * The result can be NULL if data masking has not been enabled.
     *
     * @return IDataMaskingProfileContext*
     */
    virtual IDataMaskingProfileContext* getMasker() const = 0;

    /**
     * @brief Update the controlling options of the `trace` operation.
     *
     * Has no effect if the options are already locked.
     *
     * @param enabled
     * @param locked
     */
    virtual void setTraceOptions(bool enabled, bool locked) = 0;

    /**
     * @brief Is the `trace` operation enabled?
     *
     * @return true  enabled
     * @return false disabled
     */
    virtual bool isTraceEnabled() const = 0;

    /**
     * @brief Are `trace` options immutable?
     *
     * @return true  options cannot be changed
     * @return false options can be changed
     */
    virtual bool isTraceLocked() const = 0;

protected:
    /**
     * @brief Helper class encapsulating the use of `pushMaskerScope` and `popMaskerScope`.
     */
    friend class EsdlScriptMaskerScope;

    /**
     * @brief Create and use a copy of the current data masking context.
     */
    virtual void pushMaskerScope() = 0;

    /**
     * @brief Discard the newest data masking context.
     */
    virtual void popMaskerScope() = 0;

    /**
     * @brief Helper class encapsulating the use of `pushTraceOptionsScope and
     *        `popTraceOptionsScope`.
     */
    friend class EsdlScriptTraceOptionsScope;

    /**
     * @brief Create and use a copy of the current trace aptions values.
     */
    virtual void pushTraceOptionsScope() = 0;

    /**
     * @brief Discard the newest trace options values.
     */
    virtual void popTraceOptionsScope() = 0;
};

/**
 * @brief Create an instance of IEsdlScriptContext.
 *
 * The absence of a sectional document model in the parameter list implies the returned instance
 * is responsible for creating its own model.
 */
extern "C" esdlscript_decl IEsdlScriptContext* createEsdlScriptContext(IEspContext* espCtx, IEsdlFunctionRegister* functionRegister, IDataMaskingEngine* engine);

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
#define ESDLScriptEntryPoint_ScriptedService "Service"
#define ESDLScriptEntryPoint_InitialEsdlResponse "EsdlResponse"
#define ESDLScriptEntryPoint_PreLogging "PreLogging"
#define ESDLScriptEntryPoint_Functions "Functions"

interface IEsdlTransformOperation;

interface IEsdlFunctionRegister : extends IInterface
{
    virtual void registerEsdlFunction(const char *name, IEsdlTransformOperation *esdlFunc) = 0;
    virtual void registerEsdlFunctionCall(IEsdlTransformOperation *esdlFunc) = 0;
    virtual IEsdlTransformOperation *findEsdlFunction(const char *name, bool localOnly) = 0;
};

interface IEsdlTransformOperation : public IInterface
{
    virtual bool process(IEsdlScriptContext * scriptContext, IXpathContext * targetContext, IXpathContext * sourceContext) = 0;
    virtual IInterface *prepareForAsync(IEsdlScriptContext * scriptContext, IXpathContext * targetContext, IXpathContext * sourceContext) = 0;
    virtual bool exec(CriticalSection *crit, IInterface *preparedForAsync, IEsdlScriptContext * scriptContext, IXpathContext * targetContext, IXpathContext * sourceContext) = 0;
    virtual void toDBGLog() = 0;
};

interface IEsdlTransformEntryPointMap : extends IInterface
{
    virtual IEsdlTransformSet *queryEntryPoint(const char *name) = 0;
    virtual void removeEntryPoint(const char *name) = 0;
    virtual IEsdlFunctionRegister *queryFunctionRegister() = 0;
};

interface IEsdlTransformMethodMap : extends IInterface
{
    virtual IEsdlTransformEntryPointMap *queryMethod(const char *method) = 0;
    virtual IEsdlFunctionRegister *queryFunctionRegister(const char *method) = 0;
    virtual IEsdlTransformSet *queryMethodEntryPoint(const char *method, const char *name) = 0;
    virtual void removeMethod(const char *method) = 0;
    virtual void addMethodTransforms(const char *method, const char *script, bool &foundNonLegacyTransforms) = 0;
    virtual void bindFunctionCalls() = 0;
};

esdlscript_decl IEsdlTransformMethodMap *createEsdlTransformMethodMap();

esdlscript_decl IEsdlCustomTransform *createEsdlCustomTransform(const char *scriptXml, const char *ns_prefix);

esdlscript_decl void processServiceAndMethodTransforms(IEsdlScriptContext * scriptCtx, std::initializer_list<IEsdlTransformSet *> const &transforms, const char *srcSection, const char *tgtSection);
esdlscript_decl void registerEsdlXPathExtensions(IXpathContext *xpathCtx, IEsdlScriptContext *scriptCtx, const StringArray &prefixes);

#endif /* ESDL_SCRIPT_HPP_ */
