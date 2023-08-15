/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2023 HPCC Systems®.

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

#include "platform.h"

#include "jmisc.hpp"
#include "jtrace.hpp"
#include "lnuid.h"

#include "opentelemetry/trace/semantic_conventions.h" //known span defines
#include "opentelemetry/context/propagation/global_propagator.h" // context::propagation::GlobalTextMapPropagator::GetGlobalPropagator
#include "opentelemetry/sdk/trace/tracer_provider_factory.h" //opentelemetry::sdk::trace::TracerProviderFactory::Create(context)
#include "opentelemetry/sdk/trace/tracer_context_factory.h" //opentelemetry::sdk::trace::TracerContextFactory::Create(std::move(processors));
#include "opentelemetry/sdk/trace/simple_processor_factory.h"
#include "opentelemetry/exporters/ostream/span_exporter_factory.h"// auto exporter = opentelemetry::exporter::trace::OStreamSpanExporterFactory::Create();

#include "opentelemetry/trace/propagation/http_trace_context.h" //opentel_trace::propagation::kTraceParent

using namespace ln_uid;

/*
* Sets global id if provided, and assign a localId
*/
void LogTrace::setGlobalId(const char* id)
{
    if (!isEmptyString(id))
    {
        globalId.set(id);
        assignLocalId();
    }
}

/*
* Sets global id if provided, assigns new localID
*/
LogTrace::LogTrace(const char * globalId)
{
    setGlobalId(globalId);
}

LogTrace::LogTrace()
{
    assignLocalId();
}

const char* LogTrace::assignLocalId()
{
    localId.set(createUniqueIdString().c_str());
    return localId.get();
}

const char* LogTrace::queryGlobalId() const
{
    return globalId.get();
}

void LogTrace::setCallerId(const char* id)
{
    callerId.set(id);
}

const char* LogTrace::queryCallerId() const
{
    return callerId.get();
}

const char* LogTrace::queryLocalId() const
{
    return localId.get();
}

class CHPCCHttpTextMapCarrier : public opentelemetry::context::propagation::TextMapCarrier
{
public:
    CHPCCHttpTextMapCarrier(const IProperties * httpHeaders)
    {
        if (httpHeaders)
        {
            this->httpHeaders.setown(createProperties());
            Owned<IPropertyIterator> iter = httpHeaders->getIterator();
            ForEach(*iter)
            {
                const char * key = iter->getPropKey();
                const char * val = httpHeaders->queryProp(key);
                this->httpHeaders->setProp(key, val);
            }
        }
    }

    CHPCCHttpTextMapCarrier()
    {
        httpHeaders.setown(createProperties());
    };

    virtual opentelemetry::nostd::string_view Get(opentelemetry::nostd::string_view key) const noexcept override
    {
        std::string theKey = key.data();

        if (theKey.empty())
            return "";

        Owned<IPropertyIterator> iter = httpHeaders->getIterator();
        ForEach(*iter)
        {
            const char * propKey = iter->getPropKey();
            if (stricmp(propKey, theKey.c_str()) == 0)
                return httpHeaders->queryProp(propKey);
        }

        return "";
    }

    virtual void Set(opentelemetry::nostd::string_view key, opentelemetry::nostd::string_view value) noexcept override
    {
        httpHeaders->setProp(std::string(key).c_str(), std::string(value).c_str());        
    }

private:
    Owned<IProperties> httpHeaders;
};

class HPCCStringArrayHttpTextMapCarrier : public opentelemetry::context::propagation::TextMapCarrier
{
public:
    HPCCStringArrayHttpTextMapCarrier(StringArray & headers)
    {
        httpHeaders.appendArray(headers);
    }

    HPCCStringArrayHttpTextMapCarrier() = default;

    virtual opentelemetry::nostd::string_view Get(opentelemetry::nostd::string_view key) const noexcept override
    {
        std::string theKey = key.data();
        if (theKey.empty())
            return "";

        std::string headerval;

        ForEachItemIn(x, httpHeaders)
        {
            const char* header = httpHeaders.item(x);
            if(header == nullptr)
                continue;

            const char* colon = strchr(header, ':');
            if(colon == nullptr)
                continue;

            unsigned len = colon - header;
            if((theKey.length() == len) && (strnicmp(theKey.c_str(), header, len) == 0))
            {
                headerval.append(colon + 2);
                break;
            }
        }
        return headerval;
    }

    virtual void Set(opentelemetry::nostd::string_view key, opentelemetry::nostd::string_view value) noexcept override
    {
        if(key.empty())
            return;

        StringBuffer kv;
        kv.append(std::string(key).c_str()).append(": ").append(std::string(value).c_str());

        httpHeaders.append(kv.str());
    }

private:
    StringArray httpHeaders;
};

void CTraceManager::initTracer()
{
    //Handle HPCC specific tracing configuration here
    //Target exporter? exporter connection info?
    //Target processor(s)? batch/interactive? prob not
    //Target propogator? http? grpc? binary? custom?
    //HPCC component tracing switches?
    Owned<const IPropertyTree> traceConfig;
    try
    {
        traceConfig.setown(getComponentConfigSP()->getPropTree("tracing"));
        if (traceConfig)
        {
            Owned<IPropertyTree> exportConfig = traceConfig->getPropTree("exporter");
            if (exportConfig)
            {
                if (exportConfig->getPropBool("OS", false)) //To stdout/err
                    DBGLOG("Tracing to stdout/err");
                else if (exportConfig->getPropBool("OTLP", false))
                    DBGLOG("Tracing to OTLP");
                else if (exportConfig->getPropBool("Prometheus", false))
                    DBGLOG("Tracing to Prometheus");
                else if (exportConfig->getPropBool("HPCC", false)) 
                    DBGLOG("Tracing to HPCC JLog");
            }
        }
        else
        {
            using namespace opentelemetry::trace;
            //OStream exporter, useful for development and debugging tasks and simplest to set up.
            auto exporter = opentelemetry::exporter::trace::OStreamSpanExporterFactory::Create();

            //SimpleSpanProcesser sends spans one by one to an exporter.
            //We could use a batchspanprocessor, which will group several spans together, before sending them to an exporter.
            auto processor = opentelemetry::sdk::trace::SimpleSpanProcessorFactory::Create(std::move(exporter));
            std::vector<std::unique_ptr<opentelemetry::sdk::trace::SpanProcessor>> processors;
            processors.push_back(std::move(processor));

            // Default is an always-on sampler.
            std::shared_ptr<opentelemetry::sdk::trace::TracerContext> context =
                opentelemetry::sdk::trace::TracerContextFactory::Create(std::move(processors));
            std::shared_ptr<opentelemetry::trace::TracerProvider> provider =
                opentelemetry::sdk::trace::TracerProviderFactory::Create(context);

            // Set the global trace provider
            opentelemetry::trace::Provider::SetTracerProvider(provider);

            // set global propagator
            // Injects Context into and extracts it from carriers that travel in-band
            // across process boundaries. Encoding is expected to conform to the HTTP
            // Header Field semantics.
            // Values are often encoded as RPC/HTTP request headers.
            opentelemetry::context::propagation::GlobalTextMapPropagator::SetGlobalPropagator(
                opentelemetry::nostd::shared_ptr<opentelemetry::context::propagation::TextMapPropagator>(
                    new opentelemetry::trace::propagation::HttpTraceContext()));
        }
    }
    catch (IException * e)
    {
        EXCLOG(e);
        e->Release();
    }
}

void CTraceManager::cleanupTracer()
{
    std::shared_ptr<opentelemetry::trace::TracerProvider> none;
    opentelemetry::trace::Provider::SetTracerProvider(none);
}

void CSpan::setAttributes(const IProperties * attributes)
{
    Owned<IPropertyIterator> iter = attributes->getIterator();
    ForEach(*iter)
    {
        const char * key = iter->getPropKey();
        const char * val = attributes->queryProp(key);
        span->SetAttribute(key, val);
    }
}

void CTransactionSpan::setAttriburesFromHTTPHeaders(StringArray & httpHeaders)
{
    ForEachItemIn(currentHeaderIndex, httpHeaders)
    {
        const char* httHeader = httpHeaders.item(currentHeaderIndex);
        if(!httHeader)
            continue;

        if(stricmp(httHeader, HPCCSemanticConventions::kGLOBALIDHTTPHeader) == 0)
        {
            hpccGlobalId.set(httpHeaders.item(currentHeaderIndex+1));
            continue;
        }

        if(stricmp(httHeader, HPCCSemanticConventions::kCallerIdHTTPHeader) == 0)
        {
            hpccCallerId.set(httpHeaders.item(currentHeaderIndex+1));
            continue;
        }
    }
}

void CTransactionSpan::setAttriburesFromHTTPHeaders(const IProperties * httpHeaders)
{
    if (httpHeaders)
    {
        // perform any key mapping needed...
        //Instrumented http client/server Capitalizes the first letter of the header name
        //if (key == opentel_trace::propagation::kTraceParent || key == opentel_trace::propagation::kTraceState )
        //    theKey[0] = toupper(theKey[0]);
  
        hpccGlobalId.set(httpHeaders->queryProp(HPCCSemanticConventions::kGLOBALIDHTTPHeader));
        hpccCallerId.set(httpHeaders->queryProp(HPCCSemanticConventions::kCallerIdHTTPHeader));

        const CHPCCHttpTextMapCarrier carrier(httpHeaders);
        auto globalPropegator = context::propagation::GlobalTextMapPropagator::GetGlobalPropagator();
        auto currentContext = context::RuntimeContext::GetCurrent();
        auto newContext = globalPropegator->Extract(carrier, currentContext);
        parentContext = opentelemetry::trace::GetSpan(newContext)->GetContext();
        options.parent = parentContext;
    }
}

CClientSpan::CClientSpan(const char * spanName, const char * tracerName_, const IProperties * spanAttributes)
: CSpan(opentelemetry::trace::SpanKind::kClient, spanName, tracerName_, spanAttributes)
{
}

CTransactionSpan::CTransactionSpan(const char * spanName, const char * tracerName_, StringArray & httpHeaders, const IProperties * spanAttributes)
: CSpan(opentelemetry::trace::SpanKind::kServer, spanName, tracerName_, spanAttributes)
{
    setAttriburesFromHTTPHeaders(httpHeaders);
}

CTransactionSpan::CTransactionSpan(const char * spanName, const char * tracerName_, const IProperties * httpHeaders, const IProperties * spanAttributes)
: CSpan(opentelemetry::trace::SpanKind::kServer, spanName, tracerName_, spanAttributes)
{
    setAttriburesFromHTTPHeaders(httpHeaders);
}

CSpan::CSpan(opentelemetry::trace::SpanKind spanKind, const char * spanName, const char * tracerName_, const IProperties * spanAttributes)
{
    tracerName.set(tracerName_);
    auto provider = opentelemetry::trace::Provider::GetTracerProvider();
    auto tracer = provider->GetTracer(tracerName.get());
    options.kind = spanKind;
    name.set(spanName);
    span = tracer->StartSpan(spanName, {}, options); 

    setAttributes(spanAttributes);
}

void CSpan::activate()
{
    auto provider = opentelemetry::trace::Provider::GetTracerProvider();
    auto tracer = provider->GetTracer(tracerName.get());
    auto scope = tracer->WithActiveSpan(span); //do we need to track scope? right now not member
}

static Singleton<CTraceManager> theTraceManager;
MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    return true;
}

MODULE_EXIT()
{
    theTraceManager.destroy();
}

CTraceManager * queryTraceManager()
{
    return theTraceManager.query([] { return new CTraceManager; });
}
