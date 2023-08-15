#include "spanexporterfactory.hpp"
#include "opentelemetry/sdk_config.h"
#include <iostream>
//#include "opentelemetry/sdk/trace/span_exporter.h"

namespace nostd     = opentelemetry::nostd;
namespace trace_sdk = opentelemetry::sdk::trace;
namespace trace_api = opentelemetry::trace;
namespace sdkcommon = opentelemetry::sdk::common;

class JLogSpanExporter : public trace_sdk::SpanExporter
{
public:
  JLogSpanExporter() {}

  virtual ~JLogSpanExporter() {}

  virtual std::unique_ptr<opentelemetry::sdk::trace::Recordable> MakeRecordable() noexcept override
  {
      return std::unique_ptr<opentelemetry::sdk::trace::Recordable>(new opentelemetry::sdk::trace::SpanData);
  }

    virtual sdkcommon::ExportResult Export(const nostd::span<std::unique_ptr<trace_sdk::Recordable>> &spans) noexcept override
    //virtual opentelemetry::sdk::common::ExportResult Export(const opentelemetry::sdk::trace::Span<std::unique_ptr<opentelemetry::sdk::trace::Recordable>>&spans) noexcept override
    {
        if (isShutdown())
        {
            // OTEL_INTERNAL_LOG_ERROR("[Ostream Trace Exporter] Exporting "
            //   << spans.size() << " span(s) failed, exporter is shutdown");

            return sdkcommon::ExportResult::kFailure;
        }

        for (auto &recordable : spans)
        {
            auto span = std::unique_ptr<trace_sdk::SpanData>(
            static_cast<trace_sdk::SpanData *>(recordable.release()));
/*
            if (span != nullptr)
            {
                char trace_id[32]       = {0};
                char span_id[16]        = {0};
                char parent_span_id[16] = {0};

                span->GetTraceId().ToLowerBase16(trace_id);
                span->GetSpanId().ToLowerBase16(span_id);
                span->GetParentSpanId().ToLowerBase16(parent_span_id);

                sout_ << "{"
                      << "\n  name          : " << span->GetName()
                      << "\n  trace_id      : " << std::string(trace_id, 32)
                      << "\n  span_id       : " << std::string(span_id, 16)
                      << "\n  tracestate    : " << span->GetSpanContext().trace_state()->ToHeader()
                      << "\n  parent_span_id: " << std::string(parent_span_id, 16)
                      << "\n  start         : " << span->GetStartTime().time_since_epoch().count()
                      << "\n  duration      : " << span->GetDuration().count()
                      << "\n  description   : " << span->GetDescription()
                      << "\n  span kind     : " << span->GetSpanKind()
                      << "\n  status        : " << statusMap[int(span->GetStatus())]
                      << "\n  attributes    : ";
                printAttributes(span->GetAttributes());
                sout_ << "\n  events        : ";
                printEvents(span->GetEvents());
                sout_ << "\n  links         : ";
                printLinks(span->GetLinks());
                sout_ << "\n  resources     : ";
                printResources(span->GetResource());
                sout_ << "\n  instr-lib     : ";
                printInstrumentationScope(span->GetInstrumentationScope());
                sout_ << "\n}\n";
            }
*/
        }

        return sdkcommon::ExportResult::kSuccess;
    }

    bool isShutdown() const noexcept
    {
        //const std::lock_guard<opentelemetry::common::SpinLockMutex> locked(lock_);
        //return is_shutdown_;
        return false;
    }

    virtual bool Shutdown(std::chrono::microseconds timeout = std::chrono::microseconds::max()) noexcept
    {
        return true;
    }
/*
  virtual sdkcommon::ExportResult Export(const std::vector<std::unique_ptr<trace_sdk::Recordable>> &spans) noexcept override
  {
    // TODO: Implement this method to export the spans to your backend
    return sdkcommon::ExportResult::kSuccess;
  }

  virtual std::unique_ptr<SpanExporter> Clone() const noexcept override
  {
    // TODO: Implement this method to create a new instance of the exporter
    return nullptr;
  }*/
};

std::unique_ptr<trace_sdk::SpanExporter> JLogSpanExporterFactory::Create()
{
    std::unique_ptr<trace_sdk::SpanExporter> exporter(new JLogSpanExporter());
    return exporter;
}
