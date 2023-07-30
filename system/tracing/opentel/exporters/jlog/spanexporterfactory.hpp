#pragma once

#include <iostream>
#include <memory>

#include "opentelemetry/sdk/version/version.h"
#include "opentelemetry/sdk/trace/exporter.h"

//OPENTELEMETRY_BEGIN_NAMESPACE
//namespace sdk
//{
//namespace trace
//{
//class SpanExporter;
//}  // namespace trace
//}  // namespace sdk

//namespace exporter
//{
//namespace trace
//{

class JLogSpanExporterFactory
{
public:
  /**
   * Creates a JLogSpanExporterFactory writing to the default location.
   */
  static std::unique_ptr<opentelemetry::sdk::trace::SpanExporter> Create();

  //static std::unique_ptr<opentelemetry::sdk::trace::SpanExporter> Create(std::ostream &sout);
};

//}  // namespace trace
//}  // namespace exporter
//OPENTELEMETRY_END_NAMESPACE
