# ESDL Functions Overview

This document provides an overview of Enterprise Service Description Language (ESDL) functions available in the HPCC Systems platform. 

## Content Masking Functions

[canMaskContent](../esp/esdllib/docs/canMaskContent.md)  
API documentation for the canMaskContent function, which determines whether content can be masked based on current context.

[getMaskingPropertyAwareness](../esp/esdllib/docs/getMaskingPropertyAwareness.md)  
API documentation for the getMaskingPropertyAwareness function, which retrieves masking property awareness settings.

[getMaskValueBehavior](../esp/esdllib/docs/getMaskValueBehavior.md)  
API documentation for the getMaskValueBehavior function, which returns the current mask value behavior configuration.

[maskContent](../esp/esdllib/docs/maskContent.md)  
API documentation for the maskContent function, which applies masking to specified content.

[masking context scope](../esp/esdllib/docs/masking-context-scope.md)  
Documentation for masking context scope functionality in ESDL.

[maskValue](../esp/esdllib/docs/maskValue.md)  
API documentation for the maskValue function, which masks individual values according to defined policies.

[updateMaskingContext](../esp/esdllib/docs/update-masking-context.md)  
API documentation for the updateMaskingContext function, which modifies the current masking context.

## String Processing Functions

[compressString](../esp/esdllib/docs/compressString.md)  
API documentation for the compressString function, which compresses string data.

[decompressString](../esp/esdllib/docs/decompressString.md)  
API documentation for the decompressString function, which decompresses previously compressed string data.

[decodeBase64String](../esp/esdllib/docs/decodeBase64String.md)  
API documentation for the decodeBase64String function, which decodes Base64-encoded strings.

[encodeBase64String](../esp/esdllib/docs/encodeBase64String.md)  
API documentation for the encodeBase64String function, which encodes strings using Base64 encoding.

[escapeXmlCharacters](../esp/esdllib/docs/escapeXmlCharacters.md)  
API documentation for the escapeXmlCharacters function, which escapes special XML characters in strings.

[unescapeXmlCharacters](../esp/esdllib/docs/unescapeXmlCharacters.md)  
API documentation for the unescapeXmlCharacters function, which unescapes XML character entities.

## XML Processing Functions

[toXmlString](../esp/esdllib/docs/toXmlString.md)  
API documentation for the toXmlString function, which converts data structures to XML string format.

## Tracing and Debugging Functions

[isTraceEnabled](../esp/esdllib/docs/isTraceEnabled.md)  
API documentation for the isTraceEnabled function, which checks whether tracing is currently enabled.

[setTraceOptions](../esp/esdllib/docs/set-trace-options.md)  
API documentation for the setTraceOptions function, which configures tracing options.

[trace](../esp/esdllib/docs/trace.md)  
API documentation for the trace function, which outputs trace information.

[traceContent](../esp/esdllib/docs/trace-content.md)  
API documentation for the traceContent function, which traces content with optional formatting.

[trace options scope](../esp/esdllib/docs/trace-options-scope.md)  
Documentation for trace options scope functionality in ESDL.

[traceValue](../esp/esdllib/docs/trace-value.md)  
API documentation for the traceValue function, which traces individual values.

## Transaction Summary Functions

[getTxSummary](../esp/esdllib/docs/getTxSummary.md)  
API documentation for the getTxSummary function, which retrieves transaction summary information.

[txSummaryTimer](../esp/esdllib/docs/tx-summary-timer.md)  
API documentation for the txSummaryTimer function, which manages timing information in transaction summaries.

[txSummaryValue](../esp/esdllib/docs/tx-summary-value.md)  
API documentation for the txSummaryValue function, which manages values in transaction summaries.

## Utility Functions

[delay](../esp/esdllib/docs/delay.md)  
API documentation for the delay function, which introduces processing delays.

[operations](../esp/esdllib/docs/operations.md)  
Overview of available ESDL operations and their usage patterns.

## Deprecated Functions

[deprecated decrypt string](../esp/esdllib/docs/deprecatedDecryptString.md)  
Documentation for the deprecated decrypt string API. This function should not be used in new implementations.

[deprecated encrypt string](../esp/esdllib/docs/deprecatedEncryptString.md)  
Documentation for the deprecated encrypt string API. This function should not be used in new implementations.

## Additional Resources

For comprehensive information about ESDL operations and implementation patterns, refer to the [operations](../esp/esdllib/docs/operations.md) documentation.

