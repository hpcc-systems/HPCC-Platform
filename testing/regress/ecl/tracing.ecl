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

//class=file
//version multiPart=true
//nokey
import lib_logging;

output(lib_logging.Logging.getSpanID());  //Fetches the spanID from the current span
                                          //Expected output is a 16 character hex string
                                          //Example: 379563f3fcd56bc9

output(lib_logging.Logging.getTraceID()); // Fetches the traceID from the current span
                                          // Expected output is a 32 character hex string
                                          // Example: 0c483675c6c887d9fb49885c0f2ba116


output(lib_logging.Logging.getTraceSpanHeader());   //Fetches the current span's trace/spand header
                                                    //to be propagated to the next service as 'traceparent' http header
                                                    //Expected output is '00-<traceID>-<spanID>-<traceflags>'
                                                    //Example: 00-0c483675c6c887d9fb49885c0f2ba116-379563f3fcd56bc9-01
output(lib_logging.Logging.getTraceStateHeader());  //Fetches the current span's trace state header
                                                    //to be propagated to the next service as 'tracestate' http header
                                                    //Expected output is 'key1=value1,key2=value2'
                                                    //TraceState carries vendor-specific trace identification data, represented as a list of key-value pairs.
                                                    // TraceState allows multiple tracing systems to participate in the same trace.


// Annotates current span with a custom attribute
lib_logging.Logging.setSpanAttribute('customAttribute','{"lines":[{"fields":[{"timestamp": "2023-11-06T20:52:19.265Z","hpcc.log.threadid": "232","hpcc.log.timestamp": "2023-11-06 20:52:19.265","kubernetes.container.name": "myeclccserver",}]},{"fields":[{"hpcc.log.jobid": "W20231111-222222","hpcc.log.audience": "OPR","hpcc.log.message": "hthor build community_9.4.9-closedown0Debug[heads/HPCC-30401-JTrace-optional-traceid-support-0-gc85263-dirty]", "kubernetes.pod.name": "hthor-job-w20231106-205213-jdcss",}]}]}');

// Can annotate current span multiple key-value pairs
lib_logging.Logging.setSpanAttribute('task','regress-ecl-tracing');
