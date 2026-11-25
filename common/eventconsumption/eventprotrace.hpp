/*##############################################################################

    Copyright (C) 2026 HPCC Systems®.

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

#pragma once

#include "eventtraversal.hpp"

class CMetaInfoState;
interface IBufferedSerialOutputStream;

interface IProtraceEventExporter : extends IEventTraversalHandler
{
    // Visit all input events with queryPreScanVisitor() before using queryTerminalVisitor()
    // so metadata and string-table entries are available.
};

extern event_decl IProtraceEventExporter *createProtraceEventExporter(IBufferedSerialOutputStream& out);
extern event_decl IProtraceEventExporter *createProtraceEventExporter(IBufferedSerialOutputStream& out, const CMetaInfoState& metaState);
