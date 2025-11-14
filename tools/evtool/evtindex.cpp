/*##############################################################################

    Copyright (C) 2025 HPCC Systems®.

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

#include "evtindex.hpp"

static constexpr const char* brief = "group of commands for analyzing index file events";
static constexpr const char* verbose = "Commands focused on the analysis of index file events.";

IEvToolCommand* createIndexCommand()
{
    return new CEvtCommandGroup({
        { "summarize", createIndexSummaryCommand },
        { "hotspot", createIndexHotspotCommand },
        { "plot", createIndexPlotCommand },
    }, verbose, brief);
}
