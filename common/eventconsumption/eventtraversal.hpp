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

#include "eventvisitor.h"

// Coordinates the visitors used by an event-consuming operation across its
// traversal phases. A handler may require a preparatory pass to assemble state
// before the terminal visitor consumes the main event stream. Output exporters
// are one use of this interface, but the interface is also suitable for other
// operations with custom multi-phase traversal.
interface IEventTraversalHandler : extends IInterface
{
    // Return the terminal visitor for the main event traversal. If preScanRequired() is true,
    // the pre-scan visitor must finish receiving the input before this visitor is used.
    virtual IEventVisitor& queryTerminalVisitor() = 0;

    // Return true when a pre-scan is required. Callers must query and use queryPreScanVisitor()
    // only when this returns true.
    virtual bool preScanRequired() const = 0;

    // Return the visitor for the preparatory traversal. The caller must complete this traversal
    // before using queryTerminalVisitor(), and must not call this method when preScanRequired()
    // returns false.
    virtual IEventVisitor& queryPreScanVisitor() = 0;
};
