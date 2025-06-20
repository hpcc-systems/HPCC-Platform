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

#pragma once

#include "eventconsumption.h"
#include "eventvisitor.h"
#include "jptree.hpp"

// Abstract extension of IEventVisitationLink that distinguishes between model and any other type
// of visitation link. This may also be a placeholder for future model-specific methods should
// they become necessary.
interface IEventModel : extends IEventVisitationLink
{
};

// Create an event model from a configuration. A single element named `model` is expected as
// either the root or a child of the root element.
//
// Within `model`, the `kind` attribute is used to determine the type of model to create. Accepted
// values are:
// - `index-events` (default): An index event model.
extern event_decl IEventModel* createEventModel(const IPropertyTree& config);
