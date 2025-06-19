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

#include "eventmodeling.h"

extern IEventModel* createIndexEventModel(const IPropertyTree& config);

// Use the model type, `@kind`, to choose the correct model class to instantiate. The first model,
// `index-events`, is the default if the type is not given.
IEventModel* createEventModel(const IPropertyTree& config)
{
    const char* kind = config.queryProp("@kind");
    if (isEmptyString(kind)) // create default model
        return createIndexEventModel(config);
    if (strieq(kind, "index-events"))
        return createIndexEventModel(config);
    return nullptr;
}
