/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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

#ifndef RTLNEWKEY_INCL
#define RTLNEWKEY_INCL
#include "eclrtl.hpp"

#include "rtlkey.hpp"

BITMASK_ENUM(TransitionMask);

/*
 * The RowFilter class represents a multiple-field filter of a row.
 */
class RowFilter
{
public:
    void addFilter(IFieldFilter & filter);
    bool matches(const RtlRow & row) const;

    int compareRows(const RtlRow & left, const RtlRow & right) const;
    unsigned numFilterFields() const { return filters.ordinality(); }
    const IFieldFilter & queryFilter(unsigned i) const { return filters.item(i); }

protected:
    IArrayOf<IFieldFilter> filters; // for an index must be in field order, and all values present - more thought required
};



#endif
