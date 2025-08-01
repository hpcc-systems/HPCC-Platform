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

#include "platform.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#ifdef __linux__
#include <alloca.h>
#endif
#include <algorithm>

#include "jmisc.hpp"
#include "jset.hpp"
#include "hlzw.h"

#include "ctfile.hpp"
#include "jhinplace.hpp"
#include "jstats.h"
#include "jptree.hpp"
#include "jhcache.hpp"

byte * CCachedIndexRead::getBufferForUpdate(offset_t offset, size32_t writeSize)
{
    baseOffset = offset;
    size = writeSize;
    void * base = data.ensure(writeSize);
    return static_cast<byte *>(base);
}

const byte * CCachedIndexRead::queryBuffer(offset_t offset, size32_t readSize) const
{
    if ((offset < baseOffset) || (offset + readSize > baseOffset + size))
        return nullptr;
    return static_cast<const byte *>(data.get()) + (offset - baseOffset);
}

//--------------------------------------------------------------------------------------------------------------------

void initDiskNodeCache(const IPropertyTree *config)
{
}

#ifdef _USE_CPPUNIT
#include "unittests.hpp"
#include "eclrtl.hpp"


#endif
