/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

#include "jliball.hpp"

#include "thorfile.hpp"

#include "eclhelper.hpp"
#include "eclrtl.hpp"
#include "eclrtl_imp.hpp"
#include "rtlfield_imp.hpp"
#include "rtlds_imp.hpp"

namespace thorfile {
#include "eclhelper_base.hpp"
}
#include "thorcommon.ipp"

void setExpiryTime(IPropertyTree & properties, unsigned expireDays)
{
    properties.setPropInt("@expireDays", expireDays);
}


class DiskWorkUnitReadArg : public thorfile::CThorDiskReadArg
{
public:
    DiskWorkUnitReadArg(const char * _filename, IHThorWorkunitReadArg * _wuRead) : filename(_filename), wuRead(_wuRead)
    {
        recordSize.set(wuRead->queryOutputMeta());
    }
    virtual IOutputMetaData * queryOutputMeta()
    {
        return wuRead->queryOutputMeta();
    }
    virtual const char * getFileName()
    {
        return filename;
    }
    virtual IOutputMetaData * queryDiskRecordSize()
    {
        return (IOutputMetaData *)recordSize;
    }
    virtual unsigned getFormatCrc()
    {
        return 0;
    }
    virtual unsigned getFlags()
    {
        return TDRnocrccheck;
    }
    virtual size32_t transform(ARowBuilder & rowBuilder, const void * src)
    {
        unsigned size = recordSize.getRecordSize(src);
        memcpy(rowBuilder.ensureCapacity(size, NULL), src, size);
        return size;
    }

protected:
    StringAttr filename;
    Linked<IHThorWorkunitReadArg> wuRead;
    CachedOutputMetaData recordSize;
};




IHThorDiskReadArg * createWorkUnitReadArg(const char * filename, IHThorWorkunitReadArg * wuRead)
{
    return new DiskWorkUnitReadArg(filename, wuRead);
}
