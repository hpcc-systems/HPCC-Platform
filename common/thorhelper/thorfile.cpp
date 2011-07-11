/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
    if (expireDays)
    {
        CDateTime expireTime;
        StringBuffer expireTimeText;
        expireTime.setNow();
        expireTime.adjustTime(expireDays * 24 * 60);
        expireTime.getString(expireTimeText, false);
        properties.setProp("@expires", expireTimeText.str());
    }
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
