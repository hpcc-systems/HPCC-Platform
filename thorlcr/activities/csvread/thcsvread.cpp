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

#include "dadfs.hpp"

#include "thorport.hpp"
#include "thexception.hpp"
#include "thmfilemanager.hpp"
#include "thdiskbase.ipp"
#include "thcsvread.ipp"

static CBuildVersion _bv("$HeadURL: https://svn.br.seisint.com/ecl/trunk/thorlcr/activities/csvread/thcsvread.cpp $ $Id: thcsvread.cpp 62376 2011-02-04 21:59:58Z sort $");

class CCsvReadActivityMaster : public CDiskReadMasterBase
{
public:
    CCsvReadActivityMaster(CMasterGraphElement *info) : CDiskReadMasterBase(info)
    {
    }
    void serializeSlaveData(MemoryBuffer &dst, unsigned slave)
    {
        CDiskReadMasterBase::serializeSlaveData(dst, slave);
        if (mapping && mapping->queryMapWidth(slave)>=1)
        {
            IHThorCsvReadArg *helper = (IHThorCsvReadArg *)queryHelper();
            if (fileDesc->queryProperties().hasProp("@csvQuote")) dst.append(true).append(fileDesc->queryProperties().queryProp("@csvQuote"));
            else dst.append(false);
            if (fileDesc->queryProperties().hasProp("@csvSeperate")) dst.append(true).append(fileDesc->queryProperties().queryProp("@csvSeperate"));
            else dst.append(false);
            if (fileDesc->queryProperties().hasProp("@csvTerminate")) dst.append(true).append(fileDesc->queryProperties().queryProp("@csvTerminate"));
            else dst.append(false);
        }
    }
};


CActivityBase *createCCsvReadActivityMaster(CMasterGraphElement *container)
{
    return new CCsvReadActivityMaster(container);
}

