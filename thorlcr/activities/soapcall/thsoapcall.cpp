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

#include "thexception.hpp"
#include "thsoapcall.ipp"
#include "dasess.hpp"

static CBuildVersion _bv("$HeadURL: https://svn.br.seisint.com/ecl/trunk/thorlcr/activities/soapcall/thsoapcall.cpp $ $Id: thsoapcall.cpp 62376 2011-02-04 21:59:58Z sort $");


class SoapCallActivityMaster : public CMasterActivity
{
private:
    StringBuffer authToken;

public:
    SoapCallActivityMaster(CMasterGraphElement * info) : CMasterActivity(info)
    {
    }

    virtual void init()
    {
        // Build authentication token
        StringBuffer uidpair;
        IUserDescriptor *userDesc = container.queryJob().queryUserDescriptor();
        userDesc->getUserName(uidpair);
        uidpair.append(":");
        userDesc->getPassword(uidpair);
        JBASE64_Encode(uidpair.str(), uidpair.length(), authToken);
    }

    void serializeSlaveData(MemoryBuffer &dst, unsigned slave)
    {
        dst.append(authToken.str());
    }
};


CActivityBase *createSoapCallActivityMaster(CMasterGraphElement *container)
{
    return new SoapCallActivityMaster(container);
}
