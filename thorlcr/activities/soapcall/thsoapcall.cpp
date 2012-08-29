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

#include "thexception.hpp"
#include "thsoapcall.ipp"
#include "dasess.hpp"

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
