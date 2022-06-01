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

#include "jliball.hpp"
#include "platform.h"
#include "jlib.hpp"
#include "jio.hpp"
#include "jmutex.hpp"
#include "jfile.hpp"
#include "jsocket.hpp"
#include "jdebug.hpp"
#include "mptag.hpp"

#include "daftcfg.hpp"
#include "fterror.hpp"
#include "rmtspawn.hpp"
#include "rmtfile.hpp"
#include "ftslavelib.hpp"

class FtSlave : public CRemoteSlave
{
public:
    FtSlave() : CRemoteSlave("ftslave", MPTAG_FT_SLAVE, DAFT_VERSION, false) {}

    virtual bool processCommand(byte action, ISocket * masterSocket, MemoryBuffer & msg, MemoryBuffer & results)
    {
	    return processFtCommand(action, masterSocket, msg, results);
    }
};

static constexpr const char * defaultYaml = R"!!(
version: "1.0"
ftslave:
  name: ftslave
  logging:
    detail: 50
)!!";

int main(int argc, const char * * argv)
{
    InitModuleObjects();
    setDaliServixSocketCaching(true);
    installDefaultFileHooks(nullptr);
    try
    {
        loadConfiguration(defaultYaml, argv, "ftslave", "FTSLAVE", nullptr, nullptr);
    }
    catch (IException * e)
    {
        OERRLOG(e);
        e->Release();
        return 1;
    }
    catch(...)
    {
        OERRLOG("Failed to load configuration");
        return 1;
    }
    FtSlave slave;
    slave.run(argc, argv);
    return 0;
}
