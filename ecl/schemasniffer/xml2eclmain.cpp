/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2026 HPCC Systems®.

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

#include "xml2ecl.hpp"
#include "jlib.hpp"
#include "jexcept.hpp"
#include <iostream>

int main(int argc, const char* argv[])
{
    InitModuleObjects();
    
    try
    {
        auto args = xml2ecl_cli::parseCommandLine(argc, argv);
        return xml2ecl_cli::processFiles(args.files, args.stringType, args.readFromStdin, args.fullScan);
    }
    catch (IException* e)
    {
        StringBuffer msg;
        e->errorMessage(msg);
        std::cerr << "Error: " << msg.str() << std::endl;
        e->Release();
        return 1;
    }
    catch (std::exception& e)
    {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}
