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

#include <time.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include "logging.hpp"
#include "jlog.hpp"

static char buildVersion[] = "$HeadURL: https://svn.br.seisint.com/ecl/trunk/plugins/logging/logging.cpp $ $Id: logging.cpp 62376 2011-02-04 21:59:58Z sort $";

#define LOGGING_VERSION "LOGGING 1.0.1"
static const char * compatibleVersions[] = {
    "LOGGING 1.0.0 [66aec3fb4911ceda247c99d6a2a5944c]", // linux version
    LOGGING_VERSION,
    NULL };

const char * EclDefinition = 
"export Logging := SERVICE\n"
"  dbglog(const string src) : c,action,entrypoint='logDbgLog'; \n"
"  addWorkunitInformation(const varstring txt, unsigned code=0, unsigned severity=0) : ctxmethod,action,entrypoint='addWuException'; \n"
"  addWorkunitWarning(const varstring txt, unsigned code=0, unsigned severity=1) : ctxmethod,action,entrypoint='addWuException'; \n"
"  addWorkunitError(const varstring txt, unsigned code=0, unsigned severity=2) : ctxmethod,action,entrypoint='addWuException'; \n"
"END;";

LOGGING_API bool getECLPluginDefinition(ECLPluginDefinitionBlock *pb) 
{
    if (pb->size == sizeof(ECLPluginDefinitionBlockEx))
    {
        ECLPluginDefinitionBlockEx * pbx = (ECLPluginDefinitionBlockEx *) pb;
        pbx->compatibleVersions = compatibleVersions;
    }
    else if (pb->size != sizeof(ECLPluginDefinitionBlock))
        return false;
    pb->magicVersion = PLUGIN_VERSION;
    pb->version = LOGGING_VERSION;
    pb->moduleName = "lib_logging";
    pb->ECL = EclDefinition;
    pb->flags = PLUGIN_IMPLICIT_MODULE;
    pb->description = "Logging library";
    return true;
}

//-------------------------------------------------------------------------------------------------------------------------------------------

LOGGING_API void LOGGING_CALL logDbgLog(unsigned srcLen, const char * src)
{
    DBGLOG("%.*s", srcLen, src);
}

