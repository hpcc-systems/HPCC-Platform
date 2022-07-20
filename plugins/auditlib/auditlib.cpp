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

#include "jlog.hpp"
#include "auditlib.hpp"

#define AUDITLIB_VERSION "AUDITLIB 1.0.1"
static const char * compatibleVersions[] = {
    "AUDITLIB 1.0.0 [29933bc38c1f07bcf70f938ad18775c1]", // linux version
    AUDITLIB_VERSION,
    NULL };

AUDITLIB_API bool getECLPluginDefinition(ECLPluginDefinitionBlock *pb) 
{
    if (pb->size == sizeof(ECLPluginDefinitionBlockEx))
    {
        ECLPluginDefinitionBlockEx * pbx = (ECLPluginDefinitionBlockEx *) pb;
        pbx->compatibleVersions = compatibleVersions;
    }
    else if (pb->size != sizeof(ECLPluginDefinitionBlock))
        return false;
    pb->magicVersion = PLUGIN_VERSION;
    pb->version = AUDITLIB_VERSION;
    pb->moduleName = "lib_auditlib";
    pb->ECL = NULL;
    pb->flags = PLUGIN_IMPLICIT_MODULE;
    pb->description = "AuditLib event log audit functions";
    return true;
}

#define AUDIT_TYPES_BEGIN char const * auditTypeNameMap[NUM_AUDIT_TYPES+1] = {
#define MAKE_AUDIT_TYPE(name, type, categoryid, eventid, level) #name ,
#define AUDIT_TYPES_END 0 };
#include "jelogtype.hpp"
#undef AUDIT_TYPES_BEGIN
#undef MAKE_AUDIT_TYPE
#undef AUDIT_TYPES_END

AuditType findAuditType(char const * typeString)
{
    unsigned i;
    for(i=0; i<NUM_AUDIT_TYPES; i++)
        if(strcmp(typeString, auditTypeNameMap[i])==0) return static_cast<AuditType>(i);
    return NUM_AUDIT_TYPES;
}

bool alAudit(unsigned typeLen, char const * type, unsigned msgLen, char const * msg)
{
    return alAuditData(typeLen, type, msgLen, msg, 0, 0);
}

bool alAuditData(unsigned typeLen, char const * type, unsigned msgLen, char const * msg, unsigned dataLen, void const * dataBlock)
{
    StringBuffer typeString(typeLen, type);
    typeString.toUpperCase();
    StringBuffer msgString(msgLen, msg);
    AuditType typeValue = findAuditType(typeString.str());
    if(typeValue >= NUM_AUDIT_TYPES)
        return false;
    return AUDIT(typeValue, msgString.str(), dataLen, dataBlock);
}
