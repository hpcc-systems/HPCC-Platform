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

#include "jlog.hpp"
#include "auditlib.hpp"

#define AUDITLIB_VERSION "AUDITLIB 1.0.1"
static const char * compatibleVersions[] = {
    "AUDITLIB 1.0.0 [29933bc38c1f07bcf70f938ad18775c1]", // linux version
    AUDITLIB_VERSION,
    NULL };

const char * EclDefinition = 
"export AuditLib := SERVICE\n"
"  boolean Audit(const string atype, const string msg) : c, action, volatile, entrypoint='alAudit', hole; \n"
"  boolean AuditData(const string atype, const string msg, const data datablock) : c, action, volatile, entrypoint='alAuditData', hole; \n"
"END;";


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
    pb->ECL = EclDefinition;
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
