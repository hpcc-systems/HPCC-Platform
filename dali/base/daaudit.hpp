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

#ifndef DAAUDIT_HPP
#define DAAUDIT_HPP

#ifndef da_decl
#define da_decl __declspec(dllimport)
#endif

class CDateTime;
extern da_decl unsigned queryAuditLogs(const CDateTime &from,const CDateTime &to, const char *mask,StringAttrArray &out,unsigned start=0,unsigned max=100000);


// for server use
interface IDaliServer;
extern da_decl IDaliServer *createDaliAuditServer(const char *dir); // called for coven members

extern da_decl LogMsgCategory const daliAuditLogCat;



#endif
