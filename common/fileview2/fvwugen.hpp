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

#ifndef FVWUGEN_HPP
#define FVWUGEN_HPP

#ifdef _WIN32
    #ifdef FILEVIEW2_EXPORTS
        #define FILEVIEW_API __declspec(dllexport)
    #else
        #define FILEVIEW_API __declspec(dllimport)
    #endif
#else
    #define FILEVIEW_API
#endif

#define LOWER_LIMIT_ID      "__startPos__"
#define RECORD_LIMIT_ID     "__numRecords__"

extern FILEVIEW_API IHqlExpression * buildWorkUnitViewerEcl(IHqlExpression * record, const char * wuid, unsigned sequence, const char * name);
extern FILEVIEW_API IHqlExpression * buildDiskOutputEcl(const char * logicalName, IHqlExpression * record);
extern FILEVIEW_API IHqlExpression * buildDiskFileViewerEcl(const char * logicalName, IHqlExpression * record);
extern FILEVIEW_API IHqlExpression * buildQueryViewerEcl(IHqlExpression * selectFields);

#endif
