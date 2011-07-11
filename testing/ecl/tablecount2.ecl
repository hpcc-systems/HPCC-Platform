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

//UseStandardFiles
//UseIndexes

Layout_DG_Totals := RECORD
  DG_IndexFile.DG_FirstName;
  DG_IndexFile.DG_lastName;
                BOOLEAN Exists := Exists(GROUP);
                END;
                
DG_Totals := table(DG_IndexFile,Layout_DG_Totals,DG_FirstName,DG_LastName,FEW);

output(sort(DG_Totals, dg_firstname, dg_lastname));
