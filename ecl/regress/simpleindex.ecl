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

DG_OutRec := RECORD
      unsigned4  DG_ParentID;
      string10  DG_firstname;
      string10  DG_lastname; 
      unsigned1 DG_Prange;   
END;

DG_FlatFile      := DATASET('FLAT',{DG_OutRec/*,UNSIGNED8 filepos{virtual(fileposition)}*/},FLAT);

DG_indexFile     := INDEX(
    DG_FlatFile,
    RECORD
        DG_firstname;
    END,
    RECORD
        STRING DG_LASTName := TRIM(DG_lastname);
        unsigned8 crap;
    END,
    'INDEX');

output(DG_IndexFile,{DG_firstname, DG_lastname});

