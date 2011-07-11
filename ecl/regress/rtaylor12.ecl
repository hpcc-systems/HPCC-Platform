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

export Layout_LookupCSZFile := RECORD
        QSTRING15 City;
        QSTRING2  State;
        UNSIGNED INTEGER3 ZipCode;
        UNSIGNED INTEGER4 my_rec_ID;
        UNSIGNED8 RecPos{virtual(fileposition)};
END;

export File_LookupCSZ := DATASET(
    'CSTEMP::OUT::LookupCSZ', Layout_LookupCSZFile, THOR
);

export IDX_LookupCSZ_st_city :=
    INDEX(File_LookupCSZ,
          { State, City, RecPos },
          'CSTEMP::KEY::LookupCSZ_st_city');

StateCSZRecs(STRING the_state) :=
    IDX_LookupCSZ_st_city(State = the_state);

qstring2 searchST := 'FL' : stored('searchSt');

output(StateCSZRecs(searchST));