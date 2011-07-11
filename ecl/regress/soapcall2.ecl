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

#option ('globalFold', false);

xyzServiceOutRecord := 
    RECORD
        unsigned6 id;
        unsigned8 score;
    END;

callXyzService(_NAME, _ZIPCODES, _NUMRESULTS, OUTF) := macro
        outF := SOAPCALL('myip','XyzService',
                        {
                            string Name{xpath('Name')} := _NAME, 
                            set of string zips{xpath('Zips/Item')} := _ZIPCODES,
                            unsigned numResults := _NUMRESULTS
                        }, DATASET(xyzServiceOutRecord), LOG, trim)
    endmacro;



string searchName := ''         : stored('searchName');
set of string inSet := ['123','234','345'];
person := dataset('person', { unsigned8 person_id, string1 per_sex, string2 per_st, string40 per_first_name, string40 per_last_name}, thor);

callXyzService(searchName, inSet, count(person), results);

output(results);
