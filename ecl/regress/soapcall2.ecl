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
