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