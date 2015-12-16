/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the 'License');
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an 'AS IS' BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

//version persistRefresh=true
//version persistRefresh=false

import ^ as root;

countryRecord := RECORD
    string country;
    integer4 population;
END;

ds1 := DATASET([{'Spain', 40397842},
                {'Sweden', 9016596},
                {'Switzerland', 7523934},
                {'UK',60609153}], countryRecord);

ds2 := DATASET([{'Spain', 40397842},
                {'Sweden', 9016596},
                {'Switzerland', 7523934},
                {'United Kingdom', 60609153}], countryRecord);

persistRefresh := #IFDEFINED(root.persistRefresh, true);

ds := if(persistRefresh=true, ds2, ds1);

CountriesDS := ds:PERSIST('~REGRESS::PersistRefresh', SINGLE, REFRESH(persistRefresh));

OUTPUT(CountriesDS);
