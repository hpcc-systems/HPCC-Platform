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

String20 var1 := '' : stored('watchtype');

string_rec := { string100 line; unsigned8 filepos{virtual(fileposition)}};

t := dataset(map(
                var1='nonglb'=>'~thor::BASE::Watchdog_Moxie_nonglb',
                var1='nonutility'=>'~thor::BASE::Watchdog_Moxie_nonutility',
                var1='nonglb_nonutility'=>'~thor::BASE::Watchdog_Moxie_nonglb_nonutility',
                '~thor::BASE::Watchdog_Moxie'),string_rec,flat);

b := BUILDINDEX(t,,
                map(var1='nonglb'=>'~thor::key::Watchdog_Moxie_nonglb.did',
                    var1='nonutility'=>'~thor::key::Watchdog_Moxie_nonutility.did',
                    var1='nonglb_nonutility'=>'~thor::key::Watchdog_Moxie_nonglb_nonutility.did',
                    '~thor::key::Watchdog_Moxie.did'),overwrite);

b;
