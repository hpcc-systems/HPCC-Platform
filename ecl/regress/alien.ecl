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

import dt;

namesRecord :=
            RECORD
dt.pstring      surname;
string10        forename;
integer2        age := 25;
            END;

namesTable := dataset('x',namesRecord,FLAT);

output(namesTable, {length(surname),length(forename),trim(surname), surname IN ['a','b','c','d','e','f','0','1','2','3','4','5','6','7','8','9']});

unamesRecord :=
            RECORD
dt.ustring(u'\r')   surname;
            END;

unamesTable := dataset('xu',unamesRecord,FLAT);

output(unamesTable, {length(surname),trim(surname), surname IN [u'a',u'b',u'c',u'd',u'e',u'f',u'0',u'1',u'2',u'3',u'4',u'5',u'6',u'7',u'8',u'9']});
