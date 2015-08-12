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

childRecord := RECORD
string10            location;
boolean             found;
                end;

baseRecord := RECORD
unsigned8       id;
string20        surname;
            end;

namesRecord :=
            RECORD(baseRecord)
string10        forename;
integer2        age := 25;
            END;

extraRecord := RECORD
unsigned6       id;
integer2        age;
string13        phone;
string          email;
childRecord     x;
            END;

//Clashing types
output(dataset('ds1', namesRecord AND extraRecord, thor),,'o1');
output(dataset('ds2', namesRecord OR extraRecord, thor),,'o2');
output(dataset('ds3', namesRecord AND NOT extraRecord, thor),,'o3');
output(dataset('ds4', namesRecord - extraRecord, thor),,'o4');

//Non existant field
output(dataset('ds5', namesRecord - x, thor),,'o5');
output(dataset('ds6', namesRecord - [id, x], thor),,'o6');

//No resulting fields
output(dataset('ds7', namesRecord and not namesrecord, thor),,'o7');

