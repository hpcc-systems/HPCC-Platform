/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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
unsigned8       id;
integer2        age;
string13        phone;
string          email;
childRecord     x;
            END;

output(dataset('ds1', namesRecord AND extraRecord, thor),,'o1');
output(dataset('ds2', namesRecord OR extraRecord, thor),,'o2');
output(dataset('ds3', namesRecord AND NOT extraRecord, thor),,'o3');
output(dataset('ds4', namesRecord - extraRecord, thor),,'o4');
output(dataset('ds5', namesRecord - id, thor),,'o5');
output(dataset('ds6', namesRecord - [id, surname], thor),,'o6');
output(dataset('ds7', namesRecord and not age, thor),,'o7');
output(dataset('ds8', namesRecord and not [id, surname, forename], thor),,'o8');

//More nasty: ifblocks....

biggerRecord := RECORD(baseRecord)
boolean             hasExtra;
                    ifblock(self.hasExtra)
extraRecord             extra;
                    END;
                END;

output(dataset('ds11', biggerRecord, thor),,'o11');
output(dataset('ds12', biggerRecord - surname, thor),,'o12');
output(dataset('ds13', biggerRecord - hasExtra, thor),,'o13');
output(dataset('ds14', biggerRecord - extra, thor),,'o14');
