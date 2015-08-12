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

#option ('targetClusterType', 'roxie');

namesRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
unsigned8       filepos{virtual(fileposition)};
            END;

d := dataset('x',namesRecord,FLAT);

i1 := index(d, { d } ,'\\home\\person.name_first.key1');
nameIndexRecord := recordof(i1);

errorRecord := RECORD
unsigned4           code;
string50            msg;
               END;

fullRecord := RECORD(namesRecord)
errorRecord     err;
            END;

fullRecord t(nameIndexRecord l) := transform
    SELF := l;
    SELF := [];
END;

fullRecord createError(unsigned4 code, string50 msg) := transform
    SELF.err.code := code;
    SELF.err.msg := msg;
    SELF := [];
END;

//Illegal return type
res3a := fetch(d, i1(surname='Jones'), right.filepos);
res3b := limit(res3a, 99, ONFAIL(createError(99, 'Too many matching names 3')));
output(res3b);
