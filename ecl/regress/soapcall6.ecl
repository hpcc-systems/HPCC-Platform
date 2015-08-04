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

namesRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

ds := dataset('x',namesRecord,FLAT);


inRecord :=
    RECORD
        string name{xpath('Name')};
        unsigned6 id{xpath('ADL')};
    END;

outRecord :=
    RECORD
        string name{xpath('Name')};
        unsigned6 id{xpath('ADL')};
        real8 score;
    END;

inRecord t(namesRecord l) := TRANSFORM
        SELF.name := l.surname;
        SELF.id := l.age;
    END;

string myUserName := '' : stored('myUserName');
string myPassword := '' : stored('myPassword');
string mySoapVar := 'x' : stored('mySoapVar');

SOAPCALL(ds, 'ip', 'service', {ds.surname, ds.age});
SOAPCALL(ds, 'ip', 'service', {ds.surname, ds.age}, HEADING('<x>','</x>'));
SOAPCALL(ds, 'ip', 'service', inRecord, t(LEFT));
SOAPCALL(ds, 'ip', 'service', inRecord, t(LEFT), HEADING('<x>','</x>'));

output(SOAPCALL(ds, 'ip', 'service', {ds.surname, ds.age}, DATASET(outRecord), literal,namespace('soapnamespace')));
output(SOAPCALL(ds, 'ip', 'service', {ds.surname, ds.age}, DATASET(outRecord),namespace('soapnamespace',mySoapVar), HEADING('<x>','</x>')));
output(SOAPCALL(ds, 'ip', 'service', inRecord, t(LEFT), DATASET(outRecord)));
output(SOAPCALL(ds, 'ip', 'service', inRecord, t(LEFT), DATASET(outRecord), HEADING('<x>','</x>'),group,parallel(100),merge(200)));
