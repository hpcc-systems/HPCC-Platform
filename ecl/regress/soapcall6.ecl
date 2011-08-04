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
