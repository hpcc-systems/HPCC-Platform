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

externalRecord :=
    RECORD
        unsigned8 id;
        data20 firstname;
        data20 lastname;
        string40 addr;
    END;

internalRecord :=
    RECORD
        unsigned8 id;
        unicode10 firstname;
        unicode10 lastname;
        unicode10 lastname2;
        unicode lastnamex;
        unicode lastnamex2;
        string40 addr;
    END;

external := DATASET('input', externalRecord, THOR);

string encoding := 'utf-' + '8';
string encoding2 := 'utf-' + '8n';

internalRecord t(externalRecord l) :=
    TRANSFORM
        SELF.firstname := TOUNICODE(l.firstname, 'utf' + '-16be');
        SELF.lastname := U'!' + TOUNICODE(l.lastname, encoding) + U'!#';
        SELF.lastname2 := U'!' + TOUNICODE(l.lastname, encoding2) + U'!#';
        SELF.lastnamex := TOUNICODE(l.lastname, encoding2);
        SELF.lastnamex2:= TOUNICODE(l.lastname, 'utf-32le');
        SELF := l;
    END;

cleaned := PROJECT(external, t(LEFT));

output(cleaned);
