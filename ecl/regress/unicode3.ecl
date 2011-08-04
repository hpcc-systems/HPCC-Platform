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
