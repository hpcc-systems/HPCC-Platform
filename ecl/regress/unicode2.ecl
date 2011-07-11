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


codePageText(varstring codepage, unsigned4 len) := TYPE
        export integer physicallength := len;
        export unicode load(data s) := TOUNICODE(s[1..len], codepage);
        export data store(unicode s) := FROMUNICODE(s, codepage)[1..len];
    END;


externalRecord := 
    RECORD
        unsigned8 id;
        codePageText('utf' + '-16be',20) firstname;
        codePageText('utf8',20) lastname;
        string40 addr;
    END;

internalRecord := 
    RECORD
        unsigned8 id;
        unicode10 firstname;
        unicode20 lastname;
        string40 addr;
    END;

external := DATASET('input', externalRecord, THOR);

internalRecord t(externalRecord l) := 
    TRANSFORM
        SELF := l;
    END;

cleaned := PROJECT(external, t(LEFT));

output(cleaned);
