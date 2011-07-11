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




// add columns in here if you want more arguments to test in your processing.
inRecord := 
            RECORD
integer         len;
string100       text;
ebcdic string10 etext;
            END;



inDataset := dataset('in.doo',inRecord, FLAT);

outRecord := RECORD
string50        text;
integer4        value;
            END;


outRecord doTransform(inRecord l) := TRANSFORM
            SELF.text := (varstring)l.text[1..l.len]+(varstring)'x';
            SELF.value := (integer)l.etext;
        END;

outDataset := project(inDataset, doTransform(LEFT));

output(outDataset,,'out.d00');
