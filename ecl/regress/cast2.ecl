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
string10        val1 := 'x';
ebcdic string10 val2 := 'y';
data10          val3 := 'z';
            END;



//Each line here defines a row of your dataset

inDataset := dataset('in', inRecord, FLAT);

inRecord t(inRecord l) := TRANSFORM 
    SELF.val1 := (string10)'x';
    SELF.val2 := (ebcdic string10)'y';
    SELF.val3 := (data10)'z';
END;

x := iterate(inDataset, t(LEFT));
//do your decimal calculations here, add a row for each result.
output(x);
