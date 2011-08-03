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


// add columns in here if you want more arguments to test in your processing.
inRecord :=
            RECORD
integer         val1 := 0;
unsigned8       val2 := 0;
string10        val3;
varstring10     val4;
            END;



//Each line here defines a row of your dataset

inDataset := dataset([
        {999, 999,' 700 ',' 700 '},
        {-999, -999,' -700 ',' -700 '}
        ], inRecord);

//do your decimal calculations here, add a row for each result.
output(nofold(inDataset),
    {
        '!!',
        (string8)val1,(varstring8)val1,
        (string8)val2,(varstring8)val2,
        (unsigned4)val3,(integer4)val3,
        (unsigned4)val4,(integer4)val4,
        (unsigned8)val3,(integer8)val3,
        (unsigned8)val4,(integer8)val4,
        '$$'
    }, 'out.d00');
