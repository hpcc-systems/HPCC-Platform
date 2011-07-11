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
integer         val1 := 0;
integer         val2 := 0;
integer         val3 := 0;
integer         val4 := 0;
integer         val5 := 0;
            END;



//Each line here defines a row of your dataset

inDataset := dataset([
        {0,1,2,3,4},
        {1,2,3,4,5},
        {2,3,4,5,6}
        ], inRecord);

//do your decimal calculations here, add a row for each result.
outRecord := RECORD
decimal10_2     val1    := (decimal10_2)inDataset.val1 * inDataset.val2 * (decimal10_2)1.23;
decimal10_2     val2    := (decimal10_2)inDataset.val1 + inDataset.val2;
decimal10_2     val3    := (decimal10_2)inDataset.val1 / inDataset.val2;
decimal10_2     val4    := (decimal10_2)inDataset.val1 % inDataset.val2;
decimal10_2     val5    := (decimal10_2)inDataset.val1 - inDataset.val2;
             END;

processDataset := table(nofold(inDataset), outRecord);


//you can use this to convert the values to strings
strRecord := RECORD
string40    str1    := (string40)processDataset.val1;
varstring40 vstr1   := (varstring40)processDataset.val1;
boolean     x       := processDataset.val1;
             END;

//send the results to a file...
output(processDataset, strRecord, 'out.d00');
