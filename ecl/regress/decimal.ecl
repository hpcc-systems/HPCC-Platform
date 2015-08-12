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
