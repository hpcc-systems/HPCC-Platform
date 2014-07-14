/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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
        (string8)val1,val1b := (varstring8)val1,
        (string8)val2,val2b := (varstring8)val2,
        (unsigned4)val3,val3b := (integer4)val3,
        (unsigned4)val4,val4b := (integer4)val4,
        val3c := (unsigned8)val3,val3d := (integer8)val3,
        val4c := (unsigned8)val4,val4d := (integer8)val4,
        '$$'
    }, 'out.d00');
