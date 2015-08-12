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
