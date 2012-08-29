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

/*

A simplified PROCESS statement to allow efficient processing of tree structured input.

Only operations are

PUSH
POP
OUTPUT()
and REPLACE


*/


inputRecord :=
            record
string          next;
            end;

valueRecord :=
            record
integer         value;
            end;

inputDataset := dataset(['10','20','*',15,10,'+','-','.'], inputRecord);

processed := process(inputDataset, stackRecord, resultRecord,
                     CASE(LEFT.op,
                            '+'=>REPLACE(transform(stackRecord, self.value := MATCHED[2].value+MATCHED[1].value), 2),
                            '-'=>REPLACE(transform(stackRecord, self.value := MATCHED[2].value-MATCHED[1].value), 2),
                            '*'=>REPLACE(transform(stackRecord, self.value := MATCHED[2].value*MATCHED[1].value), 2),
                            '~'=>REPLACE(transform(stackRecord, self.value := -RIGHT.value, 1),
                            '.'=>OUTPUT(RIGHT) POP(1),
                                 PUSH(transform(stackRecord, self.value := (integer)value));
                     );

output(processed);


