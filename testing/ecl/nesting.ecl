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

childPersonRecord := RECORD
string20          forename;
unsigned1         age;
    END;

personRecord := 
                RECORD
string20            forename;
string20            surname;
DATASET(childPersonRecord)   children;
                END;


personDataset := DATASET(
            [{'Gavin','Halliday',[{'Abigail',2},{'Nathan',2}]},
             {'John','Simmons',[{'Jennifer',18},{'Alison',16},{'Andrew',13},{'Fiona',10}]}],personRecord);

output(personDataset);


//This should cause the number of children to be limited to a particular range.

personRecord2 := 
                RECORD
string20            forename;
string20            surname;
unsigned2           numChildren;
DATASET(childPersonRecord,count(self.numChildren))   children;
                END;


personDataset2 := DATASET(
            [{'Gavin','Halliday',3,[{'Abigail',2},{'Nathan',2}]},
             {'John','Simmons',2,[{'Jennifer',18},{'Alison',16},{'Andrew',13},{'Fiona',10}]}],personRecord2);

output(personDataset2);
