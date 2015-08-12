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


expandedRecord := RECORD
string20            forename;
string20            surname;
unsigned1           age;
                END;

personDataset := DATASET(
            [{'Gavin','Hawthorn',[{'Abigail',2},{'Nathan',2}]},
             {'John','Simmons',[{'Jennifer',18},{'Alison',16},{'Andrew',13},{'Fiona',10}]}],personRecord);


expandedRecord extractChildren(personRecord l, childPersonRecord r) := TRANSFORM
        SELF := r;
        SELF := l;
    END;

normalizedNames := normalize(personDataset, LEFT.children, extractChildren(LEFT, RIGHT));
output(normalizedNames,,'~nested.xml',XML,overwrite);
