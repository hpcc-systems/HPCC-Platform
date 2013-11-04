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

//nothor            - doesn't support child queries

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


personDataset := nofold(DATASET(
            [{'Gavin','Halliday',[{'Abigail',2},{'Nathan',2}]},
             {'John','Simmons',[{'Jennifer',18},{'Alison',16},{'Andrew',13},{'Fiona',10}]}],personRecord));


//Global if

boolean trueValue := true : stored('trueValue');
boolean falseValue := false : stored('falseValue');

output(IF(trueValue,personDataset));
output(IF(falseValue,personDataset));


//Out of line if
personRecord t(personRecord l) := transform
    self.children := IF(l.forename = 'Gavin', sort(l.children, forename));
    self := l;
end;

output(project(personDataset, t(LEFT)));


//Inline if

personRecord t2(personRecord l) := transform
    self.children := IF(l.forename = 'Gavin', l.children);
    self := l;
end;

output(project(personDataset, t2(LEFT)));

