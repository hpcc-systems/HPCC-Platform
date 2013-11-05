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


namesRecord := 
            RECORD
string20        surname := '?????????????';
string10        forename := '?????????????';
integer2        age := 25;
            END;

namesTable := dataset([
        {'Salter','Abi',10},
        {'Halliday','Gavin',31},
        {'Halliday','Liz',30},
        {'Dear','Jo'},
        {'Dear','Matthew'},
        {'X','Z'}], namesRecord);

JoinRecord := 
            RECORD
namesRecord;
string20        otherforename;
            END;

JoinRecord JoinTransform (namesRecord l, namesRecord r) := 
                TRANSFORM
                    SELF.otherforename := r.forename;
                    SELF := l;
                END;

//This should be spotted as a join to self
Joined1 := join (namesTable, namesTable, LEFT.surname = RIGHT.surname, JoinTransform(LEFT, RIGHT));
output(Joined1);

//This should not be spotted as a self join.
Joined2 := join (namesTable, namesTable, LEFT.forename = RIGHT.surname, JoinTransform(LEFT, RIGHT));
output(joined2);