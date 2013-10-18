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

//noThor

namesRecord := 
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

namesTable1 := dataset([
        {'Halliday','Liz',34},
        {'Halliday','Gavin',35}], namesRecord);

namesTable2 := dataset([
        {'Halliday','Nathan',2},
        {'Halliday','Abigail',2}], namesRecord);


combinedRecord := 
            record
unsigned        id;
dataset(namesRecord)    parents;
dataset(namesRecord)    children;
            end;

infile := dataset([0,1,2,3],{unsigned id});     // make sure the child queries are executed multiple times.

combinedRecord t1(infile l) :=
        TRANSFORM
            SELF := l;
            SELF.parents := global(namesTable1, few);
            SELF.children := global(namesTable2, many);
        END;

p1 := project(infile, t1(LEFT));
output(p1);


combinedRecord t2(infile l) :=
        TRANSFORM
            SELF := l;
            SELF.parents := sort(global(namesTable1, few), surname, forename);
            SELF.children := sort(global(namesTable2, many), surname, forename);
        END;

p2 := project(infile, t2(LEFT));
output(p2);

bestRecord := 
            record
unsigned        id;
namesRecord     firstParent;
namesRecord     firstChild;
            end;

bestRecord t3(infile l) :=
        TRANSFORM
            SELF := l;
            SELF.firstParent:= global(namesTable1[1], few);
            SELF.firstChild := global(namesTable2[1], many);
        END;

p3 := project(infile, t3(LEFT));
output(p3);


