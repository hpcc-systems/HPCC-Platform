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


choiceRecord :=
            RECORD
unsigned        value;
            END;

choiceTable := nofold(dataset([1,2,3,4], choiceRecord));

valueRecord := record
    unsigned        value;
end;

resultRecord := record
unsigned            age;
string              name{maxlength(50)};
dataset(valueRecord) ds1{maxcount(10)};
dataset(valueRecord) ds2{maxcount(10)};
dataset(valueRecord) ds3{maxcount(10)};
            end;


//Case 1: Simple, non functional attributes, defined in both branches, not previously defined
resultRecord t1(unsigned value) := transform

    IF value = 1 then
        age := 10;
        name := 'Gavin';
        t := transform(valueRecord, self.value := 10);
        ds := dataset([1,2,3], valueRecord);
        r := row(transform(valueRecord, self.value := 1234));
    elseif value = 2 then
        age := 15;
        name := 'James';
        t := transform(valueRecord, self.value := 12);
        ds := dataset([3,4,5], valueRecord);
        r := row(transform(valueRecord, self.value := 4321));
    else
        age := 20;
        name := 'Richard';
        t := transform(valueRecord, self.value := 15);
        ds := dataset([5,6,7], valueRecord);
        r := row(transform(valueRecord, self.value := 3141));
    end;

    self.age := age;
    self.name := name;
//  self.ds1 := dataset(row(t));
    self.ds2 := ds;
    self.ds3 := dataset(r);
    self := [];
end;


output(project(choiceTable, t1(left.value)));

//Case 2: Simple, non functional attributes, defined in one branches, previously defined
//no assignments in one branch, and missing else.
resultRecord t2(unsigned value) := transform

    integer age := 10;
    string name := 'Gavin';
    t := transform(valueRecord, self.value := 10);
    ds := dataset([1,2,3], valueRecord);
    r := row(transform(valueRecord, self.value := 1234));

    if value = 1 then
    elseif value = 2 then
        integer age := 15;
        string name := 'James';
//      t := transform(valueRecord, self.value := 12);
        dataset ds := dataset([3,4,5], valueRecord);
        row r := row(transform(valueRecord, self.value := 4321));
    elseif value = 3 then
        integer age := 20;
        string name := 'Richard';
//      t := transform(valueRecord, self.value := 15);
        dataset ds := dataset([5,6,7], valueRecord);
        row r := row(transform(valueRecord, self.value := 3141));
    end;

    self.age := age;
    self.name := name;
//  self.ds1 := dataset(row(t));
    self.ds2 := ds;
    self.ds3 := dataset(r);
    self := [];
end;


output(project(choiceTable, t2(left.value)));


