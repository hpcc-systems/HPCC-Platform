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
boolean         include1;
boolean         include2;
            END;

choiceTable := nofold(dataset([{true, false}, {false, false}], choiceRecord));
choiceTable2 := nofold(dataset([{true, true}, {true, false}, { false, true }, {false, false}], choiceRecord));

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
resultRecord t1(boolean include) := transform

    if include then
        age := 10;
        name := 'Gavin';
        t := transform(valueRecord, self.value := 10);
        ds := dataset([1,2,3], valueRecord);
        r := row(transform(valueRecord, self.value := 1234));
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


output(project(choiceTable, t1(left.include1)));


//Case 2: Simple, non functional attributes, defined in one branches, previously defined
resultRecord t2(boolean include) := transform

    integer age := 10;
    string name := 'Gavin';
    t := transform(valueRecord, self.value := 10);
    ds := dataset([1,2,3], valueRecord);
    r := row(transform(valueRecord, self.value := 1234));

    if not include then
        integer age := 20;
        string name := 'Richard';
        t := transform(valueRecord, self.value := 15);
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


output(project(choiceTable, t2(left.include1)));


//Need to type_transform, instead of relying in presence of no_transform/no_newtransform.  May need large scale replacement

//Case 3: Simple, non functional attributes, defined in one branches, previously defined, then redefined in terms of previous values
resultRecord t3(boolean include, boolean flag2) := transform

    integer age := 10;
    string name := 'Gavin';
    t := transform(valueRecord, self.value := 10);
    dataset ds := dataset([1,2,3], valueRecord);
    r := row(transform(valueRecord, self.value := 1234));

    if not include then
        integer age := 20;
        string name := 'Richard';
        t := transform(valueRecord, self.value := 15);
        dataset ds := dataset([5,6,7], valueRecord);
        row r := row(transform(valueRecord, self.value := 3141));
    end;

    if flag2 then
        integer age := age * 2;
        string name := name + ' Hawthorn';
        t := transform(valueRecord, self.value := 10);
        dataset ds := ds & dataset([11,12,13], valueRecord);
        row r := project(r, transform(valueRecord, self.value := left.value - 1000000));
    else
        integer age := age * 5;
        string name := name + ' Drimbad';
        t := transform(valueRecord, self.value := 15);
        dataset ds := ds & dataset([15,16,17], valueRecord);
        row r := project(r, transform(valueRecord, self.value := left.value + 1000000));
    end;

    self.age := age;
    self.name := name;
//  self.ds1 := dataset(row(t));
    self.ds2 := ds;
    self.ds3 := dataset(r);
    self := [];
end;


output(project(choiceTable2, t3(left.include1, left.include2)));
