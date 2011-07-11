/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
        string name := name + ' Halliday';
        t := transform(valueRecord, self.value := 10);
        dataset ds := ds & dataset([11,12,13], valueRecord);
        row r := project(r, transform(valueRecord, self.value := left.value - 1000000));
    else
        integer age := age * 5;
        string name := name + ' Chapman';
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
