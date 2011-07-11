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


