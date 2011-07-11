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
        age(unsigned i) := 10 * i;
        name(string s) := 'Gavin ' +  s;
        t(unsigned i) := transform(valueRecord, self.value := i);
        ds(unsigned i) := dataset([1,i,3], valueRecord);
        r(unsigned i) := row(transform(valueRecord, self.value := i*1234));
    else
        age(unsigned i) := 20 * i;
        name(string s) := 'Richard ' +  s;
        t(unsigned i) := transform(valueRecord, self.value := i+3141);
        ds(unsigned i) := dataset([i,i-1,i-2], valueRecord);
        r(unsigned i) := row(transform(valueRecord, self.value := i*4321));
    end;

    self.age := age(99);
    self.name := name('James');
//  self.ds1 := dataset(row(t(1)));
    self.ds2 := ds(99);
    self.ds3 := dataset(r(55));
    self := [];
end;


output(project(choiceTable, t1(left.include1)));


//Case 2: Simple, non functional attributes, defined in one branches, previously defined
resultRecord t2(boolean include) := transform

    integer age(unsigned i) := 10 * i;
    string name(string s) := 'Gavin ' +  s;
    t(unsigned i) := transform(valueRecord, self.value := i);
    ds(unsigned i) := dataset([1,i,3], valueRecord);
    r(unsigned i) := row(transform(valueRecord, self.value := i*1234));
    
    if not include then
        integer age(unsigned i) := 20 * i;
        string name(string s) := 'Richard ' +  s;
//      t(unsigned i) := transform(valueRecord, self.value := i+3141);
        dataset ds(unsigned i) := dataset([i,i-1,i-2], valueRecord);
        row r(unsigned i) := row(transform(valueRecord, self.value := i*4321));
    end;

    self.age := age(99);
    self.name := name('James');
//  self.ds1 := dataset(row(t(1)));
    self.ds2 := ds(99);
    self.ds3 := dataset(r(55));
    self := [];
end;


output(project(choiceTable, t2(left.include1)));


//Need to type_transform, instead of relying in presence of no_transform/no_newtransform.  May need large scale replacement

//Case 3: Simple, non functional attributes, defined in one branches, previously defined, then redefined in terms of previous values
resultRecord t3(boolean include, boolean flag2) := transform

    integer age(unsigned i) := 10 * i;
    string name(string s) := 'Gavin ' +  s;
    t(unsigned i) := transform(valueRecord, self.value := i);
    ds(unsigned i) := dataset([1,i,3], valueRecord);
    r(unsigned i) := row(transform(valueRecord, self.value := i*1234));
    
    if not include then
        integer age(unsigned i) := 20 * i;
        string name(string s) := 'Richard ' +  s;
//      t(unsigned i) := transform(valueRecord, self.value := i+3141);
        dataset ds(unsigned i) := dataset([i,i-1,i-2], valueRecord);
        row r(unsigned i) := row(transform(valueRecord, self.value := i*4321));
    end;

    if flag2 then
        integer age(unsigned i) := age(i) * 2;
        string name(string s) := name(s + ' Halliday');
//      t(unsigned i) := t(i+100);
        dataset ds(unsigned i) := ds(i+111);
        row r(unsigned i) := r(i *2);
    else
        integer age(unsigned i) := age(i) * 5;
        string name(string s) := name(s + ' Chapman');
//      t(unsigned i) := t(i-100);
        dataset ds(unsigned i) := ds(i-111);
        row r(unsigned i) := r(i *8);
    end;

    self.age := age(99);
    self.name := name('James');
//  self.ds1 := dataset(row(t(1)));
    self.ds2 := ds(99);
    self.ds3 := dataset(r(55));
    self := [];
end;


output(project(choiceTable2, t3(left.include1, left.include2)));

