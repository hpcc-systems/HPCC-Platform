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

#option ('foldAssign', false);
#option ('globalFold', false);
personRecord := RECORD
string5    id;
string20   forename;
string1    initial;
string20   surname;
string2    age;
unsigned integer1   salaryhi;
integer1   salarylo;
integer2   salary;
decimal5_2 credit;
string8    dob;
string1    pad1;
big_endian integer2 rev1;
string2    nl;
END;

pperson := DATASET('in.d00', personRecord, FLAT);

isOld     := pperson.age > '70';

oldPerson := pperson(isOld);

adult := pperson(age > '18');

youngAdult := adult(NOT isOld);

nameRecord := RECORD
string20   surname := pperson.surname;
string20   forename := pperson.forename;
string50   fullname := (varstring)pperson.surname + ',' + (varstring)pperson.forename;
varstring10 x := pperson.surname;
unsigned4  trimlen := length(trim(pperson.surname));
unsigned4  trimvlen := length(trim((varstring)pperson.surname));
varstring20  trimname := trim(pperson.surname);
varstring20  trimvname := trim((varstring)pperson.surname);
//decimal10_2 temp := 0;
END;

nameRecord2 := RECORD
boolean   same := pperson.surname = (varstring)pperson.surname;
string10  sub1 := pperson.surname[1..10];
string10  sub2 := ('Gavin')[2..];
string10  sub3 := (trim(pperson.surname) + ',' + trim(pperson.forename))[5..15];
string5  sub4 := (realformat(11.1,4,2));
string5  sub5 := intformat(123,5,1);
END;

nameRecord3 := RECORD
string1 first1 := pperson.initial;
string1 first2 := pperson.surname[1..1];
//boolean c1 := pperson.credit * 2 > pperson.credit;
//integer c2 := pperson.credit * 2 <=> pperson.credit;
//integer c3 := pperson.credit <=> pperson.credit * 2;
decimal6_4 x1 := (decimal6_4)1 + (decimal6_4)2;
END;

EncryptService := SERVICE
string encrypt(const string src) : library='testcall', entrypoint='encrypt';
string20 decrypt(const string src) : library='testcall', entrypoint='decrypt';
    END;


nameRecord4 := RECORD
string20  surname   := EncryptService.encrypt(pperson.surname);
string20  surname2  := EncryptService.decrypt(EncryptService.encrypt(pperson.surname));
END;

Hex := [ '0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F' ];

nameRecord5 := RECORD
//real    a := 1 <=> 2;
integer   b := map(1=2=>pperson.salaryhi,2=3=>pperson.salaryhi-pperson.salarylo,false=>1.1,3)*2;
integer   c := case(1,pperson.salaryhi=>10,pperson.salaryhi-pperson.salarylo=>20,30);
integer   d := case(1.1,pperson.salaryhi=>10,pperson.salaryhi-pperson.salarylo=>20,30);
integer   e := case(1,pperson.salaryhi=>10,pperson.salaryhi-pperson.salarylo=>20.1,30);
integer   f := case(pperson.salaryhi,1=>10,2=>20.1,3=>50,30);
integer   g := pperson.initial='x';
integer   h := pperson.rev1;
big_endian integer i := pperson.salary;
big_endian integer2 j := pperson.rev1;
//string1   h := Hex[3];
//integer4   result := choose(10, pperson.salaryhi, pperson.salaryhi*2,1.1,-99);
//varstring10 name := choose(10, 'a','ab','abc','Gavin'[2..4]);
//string20  surname := pperson.surname * pperson.forename;
END;

nameRecord6 := RECORD
ebcdic string10 name := pperson.surname;
string10 name2 := (ebcdic string15)pperson.surname;
                END;

//output(pperson,{surname,forename},'out.d00');
//output(pperson,personRecord,'out.d00');
//output(oldPerson,nameRecord,'out.d00');
//output(oldPerson,{surname,forename},'out.d00');
//output(youngAdult,{surname,forename},'out.d00');

//family := DEDUP(pperson,LEFT.surname=RIGHT.surname);
//family := DEDUP(pperson,LEFT.surname=RIGHT.surname,RIGHT);
//family := DEDUP(pperson,LEFT.surname=RIGHT.surname,RIGHT,ALL);
//family := DEDUP(pperson,LEFT.surname=RIGHT.surname,KEEP(2));
//groupedPerson := GROUP(pperson,surname);
//family := DEDUP(groupedPerson,LEFT.surname=RIGHT.surname,ALL);
//family := DEDUP(pperson,surname);

personRecord combineFamily1(personRecord l, personRecord r) := TRANSFORM
    SELF.age := (string2)((integer)l.age + (integer)r.age);
    SELF := l;
END;

personRecord combineFamily2(personRecord l, personRecord r) := TRANSFORM
    SELF.salary := l.salary + r.salary - 0x30303030;
    SELF := l;
END;

family := ROLLUP(pperson,surname,combineFamily1(LEFT,RIGHT));

oldpeople := pperson(age > '20');

//output(family,{surname,forename,salary},'out.d00');
//4/4.0;

//output (oldpeople,namerecord5,'out1.d00');
//output (oldpeople,namerecord5,'out2.d00');

output (oldpeople,namerecord5);

//sqrt(4) + ln(1) + log(1) + exp(1) + round(1.0) + roundup(1.1) + truncate(1.0) + power(1,2);
