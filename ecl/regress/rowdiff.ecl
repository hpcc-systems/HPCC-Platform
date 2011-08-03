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



complexName :=
            record
string30        forename;
string20        surname;
                ifblock(self.surname <> 'Windsor')
string20            middle;
                end;
            end;


in1rec :=   record
unsigned    id;
complexName name;
unsigned    age;
string      title;
        end;

in2rec := record
unsigned    id;
complexName name;
real8       age;
boolean     dead;
        end;


in1 := dataset([
        {1,'Gavin','Hawthorn','',33,'Mr'},
        {2,'Mia','Hawthorn','',33,'Dr'},
        {3,'Elizabeth','Windsor',99,'Queen'}
        ], in1rec);


in2 := dataset([
        {1,'Gavin','Hawthorn','',33,false},
        {2,'Mia','','Jean',33,false},
        {3,'Elizabeth','Windsor',99.1,false}
        ], in2rec);

outrec :=
        record
unsigned        id;
string35        diff1;
string35        diff2;
        end;

outrec t1(in1 l, in2 r) := transform
//      self.id := if(l = r, SKIP, l.id);
        self.id := l.id;
        self.diff1 := rowdiff(l, r);
        self.diff2 := rowdiff(l.name, r.name);
    end;

output(join(in1, in2, left.id = right.id, t1(left, right)));


outrec t2(in1 l, in2 r) := transform
        self.id := l.id;
        self.diff1 := rowdiff(l, r, count);
        self.diff2 := rowdiff(l.name, r.name, count);
    end;

output(join(in1, in2, left.id = right.id, t2(left, right)));

output(workunit);
