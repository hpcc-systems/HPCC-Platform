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



complexName :=
            record
string30        forename;
string20        surname;
                ifblock(self.surname <> 'Windsor')
string20            middle;
                end;
            end;

idRec := record
unsigned    id;
END;

in1rec :=   record(idRec)
complexName name;
unsigned    age;
string      title;
        end;

in2rec := record
idRec;
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
