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

datalibx := service
        unsigned4 NameMatch(const string le_f, const string le_m, const string le_l,
                      const string ri_f,const string ri_m,const string ri_l)  : c, pure, entrypoint='dataNameMatch';
    end;

name_match() := macro
(string)(datalibx.NameMatch
(u1.fname,u1.mname,u1.lname,u2.fname,u2.mname,u2.lname))
endmacro;



r1 :=
            RECORD
qstring20       fname;
qstring20       mname;
qstring20       lname;
            END;

r2 := record
    r1;
    qstring100 res1;
    qstring100 res2;
    end;

t1 := dataset('t1',r1,FLAT);
t2 := dataset('t2',r1,FLAT);

r2 t(r1 u1, r1 u2) := transform
    self.res1 := 'name(' + name_match() + ')';
    self.res2 := 'namex(' + name_match() + ')';
    self := u1;
    end;

j := join(t1, t2, left.fname = right.fname, t(left, right));

output(j);


