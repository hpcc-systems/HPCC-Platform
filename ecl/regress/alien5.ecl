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


JPEG(INTEGER len) := TYPE
        EXPORT DATA LOAD(DATA D) := D[1..len];
        EXPORT DATA STORE(DATA D) := D[1..len];
        EXPORT INTEGER PHYSICALLENGTH(DATA D) := len;
END;


Layout_Common :=
RECORD, MAXLENGTH(50000)
    unsigned6 did := 0;
    string2 state;
    string2 rtype;
    string20 id;
    unsigned2 seq;
    string8 date;
    unsigned2 num;
    UNSIGNED4 imgLength := 3;
    JPEG(SELF.imgLength) photo;
END;

string100 fs0 := 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX';
string1000 fs1 := fs0 + fs0 + fs0 + fs0 + fs0 + fs0 + fs0 + fs0 + fs0 + fs0;
string10000 fs2 := fs1 + fs1 + fs1 + fs1 + fs1 + fs1 + fs1 + fs1 + fs1 + fs1;
string50000 fs := fs2 + fs2 + fs2 + fs2 + fs2;

df := dataset('df', Layout_Common, thor);

newrec := record, maxlength(50000)
    df;
    string filler;
end;

newrec into(df L) := transform
    self.filler := fs[1..(50000 - (L.imglength + 46))];
    self := L;
end;

newfile := project(df,into(LEFT));

x := sort(newfile, photo);
output(x);
