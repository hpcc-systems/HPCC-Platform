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
