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

ppersonRecord := RECORD
string10    surname ;
varstring10 forename;
unicode     uf1;
varunicode10 uf2;
decimal10_2  df;
integer4    nl;
  END;


string x(string a) := (trim(a) + ',');


pperson := DATASET('in.d00', ppersonRecord, FLAT);

output(pperson, {hash(surname),hash(forename),hash(nl),hash(x(surname)),hash(surname,forename)}, 'outh.d00');
output(pperson, {hashcrc(surname),hashcrc(forename),hashcrc(nl),hashcrc(x(surname)),hashcrc(surname,forename)}, 'outc.d00');
output(pperson, {hash(uf1),hash(uf2),hash(df)});
output(pperson, {hash32(surname,forename)});


output(pperson, {hash64(surname),hash64(forename),hash64(uf1),hash64(uf2),hash64(df),hash64(nl)});
output(pperson, {hash32(surname),hash32(forename),hash32(uf1),hash32(uf2),hash32(df),hash32(nl)});

hash('hi')-hash('hi ');
hash(10)-hash((integer5)10);
