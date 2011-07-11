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

export xxNameLib := SERVICE
  data      FirstNameToToken(const string20 name) : c, pure, entrypoint='nameFirstNameToToken'; 
  string20  TokenToFirstName(const data0 name) : c, pure, entrypoint='nameTokenToFirstName'; 
  unsigned4 TokenToLength(const data name) : c, pure, entrypoint='nameTokenToLength'; 
END;

export fnstring20 := TYPE
  export unsigned1 physicallength(const data s) := xxNameLib.TokenToLength(s);
  export string20 load(const data s) := xxNameLib.TokenToFirstName(s);
  export data store(string20 s) := xxNameLib.FirstNameToToken(s);
END;


rec := record
unsigned6    did;
packed integer4  score;
string       unkeyable;
qstring      title{maxlength(4)};
fnstring20   fname;
        end;


set of unsigned6 didSeek := ALL : stored('didSeek');
set of qstring5 titleSeek := ALL : stored('titleSeek');
set of string20 fnameSeek := ALL : stored('fnameSeek');

rawfile := dataset('~thor::rawfile', rec, THOR, preload);

// Combine matches
filtered := rawfile(
 keyed(did in didSeek),
 keyed(title in titleSeek),
 keyed(fname in fnameSeek));
 
output(filtered)
