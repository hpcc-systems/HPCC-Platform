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

rec :=
  record
    string2 state;
  end;


string searchstate := 'FL     ': stored('searchState');

output(dataset([{'FL'}], rec),,'regress::stringkey', OVERWRITE);
rawfile := dataset('regress::stringkey', rec, THOR, preload);

//filtered := rawfile(keyed(state in [searchstate[1..2]]));
filtered := rawfile(keyed(state = searchstate[1..2]));
 
output(filtered);


set of string searchstate2 := ['FL'] : stored('searchState2');

filtered2 := rawfile(keyed(state in searchstate2));
 
//output(filtered2);
