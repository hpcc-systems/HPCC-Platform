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

import dt;

export ebcdic_dstring(ebcdic string del) := TYPE
export integer physicallength(ebcdic string s) := StringLib.EbcdicStringUnboundedUnsafeFind(s,del)+length(del)-1;
export string load(ebcdic string s) := s[1..StringLib.EbcdicStringUnboundedUnsafeFind(s,del)-1];
export ebcdic string store(string s) := (ebcdic string)s+del;
END;


layout_L90_source := record
       ebcdic_dstring((ebcdic string2) x'bf02') name;
       ebcdic_dstring(',')  address1;
   END;

d := dataset('l90::source_block', layout_L90_source ,flat);
output(choosen(d,2), {(string10)name})

