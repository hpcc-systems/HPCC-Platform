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

RETURN MODULE

import macro3a,macro3b;

foob := record
string20 fname;
string20 mname;
string20 lname;
end;


df := dataset([{'FRED','P','FOOBAR'},{'MARY','Q','PUBLIC'},
{'ALFRED','D','BUTLER'}],foob);

outfile := df;
output(outfile);

//macro3b.macro2(df,outfile)

export foo_attr1 := outfile;

END;
