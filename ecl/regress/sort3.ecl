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

export htf_keys:= macro

htf_flat1 := RECORD
  //unsigned integer4 zip;
  string2 zip;
END;

rawfile := dataset('~.::htf_flat', { htf_flat1, unsigned8 __filepos { virtual(fileposition)}}, THOR);

zip_index := index(rawfile (NOT ISNULL(ZIP)), {ZIP, __filepos}, '~thor::htf::key.zip.tftotal.lname');

set of string5 _ZIP := [] : stored('zip');

zip_lookup := table(zip_index (ZIP in _ZIP), {__filepos});

output(zip_lookup)

endmacro;


htf_keys();

