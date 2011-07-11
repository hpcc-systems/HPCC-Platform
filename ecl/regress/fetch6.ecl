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

r := record
  string1000 line;
  boolean ok;
  unsigned integer8 __filepos { virtual(fileposition)};
end;

KJV_File := dataset('~thor::in::ktext',r,csv(separator('')));

df := kjv_file;

rs := record
  unsigned1 book := 0;
  unsigned1 chapter := 0;
  unsigned1 verse := 0;
  unsigned8 __fpos{virtual(fileposition)};
end;

Key_KJV_File := INDEX(rs,'~thor::key.kjv.verse');

i := Key_KJV_File(book=1,chapter=1,verse=2);

myline := record
  string200 line;
  boolean ok;
  unsigned8 filepos;
END;

myline get(df le, i ri) := transform
  self.filepos := ri.__fpos;
  self.line := le.line;
  self.ok := le.ok;
  end;

fd := fetch(df,i,right.__fpos,get(left, right));

output(sort(fd(line != ''), filepos), { line });
