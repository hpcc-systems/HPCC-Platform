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

#option ('globalFold', 1);

r := record
  string1000000 line1;
  varstring1000000 line2;
  unicode1000000 line3;
  varunicode1000000 line4;
  qstring1000000 line5;
  data1000000 line6;
  end;

ds := dataset([
{(string1000000)'a',(varstring1000000)'b',(unicode1000000)'c',(varunicode1000000)'d',(qstring1000000)'e',(data1000000)'f'}],r);


output(ds);


r2 := record
  string1000 line1;
  varstring1000 line2;
  unicode1000 line3;
  varunicode1000 line4;
  qstring1000 line5;
  data1000 line6;
  end;

ds2 := dataset([
{(string1000)'a',(varstring1000)'b',(unicode1000)'c',(varunicode1000)'d',(qstring1000)'e',(data1000)'f'}],r2);


output(ds2);

r3 := record
  string1000 line1;
  string1000 line2;
  unicode1000 line3;
  unicode1000 line4;
  string1000 line5;
  string1000 line6;
  end;

ds3 := dataset([
{(string1000)'a',(varstring1000)'b',(unicode1000)'c',(varunicode1000)'d',(qstring1000)'e',(data1000)'f'}],r3);


output(ds3);
