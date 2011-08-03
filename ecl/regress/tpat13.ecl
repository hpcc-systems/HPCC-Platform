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
  string line;
  end;

ds := dataset([
{'ii iv ix  viii viiii'},
{''}],r);


PATTERN i := REPEAT('I', 1, 3);
PATTERN iv := 'IV';
PATTERN ix := 'IX';
PATTERN v := 'V';
PATTERN x := REPEAT('X', 1, 3);
PATTERN xl := 'XL';
PATTERN xc := 'XC';
PATTERN l := 'L';
PATTERN c := REPEAT('C', 1, 3);
PATTERN cd := 'CD';
PATTERN cm := 'CM';
PATTERN d := 'D';
PATTERN m := REPEAT('M', 1, 3);
PATTERN roman_digits := ['I', 'V', 'X', 'L', 'C', 'D', 'M']+;
PATTERN roman_sequence :=  m? (cm? | (d? c? | cd?)) (xc? | (l? x? | xl?)) (ix? | (v? i? | iv?));
PATTERN roman_numeral := roman_digits in roman_sequence;

pattern S := roman_numeral;


results :=
    record
        string outline := MATCHTEXT;
    end;

output(PARSE(ds,line,S,results,nocase,max,many));

