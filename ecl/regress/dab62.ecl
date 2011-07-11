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


r := record,maxlength(2000)
            string the_line;
  end;

d := dataset('~in::cr20041111',r,csv);

pattern f_contents := pattern('[^¦]')*;
pattern field := f_contents '¦';
pattern body := field+ f_contents;
pattern line := first body last;

r1 := record
  string l := d.the_line;
            string txt := matchtext(body/f_contents[1]);
            string txt1 := matchtext(body/f_contents[2]);
            string txt2 := matchtext(body/f_contents[3]);
            string txt3 := matchtext(body/f_contents[4]);
  end;
p := parse(d,the_line,line,r1);

output(p)

 

