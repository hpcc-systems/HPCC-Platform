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
  string200 s;
  end;
d := dataset([{'KEYA:Fred; KEYB:Edith KEYC:John'}],r);

PATTERN labels := ['KEYA:','KEYB:','KEYC:'];
PATTERN termin := (';' OR '||' OR ' ');
PATTERN termination :=  (termin+ before ( labels or last )) or last;

//PATTERN val1 := ANY+ before labels;
//PATTERN val2 := ANY+ before ('||' or last);
pattern body := (ANY not in termin)+;
PATTERN items := opt(labels) body termination;


r1 := record
  string res := matchtext(body);
  end;

p := parse(d,s,items,r1,min,many);

output(p);
