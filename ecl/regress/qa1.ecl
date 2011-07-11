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

datasetlayout :=
RECORD
  STRING9 own_1_ssn;
  unsigned integer8 fpos{virtual(fileposition)};
END;
key := INDEX(dataset([],datasetlayout),{own_1_ssn,fpos},'~thor::key::a.b.c.key');


STRING9 inssn := '' : STORED('ssn');
i := MAP(inssn<>''=> key(own_1_ssn=inssn), key(own_1_ssn=inssn[1..4]));


l := LIMIT(i,5000,keyed);


output(l);

