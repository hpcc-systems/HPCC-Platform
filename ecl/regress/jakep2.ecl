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

#option ('optimizeGraph', false);

ppersonRecord := RECORD
string3     id := '000';
string10    surname := '';
string10    forename := '';
unsigned1 nl1 := 13;
unsigned1 nl2 := 10;
  END;

pperson := DATASET('names.d00', ppersonRecord, FLAT, OPT);
// pperson := DATASET('%jaketest%', ppersonRecord, FLAT, OPT);
pperson2 := DATASET('out1.d00', ppersonRecord, FLAT);
pperson3 := DATASET('names3.d00', ppersonRecord, FLAT);
pperson4 := DATASET('names4.d00', ppersonRecord, FLAT);

smiths := pperson(Stringlib.StringToLowerCase(surname)='Smith.....');


// s1 := sort(pperson+pperson2, surname);
s1 := sort(pperson, surname);
s2 := sort(s1, forename);

// output(s1, ,'out1.d00');
//output(pperson2, ,'out2.d00');

count(s1):PERSIST('xxx');
