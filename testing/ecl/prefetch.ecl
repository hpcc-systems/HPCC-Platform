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

//UseStandardFiles
//nothor

in := dataset([{'boy'}, {'sheep'}], {string5 word});

outrec := record
  string5 word;
  unsigned doc;
END;

outrec trans1(in L) := TRANSFORM
  SELF.word := L.word;
  SELF.doc := SORTED(TS_WordIndex(KEYED(kind = TS_kindType.TextEntry),keyed(word = l.word)), doc)[1].doc;
END;

outrec trans2(in L) := TRANSFORM
  SELF.word := L.word;
  SELF.doc := SORTED(TS_WordIndex(KEYED(kind = TS_kindType.TextEntry),keyed(word = 'gobbledegook'+l.word)), doc)[1].doc;
END;

output(project(in, trans1(LEFT))) : independent;
output(project(in, trans1(LEFT), PREFETCH(20))) : independent;
output(project(in, trans2(LEFT))) : independent;
output(project(in, trans2(LEFT), PREFETCH(20, PARALLEL))) : independent;
