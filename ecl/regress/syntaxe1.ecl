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

IMPORT Std;
IMPORT Std.Str;

NGramRow := RECORD
  STRING ngram;
  UNSIGNED year;
  UNSIGNED match_count;
  UNSIGNED page_count;
  UNSIGNED volume_count;
END;

NGramFile(STRING filename) := FUNCTION
  RETURN DATASET(filename, NGramRow, CSV(SEPARATOR('\t'))); END;

_3gram := NGramFile('ngram0.csv') +
          NGramFile('ngram1.csv');

clean(_3gram f) := function
  return f(false);
end;

OUTPUT(CHOOSEN(clean(_3gram),100));

