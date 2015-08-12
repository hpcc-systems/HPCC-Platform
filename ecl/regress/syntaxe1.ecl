/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
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

