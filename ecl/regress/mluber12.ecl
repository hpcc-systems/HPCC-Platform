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

rs :=
RECORD
STRING100 line;
END;

ds := DATASET([{'aaaaa'}], rs);

Pattern A := PATTERN('[a]');
PATTERN twoAs := A A NOT BEFORE A;

outRec :=
RECORD
outPos := matchposition;
outLength := matchlength;
outField := MATCHTEXT(twoAs);
END;

outDS := PARSE(ds, line, twoAs, outRec, Scan);

output(outDs);
