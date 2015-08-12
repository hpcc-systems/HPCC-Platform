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

pattern a := 'a';
pattern b := 'b';
pattern c := 'c';
pattern d := 'd';

pattern x1 := 'x1';
pattern x3 := 'x3';
pattern x4 := 'x4';

pattern y1 := 'y1';
pattern y2 := 'Z'*;

pattern y := y1 y2;
pattern x2 := y;
pattern x := x1 x2 x3 x4;
pattern grammar := a x b y c x d;








infile := DATASET(
    [{'ax1y1ZZZx3x4by1ZZcx1y1Zx3x4d'},          // two matches
     {'aba'}
     ], { string line, string expected := ''});

results :=
    record
        MATCHTEXT(grammar);
        MATCHTEXT(y2);          // ZZZ
        MATCHTEXT(y2[2]);       // ZZ
        MATCHTEXT(y2[3]);       // Z
    end;


outfile := PARSE(infile,line,grammar,results,SCAN,ALL);

output(outfile);
