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
    STRING line;
END;

pattern Alpha := pattern('[[:alpha:]]');

ds := DATASET([{'area. Most of eastern and central Pennsylvania is drained by the Susquehanna and Delaware systems. The western part of the state is drained by the Allegheny and Monongahela rivers, which join at Pittsburgh'}], rs);

PATTERN Back  := (ANY NOT IN Alpha) | LAST;
PATTERN Front := (ANY NOT IN Alpha) | FIRST;

PATTERN Word := Alpha+;
PATTERN TrueWord := ((Word BEFORE Back) AFTER Front);

Matches :=
    record
        STRING50 wrd := MATCHTEXT(Word);
        UNSIGNED4 MatchPos  := MATCHPOSITION(Word);
        UNSIGNED2 MatchLen  := MATCHLENGTH(Word);
    end;

found := PARSE(ds, line, TrueWord, Matches, BEST, MANY, MAX);



output(found,, 'a.out',csv,overwrite);

token Word2 := Alpha+;

Matches2 :=
    record
        STRING50 wrd := MATCHTEXT(Word2);
        UNSIGNED4 MatchPos  := MATCHPOSITION(Word2);
        UNSIGNED2 MatchLen  := MATCHLENGTH(Word2);
    end;

found2 := PARSE(ds, line, Word2, Matches2);
output(found2,, 'b.out',csv,overwrite);


