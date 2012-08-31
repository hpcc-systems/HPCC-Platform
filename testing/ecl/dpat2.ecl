/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

 d := dataset([{'In the beginning God created the heaven and the earth.'},{'I like cheese'}],
{string1000 line});

token Word := PATTERN('[-a-zA-Z\']+');
pattern Verbs := ['created'];
rule Verb := Word IN Verbs;
pattern Articles := ['a','the'];
rule article := Word IN Articles;
pattern Conjunctions := ['and'];
rule conjunction := Word IN Conjunctions;
rule NounPhraseComponent := Word penalty(1) | article Word;
rule NounPhrase := NounPhraseComponent opt(conjunction NounPhraseComponent);
rule clause := NounPhrase Verb NounPhrase;

results := 
    record
        MATCHED;
        Subject :=  MATCHTEXT(NounPhrase[1]);
        Object  :=  MATCHTEXT(NounPhrase[2]);
        Action :=  MATCHTEXT(Verb[1]);
    end;

outfile1 := PARSE(d,line,clause,results,best,max,scan,atmost(1),skip([' ','\t',',','.',';',':']));

count(outfile1);
output(choosen(outfile1,1000),named('Standard'));

output(PARSE(d,line,clause,results,best,max,scan,not matched,skip([' ','\t',',','.',';',':'])),named('NotMatched'));
output(PARSE(d,line,clause,results,best,max,scan,not matched only,skip([' ','\t',',','.',';',':'])),named('NotMatchedOnly'));
