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

ds := DATASET([{'The man sat on the chair.;'}], rs);

pattern alpha := pattern('[[:alpha:]]');
PATTERN Article := ['a', 'the', 'an'];
PATTERN EndPunct := ['.', '!', '?', ';'];
PATTERN Word := Alpha+;

TOKEN Preposition := ['of', 'on', 'above', 'below', 'under', 'over', 'in', 'during'];

RULE Noun :=  (Word penalty(5)) | (Article Word) ;
RULE Verb := Word;

RULE PP := Preposition USE(NP);
RULE NP := Noun | (SELF PP);

RULE Object := NP;
RULE IObject := PP;

RULE VP := Verb OPT(Object) OPT(IObject);

RULE Sentence := NP VP EndPunct;

NLPRec :=
RECORD
//  string tree := 'Tree: '+parseLib.getParseTree();
    MatchSentence := MATCHTEXT(Sentence);
    MatchNP := MATCHTEXT(Sentence/NP);
    MatchVP := MATCHTEXT(Sentence/VP);
    MatchVerb := MATCHTEXT(Sentence/VP/Verb);
    MatchObject := MATCHTEXT(Sentence/VP/Object);
    MatchIObject := MATCHTEXT(Sentence/VP/IObject);
END;

ParseIt := PARSE(ds, line, Sentence, NLPRec,
                    SKIP([' ','\n','\t','\r']*), PARSE, BEST, many, scan all, NOCASE);

output(ParseIt);
