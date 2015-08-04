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

l :=
RECORD
    STRING30 word;
    unsigned fpos;
END;


wordds := dataset([{'MARK',1},{'LUBER',1},{'DRIMBAD',2},{'MARK',2}],l);


I := INDEX(wordds,{wordds},'~thor::wordsearchtest');


WordRec :=
RECORD
    STRING20 word;
    unsigned pos;
END;

PhraseRec :=
RECORD
    DATASET(WordRec) words;
    boolean isNot;
END;

Conjunction :=
RECORD
    DATASET(PhraseRec) phrases;
END;

words := DATASET([{'MARK'}],WordRec);
phraseds := DATASET([{wordds,false}],phraserec);
ds := DATASET([{phraseds}],Conjunction) : STORED('conj');



Doc :=
RECORD
    unsigned docID;
END;

Results :=
RECORD
    DATASET(Doc) Docs;
END;

Layout_PhrasesExpanded :=
RECORD
    Results;
    PhraseRec;
END;

Layout_wordsExpanded :=
RECORD
    Results;
    WordRec;
END;

Layout_PhrasesExpanded expandCons(PhraseRec le) :=
TRANSFORM
    SELF := le;
    SELF.docs := [];
END;

Layout_WordsExpanded expandPhrase(WordRec le) :=
TRANSFORM
    SELF := le;
    SELF.docs := [];
END;

Layout_WordsExpanded rollPhrase(Layout_WordsExpanded le, Layout_WordsExpanded ri) :=
TRANSFORM
    setofdocs := SET(le.docs,docid);

    SELF.docs := PROJECT(i(keyed(le.word=word) AND
                                                                                (fpos IN setofdocs OR setofdocs=[])),TRANSFORM(Doc,SELF.docid := LEFT.fpos));
    SELF := le;
END;

Layout_PhrasesExpanded rollCons(Layout_PhrasesExpanded le, Layout_PhrasesExpanded ri) :=
TRANSFORM
    wordsExpanded := PROJECT(le.words,expandPhrase(LEFT));

    SELF.docs := ROLLUP(wordsExpanded,true,rollPhrase(LEFT,RIGHT))[1].docs;
    SELF := le;
END;


Results proj(Conjunction le) :=
TRANSFORM
    phrasesExpanded := PROJECT(le.phrases,expandCons(LEFT));

    SELF.Docs := ROLLUP(phrasesExpanded,true,rollCons(LEFT,RIGHT))[1].docs;
END;

Docs := PROJECT(ds, proj(LEFT));

output(docs);
