/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2021 HPCC Systems®.

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

#onwarning (4522, ignore);
#onwarning (4523, ignore);

//version multiPart=false,createValueSets=false
//version multiPart=true,createValueSets=false

//version multiPart=false,createValueSets=true
//version multiPart=true,createValueSets=true
//version multiPart=false,useLocal=true,createValueSets=true

import ^ as root;
multiPart := #IFDEFINED(root.multiPart, false);
useLocal := #IFDEFINED(root.useLocal, false);
createValueSets := #IFDEFINED(root.createValueSets, true);

//--- end of version configuration ---
#option ('createValueSets', createValueSets);

import $.Setup;
Files := Setup.Files(multiPart, useLocal);
wordIndex := Files.getWordIndex();

searchWords := DATASET(['am', 'and'], { string search});
resultRecord := RECORD
    recordof(searchWords);
    recordof(wordIndex);
END;

wordIndex mkFailureIR(string search) := TRANSFORM
    SELF.word := 'Failed to match';
    SELF := [];
END;

resultRecord mkFailureKJ(string search) := TRANSFORM
    SELF.search := search;
    SELF.word := 'Failed to match';
    SELF := [];
END;

SEQUENTIAL(
    output('non-limited');
    output(wordIndex(KEYED(kind =1) AND KEYED(word = 'am') AND WILD(doc) AND KEYED(segment = 0)));
    count(wordIndex(KEYED(kind =1) AND KEYED(word = 'and') AND WILD(doc) AND KEYED(segment = 0)));
    output(wordIndex(KEYED(kind =1) AND KEYED(word = 'and') AND WILD(doc) AND KEYED(segment = 0)));
    count(wordIndex(KEYED(kind =1) AND KEYED(word = 'and') AND WILD(doc) AND KEYED(segment = 0) AND KEYED(wpos < 20)));
    output(wordIndex(KEYED(kind =1) AND KEYED(word = 'and') AND WILD(doc) AND KEYED(segment = 0) AND KEYED(wpos < 20)));
    output(wordIndex(KEYED(kind =1) AND KEYED(word = 'and') AND KEYED(doc=0 or 0 = 0) AND KEYED(segment = 0, OPT) AND KEYED(wpos < 20, OPT)));
    output('limited');
    output(limit(wordIndex(KEYED(kind =1) AND KEYED(word = 'am') AND WILD(doc) AND KEYED(segment = 0)), 3, keyed, onfail(mkFailureIR('am'))));
    //Filter on wpos should ensure there are < 9
    output(limit(wordIndex(KEYED(kind =1) AND KEYED(word = 'and') AND WILD(doc) AND KEYED(segment = 0) AND KEYED(wpos < 20)), 9, keyed, onfail(mkFailureIR('and'))));
    //Filter on wpos is opt, so limit will be triggered
    output(limit(wordIndex(KEYED(kind =1) AND KEYED(word = 'and') AND KEYED(doc=0 or 0 = 0) AND KEYED(segment = 0, OPT) AND KEYED(wpos < 20, OPT)), 9, keyed, onfail(mkFailureIR('and'))));
    output('keyed');
    output(join(searchWords, wordIndex,
            KEYED(RIGHT.kind = 1) AND KEYED(RIGHT.word = LEFT.search) AND WILD(RIGHT.doc) AND KEYED(RIGHT.segment = 0) AND KEYED(RIGHT.wpos < 20)
            ));
    output('keyed limited');
    output(join(searchWords, wordIndex,
            KEYED(RIGHT.kind = 1) AND KEYED(RIGHT.word = LEFT.search) AND WILD(RIGHT.doc) AND KEYED(RIGHT.segment = 0) AND KEYED(RIGHT.wpos < 20),
            LIMIT(9, COUNT), ONFAIL(mkFailureKJ(LEFT.search))));
    output('done')
);
