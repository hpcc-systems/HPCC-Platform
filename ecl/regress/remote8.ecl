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

#option ('targetClusterType', 'roxie');
//Check thisnode is handled, this time inside a nested allnodes;

mainRecord :=
        RECORD
string20            word;
unsigned8           doc;
unsigned8           wpos;
unsigned4           relevance;
        END;


wordDataset := dataset('~words.d00',mainRecord,THOR);
matchedAlready := dataset('~matched.d00',mainRecord,THOR);
bestPrevious1 := topn(matchedAlready, 1, -relevance);

wordKey := INDEX(wordDataset , { wordDataset }, {}, 'word.idx', distributed(hash(doc)));
wordKey2 := INDEX(wordDataset , { wordDataset }, {}, 'word2.idx');


bestPrevious := project(bestPrevious1, transform(recordof(wordKey2), self := left; self := []));

wordKey t(wordKey l) := transform
    matches := wordKey2(word = l.word);
    best1 := topn(thisnode(bestPrevious) + matches, 1, -relevance);
    best2 := topn(allnodes(local(best1)), 1, -relevance);
    self.doc := best2[1].doc;
    self := l;
    end;

q := project(wordKey, t(left));
output(allnodes(q));


