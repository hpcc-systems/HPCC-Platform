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

mainRecord :=
        RECORD
string20            word;
unsigned8           doc;
unsigned8           wpos;
unsigned4           relevance;
        END;

wordDataset := dataset('~words.d00',mainRecord,THOR);

wordKey := INDEX(wordDataset , { wordDataset }, {}, 'word.idx');


string20 word1 := '' : stored('word1');
string20 word2 := '' : stored('word2');
unsigned4 numResults := 20 : stored('numResults');

s1 := join(wordKey(word=word1), wordKey, left.doc = right.doc and right.word = word2, transform(left));

t1 := group(topn(s1, numResults, -relevance, doc, wpos), doc);

t2 := topn(allnodes(local(t1)), numResults, -relevance, doc, wpos);

output(t2);
