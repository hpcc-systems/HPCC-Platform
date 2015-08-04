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
//Ensure that local is correctly added to sub queries and all children.

mainRecord :=
        RECORD
string20            word;
unsigned8           doc;
unsigned8           wpos;
unsigned4           relevance;
        END;

wordDataset := dataset('~words.d00',mainRecord,THOR);

wordKey := INDEX(wordDataset , { wordDataset }, {}, 'word.idx');
wordKey2 := INDEX(wordDataset , { wordDataset }, {}, 'word2.idx');


wordKey t(wordKey l) := transform
    self.doc := wordKey2(word = l.word)[1].doc;
    self := l;
    end;
x := project(wordKey, t(left));

output(local(x));


//Ensure join(Local(key)) is treated as a local join
j := join(local(wordKey), local(wordKey2), left.word = right.word and left.doc = right.doc, transform(mainRecord, self.wpos := right.wpos; self := left));
output(j);


//Ensure join(noLocal(key)) is treated as a nonlocal join
j2 := join(wordKey, nolocal(wordKey2), left.word = right.word and left.doc = right.doc, transform(mainRecord, self.wpos := right.wpos; self := left));
output(local(j2));
