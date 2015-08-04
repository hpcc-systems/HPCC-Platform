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
//Check thisnode is handled as a scalar
//(using a mock up of the main text processing loop...)

mainRecord :=
        RECORD
string20            word;
unsigned8           doc;
unsigned8           wpos;
unsigned4           relevance;
        END;


executionRecord := record
unsigned1           seq;
unsigned1           action;
unsigned            lhs;
unsigned            rhs;
string20            word;
                    end;

resultRecord    := record
unsigned8             doc;
unsigned8             wpos;
                   end;

executionPlan := dataset('plan', executionRecord, thor);
optPlan := dedup(sort(executionPlan, seq), seq);


wordDataset := dataset('~words.d00',mainRecord,THOR);

wordKey := INDEX(wordDataset , { wordDataset }, {}, 'word.idx', distributed(hash(doc)));


runPlan(dataset(resultRecord) prev, dataset(executionRecord) _plan) := function

    plan := thisnode(_plan);
    numSteps1 := count(plan);                   // scalar thisnode
    numSteps2 := thisnode(count(_plan));            // scalar thisnode
    numSteps3 := thisnode(count(_plan(seq != 0)) * 100);    // scalar this node which is not a select

    doStep(dataset(resultRecord) inRows, unsigned cnt) :=
        case(plan[cnt].action,
             0=>inRows(wpos != 0),
             1=>inRows+project(wordKey(word=plan[cnt].word), transform(resultRecord, self := left)),
             inRows);

    return allnodes(loop(thisnode(prev), numSteps1 + numSteps2 + numSteps3, doStep(rows(left), counter)));
end;


initial := dataset([], resultRecord);

output(runPlan(initial, optPlan));

