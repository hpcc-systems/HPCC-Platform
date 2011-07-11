/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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

