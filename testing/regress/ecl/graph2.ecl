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

//nothor
//nothorlcr

stepRecord := 
            record
string          next{maxlength(20)};
unsigned        leftStep;
unsigned        rightStep;
            end;

    
stateRecord :=
            record
unsigned        step;
unsigned        docid;  //dummy field
unsigned        value;
            end;

mkValue(unsigned value) := transform(stepRecord, self.next := (string)value; self.leftStep := -1; self.rightStep := -1);
mkOp(string1 op, unsigned l, unsigned r = -1) := transform(stepRecord, self.next := op; self.leftStep := l; self.rightStep := r);

mkState(unsigned step, unsigned value, unsigned docid = 1) := transform(stateRecord, self.step := step; self.docid := docid; self.value := value);

processExpression(dataset(stepRecord) actions, dataset(stateRecord) initial = dataset([], stateRecord)) := function

    processNext(set of dataset(stateRecord) inputs, unsigned thisStep, stepRecord action) := function
        thisLeft := inputs[action.leftStep];
        thisRight := inputs[action.rightStep];


        result := case(action.next,
                           '+'=>join(sorted(thisLeft, docid), sorted(thisRight, docid), left.docid = right.docid, 
                                     mkState(thisStep, left.value + right.value, left.docid)),
                           '-'=>join(sorted(thisLeft, docid), sorted(thisRight, docid), left.docid = right.docid, 
                                     mkState(thisStep, left.value - right.value, left.docid)),
                           '*'=>join(sorted(thisLeft, docid), sorted(thisRight, docid), left.docid = right.docid, 
                                     mkState(thisStep, left.value * right.value, left.docid)),
                           '/'=>join(sorted(thisLeft, docid), sorted(thisRight, docid), left.docid = right.docid, 
                                     mkState(thisStep, left.value / right.value, left.docid)),
                           '~'=>project(thisLeft, mkState(thisStep, -left.value, left.docid)),
                           //make two rows for the default action, to ensure that join is grouping correctly.
                           dataset([mkState(thisStep, (unsigned)action.next), mkState(thisStep, (unsigned)action.next*10, 2)]));

        return result;
    END;
                
    result := GRAPH(initial, count(actions), processNext(rowset(left), counter, actions[NOBOUNDCHECK counter]), parallel);
    return result;
end;

actions := dataset([mkValue(10), mkValue(20), mkOp('*', 1, 2), 
                    mkValue(15), mkValue(10), mkOp('+', 4, 5), mkOp('~', 6), mkOp('-', 3, 7)]);
o1 := output(processExpression(global(actions,few)));

actions2 := dataset([mkOp('*', 0, 0)]);
initial2 := dataset([{0,99,50},{0,15,40}], stateRecord);
o2 := output(processExpression(global(actions2,few), initial2));

sequential(o2);
