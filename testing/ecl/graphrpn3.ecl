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

processExpression(dataset(stepRecord) actions) := function

    processNext(set of dataset(stateRecord) in, unsigned thisStep, stepRecord action) := function
        thisLeft := in[action.leftStep];
        thisRight := in[action.rightStep];


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
                
    initial := dataset([], stateRecord);
    result := GRAPH(initial, count(actions), processNext(rowset(left), counter, actions[NOBOUNDCHECK counter]), parallel);
    return result;
end;

actions := dataset([mkValue(10), mkOp('*', 1, 1), 
                    mkValue(15), mkOp('+', 3, 3), mkOp('-', 2, 4)]);
output(processExpression(global(actions,few)));
