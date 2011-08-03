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

mkValue(unsigned value) := transform(stepRecord, self.next := (string)value; self := []);
mkOp(string1 op, unsigned l, unsigned r = 0) := transform(stepRecord, self.next := op; self.leftStep := l; self.rightStep := r);

mkState(unsigned step, unsigned value, unsigned docid = 1) := transform(stateRecord, self.step := step; self.docid := docid; self.value := value);

processExpression(dataset(stepRecord) actions) := function

    processNext(dataset(stateRecord) in, unsigned thisStep, stepRecord action) := function
        thisLeft := in(step = action.leftStep);
        thisRight := in(step = action.rightStep);
        otherSteps := in(step != action.leftStep and step != action.rightStep);

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

        return otherSteps + result;
    END;

    initial := dataset([], stateRecord);
    result := LOOP(initial, count(actions), processNext(rows(left), counter, actions[NOBOUNDCHECK counter]));
    return result;
end;

actions := dataset([mkValue(10), mkValue(20), mkOp('*', 1, 2),
                    mkValue(15), mkValue(10), mkOp('+', 4, 5), mkOp('~', 6), mkOp('-', 3, 7)]);
output(processExpression(actions));
