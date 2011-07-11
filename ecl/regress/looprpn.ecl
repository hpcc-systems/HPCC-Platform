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
unsigned        value;
            end;

mkValue(unsigned value) := transform(stepRecord, self.next := (string)value; self := []);
mkOp(string1 op, unsigned l, unsigned r) := transform(stepRecord, self.next := op; self.leftStep := l; self.rightStep := r);

processExpression(dataset(stepRecord) actions) := function

    processNext(dataset(stateRecord) in, unsigned step, stepRecord action) := function
        thisLeft := in(step = action.leftStep);
        thisRight := in(step = action.rightStep);
        otherSteps := in(step != action.leftStep and step != action.rightStep);

        newValue := case(action.next,
                           '+'=>thisLeft[1].value + thisRight[1].value,
                           '*'=>thisLeft[1].value * thisRight[1].value,
                           '/'=>thisLeft[1].value / thisRight[1].value,
                           '-'=>thisLeft[1].value - thisRight[1].value,
                           '~'=>-thisLeft[1].value,
                           (unsigned)action.next);

        result := row(transform(stateRecord, self.step := step; self.value := newValue));
        return otherSteps + result;
    END;
                
    initial := dataset([], stateRecord);
    result := LOOP(initial, count(actions), processNext(rows(left), counter, actions[counter]));
    return result[1].value;
end;

actions := dataset([mkValue(10), mkValue(20), mkOp('*', 1, 2), 
                    mkValue(15), mkValue(10), mkOp('+', 4, 5), mkOp('-', 3, 6)]);
output(processExpression(actions));
