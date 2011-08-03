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

/*
State machine iteration etc.



Functionality.  Want the following functionality


Action

NextRecord, apply transform and push onto stack
PopRecord from stack, apply transform to generate output
Conditionally change to a new state, depending on contents of input/top of stack.
PopRecord
Apply Transformation to elementn + element m + generate a new tos. - including clone.  Option to pop n records first

Proposal:

A new statement,
PROCESS(dataset, stackRecord, resultRecord, rootState, states);

While evaluating states the following symbols are available:
LEFT=next INPUT record
MATCHED= a dataset of stack records (count, list of pointers in this case)
RIGHT=MATCHED[1]

States are a list of states of the format

stateName := <list-of-state-actions>.  The state actions can be one of the following:

INPUT(nextState, nextStateIfInputIsEmpty)
    consume the next input record.
INPUT(transform, nextState, nextStateIfInputIsEmpty)
    create a new record on the top of stack, and then consume the next input record.  Same as CREATE() followed by an INPUT() except the empty test is done earlier.
OUTPUT(transform(RIGHT))
    Generate a record into the output stread.
IF(condition(LEFT,RIGHT), trueState, falseState)
    Conditionally decide which is the next state
CASE(condition(LEFT,RIGHT,MATCHED), value0=>value0State, defaultState)
    Conditionally decide which is the next state
POP(n/ALL);
    Remove n/ALL entries from the stack
NEXT(stateName)
    Specify which state is executed next
CREATE(transform(MATCHED[n])
    Push a new record onto the stack
REPLACE(transform(MATCHED), n, newState);
    Create a new record, pop n records off the stack, and push the new record on the top
FAIL(...)
    Fail...
END
    Don't read any more input.

*/


inputRecord :=
            record
string          next;
            end;

stackRecord :=
            record
unsigned        value;
string1         op;
            end;

outputRecord :=
            record
unsigned        value;
            end;

stackRecord createStack(inputRecord l) := transform
        self.value := (unsigned)l.next;
        self.op := IF(REGEXFIND('[0-9]+', l.next),'0', l.next[1]);
    end;


outputRecord createResult(stackRecord l) := transform
        self.value := l.value;
    end;

inputDataset := dataset(['10','20','*',15,10,'+','-'], inputRecord);

processed := process(inputDataset, stackRecord, resultRecord, readNextState,
                     readNextState      := INPUT(createStack(LEFT), outputState), NEXT(reduceState)
                     reduceState        := CASE(RIGHT.op,
                                                '0'=>readNextState,
                                                '+'=>doAddState,
                                                '-'=>doSubState,
                                                '*'=>doMultState,
                                                '~'=>doNegateState,
                                                '.'=>outputSingle,
                                                FAIL()),
                     doAddState         := REPLACE(transform(stackRecord, self.op := '0', self.value := MATCHED[2].value+MATCHED[1].value, 2) NEXT(readNextState),
                     doSubState         := REPLACE(transform(stackRecord, self.op := '0', self.value := MATCHED[2].value-MATCHED[1].value, 2, NEXT(readNextState),
                     doMultState        := REPLACE(transform(stackRecord, self.op := '0', self.value := MATCHED[2].value*MATCHED[1].value, 2) NEXT(readNextState),
                     doNegateState      := REPLACE(transform(stackRecord, self.op := '0', self.value := -RIGHT.value, 1) NEXT(readNextState),
                     outputSingleState  := POP(1), OUTPUT(createResult(RIGHT)) NEXT(readNextState)
                     outputState        := OUTPUT(createResult(RIGHT)) IF(EXISTS(MATCHED), outputState, END)
                     );

output(processed);

//How covert to reverse polish?

