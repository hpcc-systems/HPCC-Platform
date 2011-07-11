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

inputDataset := dataset(['10','20','*',15,10,'+','-'], inputRecord) : stored('inputDataset');

firstState := dataset(row(stateRecord, { State:readNext });

StateCode := enum(StateReadNext, StateReduce);
 
stateRecord doReadNext(dataset(inputRecord) in, stateRecord l) := 
    transform
        nextValue = in[l.nextSymbol].value;
        nextCode = case(nextValue, '+'=>CodePlus, '-'=>CodeMinus, ''=>CodeEnd, CodeValue);
        self.values = left.values + IF(nextCode = CodeValue, dataset(row(valuerecord, {nextValue}))));
        self.state = CASE(nextCode, 
                        CodeValue=>StateReadNext, 
                        CodePlus=>StateReducePlus,
                        CodeMinus=>StateReduceMinus,
                        StateDone);
        self.nextSymbol = left.nextSymbol + 1;
        self := l;
    end;
 

stateRecord doReducePlus(dataset(inputRecord) in, stateRecord l) := 
    transform
        numValues := count(l.values);
        self.state := State:ReadNext;
        self.values := l.values[1..numValues-2] + row(valueRecord, { l.values[numValues]+l.values[numValues-1] });
        self := l;
    end;


processNext(dataset(inputRecord) in, dataset(stateRecord) state) :=
    caserow(state.curState,
            State:ReadNext=>project(state, doReadNext(in, LEFT)),
            State:ReducePlus=>project(state, doReducePlus(in, LEFT)),
            State:ReduceMinus=>project(state, doReduceMinus(in, LEFT)))

doProcess := loop(firstState, row,
                left.state != State:Done,
                processNext(inputDataset, rows(left)));


output(doProcess, {  values[1]; });

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

