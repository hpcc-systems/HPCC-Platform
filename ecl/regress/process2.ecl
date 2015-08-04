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

