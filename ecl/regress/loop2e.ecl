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

stepRecord :=
            record
string          next;
            end;

stackRecord :=
            record
unsigned        value;
            end;

stateRecord :=
            record
unsigned        step;
dataset(stackRecord) stack;
            end;


newStateRecord :=
            record
unsigned2       step;
dataset(stackRecord) stack;
            end;
actions := dataset(['10','20','*','15','10','+','-','.'], stepRecord);


newStateRecord processNext(stateRecord in) := TRANSFORM
    string next := actions[in.step].next;
    unsigned tos := count(in.stack);
    SELF.step := in.step+1;
    SELF.stack := CASE(next,
                       '+'=>in.stack[1..tos-2] + row(transform(stackRecord, self.value := in.stack[tos-1].value+in.stack[tos].value)),
                       '*'=>in.stack[1..tos-2] + row(transform(stackRecord, self.value := in.stack[tos-1].value*in.stack[tos].value)),
                       '-'=>in.stack[1..tos-2] + row(transform(stackRecord, self.value := in.stack[tos-1].value-in.stack[tos].value)),
                       '~'=>in.stack[1..tos-1] + row(transform(stackRecord, self.value := -in.stack[tos].value)),
                       in.stack + row(transform(stackRecord, self.value := (integer)next)));
    END;


initial := dataset([{1,[]}], stateRecord);

result := LOOP(initial, actions[left.step].next != '.', project(rows(left), processNext(LEFT)));

output(result[1].stack[1].value);
