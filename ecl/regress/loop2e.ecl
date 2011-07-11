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
