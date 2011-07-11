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

A simplified PROCESS statement to allow efficient processing of tree structured input.

Only operations are

PUSH
POP
OUTPUT()
and REPLACE


*/


inputRecord := 
            record
string          next;
            end;

valueRecord :=
            record
integer         value;
            end;
    
inputDataset := dataset(['10','20','*',15,10,'+','-','.'], inputRecord);

processed := process(inputDataset, stackRecord, resultRecord,
                     CASE(LEFT.op, 
                            '+'=>REPLACE(transform(stackRecord, self.value := MATCHED[2].value+MATCHED[1].value), 2),
                            '-'=>REPLACE(transform(stackRecord, self.value := MATCHED[2].value-MATCHED[1].value), 2),
                            '*'=>REPLACE(transform(stackRecord, self.value := MATCHED[2].value*MATCHED[1].value), 2),
                            '~'=>REPLACE(transform(stackRecord, self.value := -RIGHT.value, 1),
                            '.'=>OUTPUT(RIGHT) POP(1),
                                 PUSH(transform(stackRecord, self.value := (integer)value));
                     );

output(processed);


