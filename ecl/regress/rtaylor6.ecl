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

layout := RECORD
String3 field1;
varstring256 field2;
varstring256 field3;
varstring16 field4;
varstring16 field5;
END;

layout tra(layout realdata) := TRANSFORM
Self.field1 := 'ZZZ';
Self.field2 := 'ZZZ-12345678';
Self.field3 := 'ZZZZ12345789';
Self.field4 := 'ZZZZZZ';
Self.field5 := '';
END;

SeedRec := dataset([{'ZZZ','ZZZ-12345678','ZZZZ12345789','ZZZZZZ',''}],layout);
fake_data := normalize(seedrec,88377,tra(Left)); 
output(fake_data,,'RTTEMP::fake_data',OVERWRITE);

foo := DATASET('RTTEMP::fake_data',layout,THOR); 
output(foo(field4='ZZZZZZ'));

