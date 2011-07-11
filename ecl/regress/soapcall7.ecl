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

outRecord := 
RECORD
string500 Current_Did{xpath('Current_Did')};
END;

inRecord :=
RECORD
unsigned6 olddid;
boolean glb;
boolean bestappend;
END;

d := dataset([{ 93191060, true, true }], inRecord);

e := SOAPCALL(d, '127.0.0.1:9876', 'didville.did_did_update_service',inRecord, DATASET(outRecord));

output(e);
