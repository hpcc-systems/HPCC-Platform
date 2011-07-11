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

inputFile := dataset('input', {string9 ssn, string9 did, string20 addr},thor);


keyedFile := dataset('keyed', {string9 ssn, string9 did, string20 surname, string20 forename}, thor, encrypt('myKey'));
SsnKey := INDEX(keyedFile, {ssn,unsigned8 fpos {virtual(fileposition)}}, '~index.ssn');
DidKey := INDEX(keyedFile, {did,unsigned8 fpos {virtual(fileposition)}}, '~index.did');

filledRec :=   RECORD
        string9 ssn;
        string9 did;
        string20 addr;
        string20 surname;
        string20 forename;
    END;

filledRec getNames(inputFile l, keyedFile r) := TRANSFORM
        SELF := l;
        SELF := r;
    END;

KeyedTable := keyed(keyedFile, SsnKey, DidKey);

FilledRecs := join(inputFile, KeyedTable,left.did=right.did,getNames(left,right), KEYED(DidKey));
output(FilledRecs);

