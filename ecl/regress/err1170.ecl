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

NamesRec := RECORD
    UNSIGNED1 numRows := 0;
    STRING20  thename;
    STRING20  addr1 := '';
    STRING20  addr2 := '';
    STRING20  addr3 := '';
    STRING20  addr4 := '';
END;

NamesTable := DATASET([
    {1, 'Gavin', '10 Slapdash Lane'},
    {2, 'Mia', '10 Slapdash Lane', '3 The Cottages'},
    {0, 'Mr Nobody'},
    {4, 'Mr Everywhere', 'Here', 'There', 'Near', 'Far'}
    ], NamesRec);

OutRec := RECORD
    UNSIGNED1 numRows;
    STRING20  thename;
//  STRING20  addr;
END;


OutRec NormalizeAddresses(NamesRec L, INTEGER C) :=
    TRANSFORM
      SELF := L;
//      SELF.addr := CHOOSE(C, L.addr1, L.addr2, L.addr3, L.addr4);
    END;

/*
NormalizeAddrs := NORMALIZE(namesTable, LEFT.numRows,
    NormalizeAddresses(LEFT,COUNTER));

//OUTPUT(NormalizeAddrs);

OUTPUT(NamesTable, OutRec);

*/

/*
NamesRec := RECORD
    UNSIGNED1 numRows := 0;
    STRING20  thename;
    STRING20  addr1 := '';
    STRING20  addr2 := '';
    STRING20  addr3 := '';
    STRING20  addr4 := '';
END;

NamesTable := DATASET([
    {1, 'Gavin', '10 Slapdash Lane'},
    {2, 'Mia', '10 Slapdash Lane', '3 The Cottages'},
    {0, 'Mr Nobody'},
    {4, 'Mr Everywhere', 'Here', 'There', 'Near', 'Far'}
    ], NamesRec);


OutRec := RECORD
    UNSIGNED1 numRows;
    STRING20  thename;
    STRING20  addr;
END;

OutRec NormalizeAddresses(NamesRec L, INTEGER C) :=
    TRANSFORM
//      SELF := L;
//      SELF.addr := CHOOSE(C, L.addr1, L.addr2, L.addr3, L.addr4);
    END;

//NormalizeAddrs := NORMALIZE(namesTable, LEFT.numRows,
//    NormalizeAddresses(LEFT,COUNTER));

//OUTPUT(NormalizeAddrs);

//OUTPUT(NamesTable, OutRec);
*/
