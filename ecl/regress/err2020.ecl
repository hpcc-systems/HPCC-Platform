/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

#onwarning (2387,warning);

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
    UNSIGNED1 order;
        UNSIGNED1 numRows;
        STRING20  thename;
        STRING20  addr;
END;

OutRec NormalizeAddresses(NamesRec L, INTEGER C) := TRANSFORM
      SELF := L;
      SELF.order := C;
      SELF.addr := CHOOSE(C, L.addr1, L.addr2, L.addr3, L.addr4);
    END;

NormalizeAddrs := NORMALIZE(namesTable, LEFT.numRows,
    NormalizeAddresses(LEFT,COUNT));

OUTPUT(NormalizeAddrs);
