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

SHARED rec := RECORD
  INTEGER1 num;
END;

export Interface_Def := INTERFACE
    EXPORT DATASET(rec) Records;
END;

ds1 := DATASET([{1},{2},{3}],rec);

context1 := MODULE(Interface_Def)
    EXPORT DATASET(rec) Records := ds1;
END;

OUTPUT(context1.Records,NAMED('context1'));

export Interface_Def2 := INTERFACE
    EXPORT GROUPED DATASET(rec) Records;
END;

ds2 := GROUP(SORTED(DATASET([{1},{2},{3}],rec),num),num);

// ----- This will not syntax check
// ----------- Gives error "Error: Explicit type for Records doesn't match definitin in base module (30,2)"
// ------------------ code 2346
context2 := MODULE(Interface_Def2)
    EXPORT GROUPED DATASET(rec) Records := ds2;
END;

OUTPUT(context2.Records,NAMED('context2'));
