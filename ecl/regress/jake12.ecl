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

import std.system.thorlib;

namesRecord := RECORD
string3     id;
string10    surname ;
string10    forename;
string2     nl;
  END;

natRecord := RECORD
string3     id;
string10    nationality ;
string2     nl;
  END;

nameAndNatRecord := RECORD
string3     id := 1;
string10    surname := '' ;
string10    forename := '';
string10    nationality := '' ;
string2     nl := '';
  END;

names := DATASET('in.d00', namesRecord, FLAT);
nationalities := DATASET('nat.d00', natRecord, FLAT);

namesRecord JoinTransform (namesRecord l, namesRecord r)
:=
    TRANSFORM
                   self.forename := thorlib.logicalToPhysical('testc.d00', true);
                   self := r;
    END;

snames := ITERATE(names, JoinTransform(LEFT,RIGHT));

output(snames, , 'out.d00');
