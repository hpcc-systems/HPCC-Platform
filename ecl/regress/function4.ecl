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

//BUF: #13104 - binding parameter causes type of a transform record to change.

export BestByDate(unsigned6 myDid) := FUNCTION
rec := { string20 surname, unsigned6 did; };
ds10 := dataset('ds', rec, thor);
f := ds10(did = myDid);
t2 := table(f, { surname, did, myDid });

typeof(t2) t(t2 l) := transform
      SELF.surname := trim(l.surname) + 'x';
      SELF := l;
  END;

return project(t2, t(LEFT));

END;

output(BestByDate(000561683165));

