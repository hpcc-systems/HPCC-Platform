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

import dt;

NameRecord :=
record
  string5    title;
  string20   fname;
  string20   mname;
  string20   lname;
  string5    name_suffix;
  string3    name_score;
end;

LocalAddrCleanLib := SERVICE
NameRecord dt(const string name, const string server = 'x') : c,entrypoint='aclCleanPerson73',pure;
END;


MyRecord := 
record
  unsigned id;
  string uncleanedName;
  NameRecord   Name;
end;

x := dataset('x', MyRecord, thor);

myRecord t(myRecord l) :=
    TRANSFORM
        SELF.Name := LocalAddrCleanLib.dt(l.uncleanedName);
        SELF := l;
    END;

y := project(x, t(LEFT));

output(y);

