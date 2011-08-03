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


nameRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

addressRecord := record
string          address;
string8         postcode;
string16        phone;
dataset(nameRecord) occupants;
            end;



addressTable := dataset('x',addressRecord,FLAT);


addressRecord t(addressRecord l) := transform
    nameRecord tname(nameRecord lname) := transform,skip(lname.age != 12)
        self.age := lname.age + 1;
        self := lname;
    end;

    self.occupants := project(l.occupants, tname(left));
    self := l;
end;


//output(project(addressTable, t(LEFT)));

addressRecord t2(addressRecord l) := transform
    nameRecord tname(nameRecord lname) := transform
        self.age := if(lname.age = 11, skip, lname.age + 1);
        self := lname;
    end;

    self.occupants := project(l.occupants, tname(left));
    self := l;
end;


//output(project(addressTable, t2(LEFT)));


addressRecord t3(addressRecord l) := transform
    nameRecord tname(nameRecord lname) := transform,skip(lname.age != 12)
        self.age := lname.age + 1;
        self := lname;
    end;

    self.occupants := nofold(project(l.occupants, tname(left)))(age != 10);
    self := l;
end;


output(project(addressTable, t3(LEFT)));

addressRecord t4(addressRecord l) := transform
    nameRecord tname(nameRecord lname) := transform
        self.age := if(lname.age = 11, skip, lname.age + 1);
        self := lname;
    end;

    self.occupants := nofold(project(l.occupants, tname(left)))(age != 11);
    self := l;
end;


output(project(addressTable, t4(LEFT)));

