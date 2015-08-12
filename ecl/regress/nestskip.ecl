/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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

