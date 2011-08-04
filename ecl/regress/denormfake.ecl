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


/* Used to test denormalization in hthor - output record cannot change size */
householdRecord := RECORD
string4 house_id;
string20  address1;
string50  allNames := '';
    END;


personRecord := RECORD
string4 house_id;
string20  forename;
    END;

householdDataset := dataset([
        {'0001','166 Woodseer Street'},
        {'0002','10 Slapdash Lane'},
        {'0004','Buckingham Palace'},
        {'0005','Bomb site'}], householdRecord);

personDataset := dataset([
        {'0002','Spiders'},
        {'0001','Gavin'},
        {'0002','Gavin'},
        {'0002','Mia'},
        {'0003','Extra'},
        {'0001','Mia'},
        {'0004','King'},
        {'0004','Queen'}], personRecord);

householdRecord doDenormalize(householdRecord l, personRecord r) :=
                TRANSFORM
                    SELF.allNames := IF(l.allNames<>'', TRIM(l.allNames) + ',' + r.forename, r.forename);
                    SELF := l;
                END;


o := denormalize(householdDataset, personDataset, LEFT.house_id = RIGHT.house_id, doDenormalize(LEFT, RIGHT));

output(o,,'out.d00');
