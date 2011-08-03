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

childRecord := RECORD
unsigned4 person_id;
dt.pstring per_forename;
string20  per_surname;
unsigned8 holepos;
    END;

parentRecord :=
                RECORD
unsigned8           id;
string20            address1;
string20            address2;
string20            address3;
unsigned2           numPeople;
DATASET(childRecord)   children;
string10            postcode;
                END;

parentDataset := DATASET('test',parentRecord,FLAT);


newChildRecord := RECORD
unsigned4 person_id;
string20  per_surname;
    END;

newParentRecord :=
                RECORD
unsigned8           id;
string20            address1;
string20            address2;
string20            address3;
unsigned2           numPeople;
DATASET(newChildRecord)   children;
string10            postcode;
                END;


//MORE: Really need a way of passing dataset parameters...
doProjectParent(unsigned8 idAdjust, unsigned8 numChildren) :=
FUNCTION
    newParentRecord copyChooseProject(parentRecord l) :=
        TRANSFORM

            doProjectChild(unsigned8 idAdjust2) :=
            FUNCTION
                newChildRecord copyChild(childRecord l) :=
                    TRANSFORM
                        SELF.person_id := l.person_id + idAdjust2;
                        SELF := l;
                    END;
                RETURN project(choosen(l.children, numChildren), copyChild(LEFT));
            END;

            SELF.children := doProjectChild(l.id);
            SELF.id := l.id + idAdjust;
            SELF := l;
        END;

    RETURN project(parentDataset,copyChooseProject(LEFT));
END;


output(doProjectParent(10, 2),,'out1.d00');
