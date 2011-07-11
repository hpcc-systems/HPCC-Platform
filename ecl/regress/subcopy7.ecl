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

#option ('optimizeGraph', false);
//Project used inside a child dataset operation.
//First example should be more efficient since should assign directly to the target dataset
//second example will need to create a temporary row.

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


newChildRecord copyChild(childRecord l) := TRANSFORM
        SELF := l;
    END;

newParentRecord copyProjectChoose(parentRecord l) := 
TRANSFORM
    SELF.children := project(choosen(l.children, 2), copyChild(LEFT));
    SELF := l;
END;

output(PROJECT(parentDataset,copyProjectChoose(LEFT)),,'out1.d00');

newParentRecord copyChooseProject(parentRecord l) := 
TRANSFORM
    SELF.children := choosen(project(l.children, copyChild(LEFT)), 2);
    SELF := l;
END;

output(PROJECT(parentDataset,copyChooseProject(LEFT)),,'out2.d00');
