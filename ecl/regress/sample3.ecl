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

//output(SAMPLE(TrainingKevinLogemann.File_People_Slim,100,1));
//output(Enth(TrainingKevinLogemann.File_People_Slim,100));
//output(choosen(TrainingKevinLogemann.File_People_Slim,100));
//output(TrainingKevinLogemann.File_People_Slim);

#option ('applyInstantEclTransformations', true);
#option ('applyInstantEclTransformationsLimit', 999);

namesRecord := 
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

File_People_Slim := dataset('x',namesRecord,FLAT);

output(SAMPLE(File_People_Slim,100,1));
output(choosen(File_People_Slim,100));
output(File_People_Slim);
