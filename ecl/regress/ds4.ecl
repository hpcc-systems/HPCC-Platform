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

export childPersonRecord := RECORD
string20          forename;
unsigned1         age;
    END;

export personRecord :=
                RECORD
string20            forenamex;
string20            surname;
DATASET(childPersonRecord)   children;
                END;


export testService :=   service
sendDataset(dataset src) :                              eclrtl,pure,library='eclrtl',entrypoint='sendDataset';
dataset dataset2DatasetX(const dataset src) :                   eclrtl,pure,library='eclrtl',entrypoint='rtlStrToDataX';
                end;

#option ('targetClusterType', 'roxie');



personDataset := DATASET(
            [{'Gavin','Hawthorn',[{'Abigail',2},{'Nathan',2}]},
             {'John','Simmons',[{'Jennifer',18},{'Alison',16},{'Andrew',13},{'Fiona',10}]}],personRecord);

processed := testService.dataset2datasetX(personDataset);

testService.sendDataset(processed);

//output(getPeople());
