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

export SimpleRepro () := function
    STRING inWuid := '' : STORED('workUnitId');
    string declaredWuid := 'W20090604-173624'; 
    ds := dataset(workunit(inWuid,0), {STRING Value1}); // this gets a compile error
    // ds := dataset(workunit(declaredWuid,0), {STRING Value1});  //this works
    return sequential(output(ds));
end;


SimpleRepro();
