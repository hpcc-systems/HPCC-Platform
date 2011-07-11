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



filenameRecord := record
string10        name;
string8         size;
            end;

myService := service
dataset(filenameRecord) doDirectory(string dir) : entrypoint='doDirectory';
end;


dataset(filenameRecord) doDirectory(string dir) := beginC++
    __lenResult = 36;
    __result = malloc(36);
    memcpy(__result, "Gavin.hql 00000001Nigel     00050000", 36);
ENDC++;


output(doDirectory('c:\\'));
output(myService.doDirectory('c:\\'));
