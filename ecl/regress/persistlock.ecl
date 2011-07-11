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


export rtl := SERVICE
 unsigned4 display(const string src) : eclrtl,library='eclrtl',entrypoint='rtlDisplay';
 unsigned4 sleep(unsigned4 dxelay) : eclrtl,library='eclrtl',entrypoint='rtlSleep';
END;

namesRecord :=  RECORD
 unsigned4      f1;
 unsigned4      f2;
 unsigned4      f3;
END;

unsigned4 sleepTime := 10000;

namesTable2 := dataset([
        {rtl.display('Before Sleep 1'), rtl.sleep(sleepTime), rtl.display('done')+0},
        {rtl.display('Before Sleep 2'), rtl.sleep(sleepTime), rtl.display('done')+0},
        {rtl.display('Before Sleep 3'), rtl.sleep(sleepTime), rtl.display('done')+0},
        {rtl.display('Before Sleep 4'), rtl.sleep(sleepTime), rtl.display('done')+0}]
        , namesRecord);

x := namesTable2 : persist('namesTable');

count(x);
