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



integer MyLib(integer dummy) := Service  // service can not have parm and type
   // shared or export is not allowed
   SHARED String gx(String X);
   // duplicated func
   String gx(String X) : entrypoint = rtlF;
   String h(String X) : c,entrypoint = 'rtlF';
   String i(String X) : c,library='dab',entrypoint = 'rtlF';
   String j(String X) : c,eclrtl,library='dab',entrypoint = 'rtlF';
   // bad entrypoint
   String k1(String X) : entrypoint;
   String k2(String X) : entrypoint='';
   String k3(String X) : entrypoint='a c';
   String k4(String X) : entrypoint='2ac';

   // bad initfunc
   String k1x(String X) : initfunction;
   String k2x(String X) : initfunction='';
   String k3x(String X) : initfunction='a c';
   String k4x(String X) : initfunction='2ac';

   //ok
   String k5(String X) : entrypoint='_2ac';
   String k6(String X) : entrypoint=_2ac;
   // dup attr
   String m1(String X) : c,Entrypoint='rtlM',library,c,entrypoint;
   // bad library
   String m2(String X) : entrypoint='rtlM',library;
   String m3(String X) : entrypoint='rtlM',library='';
   // bad include
   String m4(String X) : entrypoint='rtlM',include;
   String m5(String X) : entrypoint='rtlM',include='';

   // warning
   String n1(String X) : entrypoint='rtlM', c = 'extern';
   String n2(String X) : entrypoint='rtlM', eclrtl = 'true';
   String n3(String X) : entrypoint='rtlM', bcd = '1';
   String n4(String X) : entrypoint='rtlM', newattr;
   String n5(String X) : entrypoint='rtlM', newattr2 = '1';
END;

gx := 3;
