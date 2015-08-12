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
