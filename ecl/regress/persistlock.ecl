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
