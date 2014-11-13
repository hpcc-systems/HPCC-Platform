/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2014 HPCC Systems.

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

IMPORT * FROM memcached.memcached;

STRING servers := '--SERVER=127.0.0.1:11211';
MClear(servers);

MSetBoolean(servers, 'b', TRUE);
MGetBoolean(servers, 'b');

REAL pi := 3.14159265359;
MSetReal(servers, 'pi', pi);
MGetReal(servers, 'pi');

INTEGER i := 12345689;
MSetInteger(servers, 'i', i);
MGetInteger(servers, 'i');

UNSIGNED u := 7;
MSetUnsigned(servers, 'u', u);
MGetUnsigned(servers, 'u');

STRING str  := 'supercalifragilisticexpialidocious';
MSetString(servers, 'str', str);
MGetString(servers, 'str');

UNICODE uni := U'אבגדהוזחטיךכלםמןנסעףפץצקרשת';
MSetUnicode(servers, 'uni', uni);
MGetUnicode(servers, 'uni');

UTF8 utf := U'אבגדהוזחטיךכלםמןנסעףפץצקרשת';
MSetUtf8(servers, 'utf8', utf);
MGetUtf8(servers, 'utf8');

DATA mydata := x'd790d791d792d793d794d795d796d798d799d79ad79bd79cd79dd79dd79ed79fd7a0d7a1d7a2d7a3d7a4d7a5d7a6d7a7d7a8d7a9d7aa';
MSetData(servers, 'data', mydata);
MGetData(servers,'data');

MExist(servers, 'utf8');
MKeyType(servers,'utf8');

//The following test some exceptions
MGetInteger(servers, 'pi');
MClear(servers);
MExist(servers, 'utf8');
MKeyType(servers,'utf8');
