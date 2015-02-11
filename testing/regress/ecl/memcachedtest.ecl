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

IMPORT memcached FROM lib_memcached;

STRING servers := '--SERVER=127.0.0.1:11211';
memcached.Clear(servers);

memcached.SetBoolean(servers, 'b', TRUE);
memcached.GetBoolean(servers, 'b');

REAL pi := 3.14159265359;
memcached.SetReal(servers, 'pi', pi);
memcached.GetReal(servers, 'pi');

INTEGER i := 123456789;
memcached.SetInteger(servers, 'i', i);
memcached.GetInteger(servers, 'i');

UNSIGNED u := 7;
memcached.SetUnsigned(servers, 'u', u);
memcached.GetUnsigned(servers, 'u');

STRING str  := 'supercalifragilisticexpialidocious';
memcached.SetString(servers, 'str', str);
memcached.GetString(servers, 'str');

UNICODE uni := U'אבגדהוזחטיךכלםמןנסעףפץצקרשת';
memcached.SetUnicode(servers, 'uni', uni);
memcached.GetUnicode(servers, 'uni');

UTF8 utf := U'אבגדהוזחטיךכלםמןנסעףפץצקרשת';
memcached.SetUtf8(servers, 'utf8', utf);
memcached.GetUtf8(servers, 'utf8');

DATA mydata := x'd790d791d792d793d794d795d796d798d799d79ad79bd79cd79dd79dd79ed79fd7a0d7a1d7a2d7a3d7a4d7a5d7a6d7a7d7a8d7a9d7aa';
memcached.SetData(servers, 'data', mydata);
memcached.GetData(servers,'data');

memcached.Exists(servers, 'utf8');
memcached.KeyType(servers,'utf8');

//The following test some exceptions
memcached.GetInteger(servers, 'pi');
memcached.Clear(servers);
memcached.Exists(servers, 'utf8');
memcached.KeyType(servers,'utf8');
