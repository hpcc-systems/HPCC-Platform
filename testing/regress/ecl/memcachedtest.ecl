/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2014 HPCC Systems®.

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

//class=embedded
//class=3rdparty
//class=3rdpartyservice

IMPORT memcached FROM lib_memcached;
IMPORT Std;

STRING servers := '--SERVER=127.0.0.1:11211';
SEQUENTIAL(
    memcached.Clear(servers);

    memcached.SetBoolean('b', TRUE, servers);
    memcached.GetBoolean('b', servers);
    );

REAL pi := 3.14159265359;
SEQUENTIAL(
    memcached.SetReal('pi', pi, servers);
    memcached.GetReal('pi', servers);
    );

INTEGER i := 123456789;
SEQUENTIAL(
    memcached.SetInteger('i', i, servers);
    memcached.GetInteger('i', servers);
    );

UNSIGNED u := 7;
SEQUENTIAL(
    memcached.SetUnsigned('u', u, servers);
    memcached.GetUnsigned('u', servers);
    );

STRING str  := 'supercalifragilisticexpialidocious';
SEQUENTIAL(
    memcached.SetString('str', str, servers);
    memcached.GetString('str', servers);
    );

UNICODE uni := U'אבגדהוזחטיךכלםמןנסעףפץצקרשת';
SEQUENTIAL(
    memcached.SetUnicode('uni', uni, servers);
    memcached.GetUnicode('uni', servers);
    );

UTF8 utf := U'אבגדהוזחטיךכלםמןנסעףפץצקרשת';
SEQUENTIAL(
    memcached.SetUtf8('utf8', utf, servers);
    memcached.GetUtf8('utf8', servers);
    );

DATA mydata := x'd790d791d792d793d794d795d796d798d799d79ad79bd79cd79dd79dd79ed79fd7a0d7a1d7a2d7a3d7a4d7a5d7a6d7a7d7a8d7a9d7aa';
SEQUENTIAL(
    memcached.SetData('data', mydata, servers);
    memcached.GetData('data', servers);
    );

SEQUENTIAL(
    memcached.Exists('utf8', servers);
    memcached.KeyType('utf8', servers);
    );

//The following test some exceptions
SEQUENTIAL(
    memcached.GetInteger('pi', servers);
    memcached.Clear(servers);
    memcached.Exists('utf8', servers);
    memcached.KeyType('utf8', servers);
    );

SEQUENTIAL(
    memcached.Clear(servers);
    memcached.SetString('testExpire', 'foobar', servers,, 10);
    memcached.Exists('testExpire', servers);
    Std.System.Debug.Sleep(9 * 1000);
    memcached.Exists('testExpire', servers);
    Std.System.Debug.Sleep(2 * 1000);
    memcached.Exists('testExpire', servers);
    );

SEQUENTIAL(
    memcached.SetString('testDelete', 'foobar', servers);
    memcached.Exists('testDelete', servers);
    memcached.Delete('testDelete', servers);
    memcached.Exists('testDelete', servers);

    memcached.SetString('testDelete', 'foobar', servers, 'hashWithThis');
    memcached.Exists('testDelete', servers, 'hashWithThis');
    memcached.Delete('testDelete', servers, 'hashWithThis');
    memcached.Exists('testDelete', servers, 'hashWithThis');
    );
