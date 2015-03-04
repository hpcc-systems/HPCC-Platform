/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems.

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

IMPORT redisServer FROM lib_redis;
IMPORT Std;

STRING server := '--SERVER=127.0.0.1:6379';
STRING password := 'foobared';
myRedis := redisServer(server, password);

myFuncStr(STRING key) := FUNCTION
 value := myRedis.GetString(key);
 return value;
END;
myFuncUtf8(STRING key) := FUNCTION
 value := myRedis.GetUtf8(key);
 return value;
END;
myFuncUni(STRING key) := FUNCTION
 value := myRedis.GetUnicode(key);
 return value;
END;

getString(STRING key, STRING key2, myFuncStr func) := FUNCTION
    value := myRedis.locking.GetString(key);
    RETURN IF (LENGTH(value) = 0, myRedis.locking.SetString(key, func(key2)), value);
END;
getUtf8(STRING key, STRING key2, myFuncUtf8 func) := FUNCTION
    value := myRedis.locking.GetUtf8(key);
    RETURN IF (LENGTH(value) = 0, myRedis.locking.SetUtf8(key, func(key2)), value);
END;
getUnicode(STRING key, STRING key2, myFuncUni func) := FUNCTION
    value := myRedis.locking.GetUnicode(key);
    RETURN IF (LENGTH(value) = 0, myRedis.locking.SetUnicode(key, func(key2)), value);
END;

//Test compatibiltiy between locking and normal functions.
SEQUENTIAL(
    myRedis.FlushDB(),
    myRedis.SetUtf8('utf8', U'אבגדהוזחטיךכלםמןנסעףפץצקרשת'),
    myRedis.GetUtf8('utf8'),
    myRedis.locking.GetUtf8('utf8'),
    myRedis.FlushDB(),
    myRedis.locking.SetUtf8('utf8-2', U'אבגדהוזחטיךכלםמןנסעףפץצקרשת'),
    myRedis.GetUtf8('utf8-2'),
    myRedis.locking.GetUtf8('utf8-2'),
    myRedis.FlushDB(),

    myRedis.SetUnicode('unicode', U'אבגדהוזחטיךכלםמןנסעףפץצקרשת'),
    myRedis.GetUnicode('unicode'),
    myRedis.locking.GetUnicode('unicode'),
    myRedis.FlushDB(),
    myRedis.locking.SetUnicode('unicode-2', U'אבגדהוזחטיךכלםמןנסעףפץצקרשת'),
    myRedis.GetUnicode('unicode-2'),
    myRedis.locking.GetUnicode('unicode-2'),
    myRedis.FlushDB()
    );

SEQUENTIAL(
    myRedis.FlushDB(),
    myRedis.SetUtf8('utf8-4', U'אבגדהוזחטיךכלםמןנסעףפץצקרשת'),
    myRedis.GetUtf8('utf8-4'),
    myRedis.locking.GetUtf8('utf8-4'),
    myRedis.FlushDB()
    );

SEQUENTIAL(
    myRedis.FlushDB(),
    myRedis.locking.SetUtf8('utf8-5', U'אבגדהוזחטיךכלםמןנסעףפץצקרשת'),
    myRedis.GetUtf8('utf8-5'),
    myRedis.locking.GetUtf8('utf8-5'),
    myRedis.FlushDB()
    );

SEQUENTIAL(
    myRedis.FlushDB(),
    myRedis.setUtf8('utf8-6', U'אבגדהוזחטיךכלםמןנסעףפץצקרשת'),
    getUtf8('utf8', 'utf8-6', myFuncUtf8),
);
SEQUENTIAL(
    myRedis.FlushDB(),
    myRedis.setUnicode('utf8-7', U'אבגדהוזחטיךכלםמןנסעףפץצקרשת'),
    getUnicode('utf8', 'utf8-7', myFuncUni),
);

SEQUENTIAL(
    myRedis.FlushDB(),
    myRedis.setString('Einnie sit', 'Good boy Einnie!'),
    getString('Einstein', 'Einnie sit', myFuncStr),

    myRedis.setString('Einstein', 'This way kido'),
    getString('Einstein', 'Einnie sit', myFuncStr),
);

//Test unlock
SEQUENTIAL(
    myRedis.FlushDB(),
    myRedis.locking.Getstring('testlock'),/*by default lock expires after 1s*/
    myRedis.exists('testlock'),
    Std.System.Debug.Sleep(2000);
    myRedis.exists('testlock'),

    myRedis.setString('testlock', 'redis_ecl_lock_blah_blah_blah'),
    myRedis.exists('testlock'),
    myRedis.locking.unlock('testlock'),
    myRedis.exists('testlock'),
    myRedis.FlushDB(),
    );
