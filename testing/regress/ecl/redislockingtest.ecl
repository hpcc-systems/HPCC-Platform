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
    value := myRedis.GetOrLockString(key);
    RETURN IF (LENGTH(value) = 0, myRedis.SetAndPublishString(key, func(key2)), value);
END;
getUtf8(STRING key, STRING key2, myFuncUtf8 func) := FUNCTION
    value := myRedis.GetOrLockUtf8(key);
    RETURN IF (LENGTH(value) = 0, myRedis.SetAndPublishUtf8(key, func(key2)), value);
END;
getUnicode(STRING key, STRING key2, myFuncUni func) := FUNCTION
    value := myRedis.GetOrLockUnicode(key);
    RETURN IF (LENGTH(value) = 0, myRedis.SetAndPublishUnicode(key, func(key2)), value);
END;

//Test compatibiltiy between locking and normal functions.
SEQUENTIAL(
    myRedis.FlushDB(),
    myRedis.SetUtf8('utf8', U'אבגדהוזחטיךכלםמןנסעףפץצקרשת'),
    myRedis.GetUtf8('utf8'),
    myRedis.GetOrLockUtf8('utf8'),
    myRedis.FlushDB(),
    myRedis.SetAndPublishUtf8('utf8-2', U'אבגדהוזחטיךכלםמןנסעףפץצקרשת'),
    myRedis.GetUtf8('utf8-2'),
    myRedis.GetOrLockUtf8('utf8-2'),
    myRedis.FlushDB(),

    myRedis.SetUnicode('unicode', U'אבגדהוזחטיךכלםמןנסעףפץצקרשת'),
    myRedis.GetUnicode('unicode'),
    myRedis.GetOrLockUnicode('unicode'),
    myRedis.FlushDB(),
    myRedis.SetAndPublishUnicode('unicode-2', U'אבגדהוזחטיךכלםמןנסעףפץצקרשת'),
    myRedis.GetUnicode('unicode-2'),
    myRedis.GetOrLockUnicode('unicode-2'),
    myRedis.FlushDB()
    );

SEQUENTIAL(
    myRedis.FlushDB(),
    myRedis.SetUtf8('utf8-4', U'אבגדהוזחטיךכלםמןנסעףפץצקרשת'),
    myRedis.GetUtf8('utf8-4'),
    myRedis.GetOrLockUtf8('utf8-4'),
    myRedis.FlushDB()
    );

SEQUENTIAL(
    myRedis.FlushDB(),
    myRedis.SetAndPublishUtf8('utf8-5', U'אבגדהוזחטיךכלםמןנסעףפץצקרשת'),
    myRedis.GetUtf8('utf8-5'),
    myRedis.GetOrLockUtf8('utf8-5'),
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
    myRedis.GetOrLockString('testlock'),/*by default lock expires after 1s*/
    myRedis.Exists('testlock'),
    Std.System.Debug.Sleep(2000),
    myRedis.Exists('testlock'),

    myRedis.SetString('testlock', 'redis_ecl_lock_blah_blah_blah'),
    myRedis.Exists('testlock'),
    myRedis.Unlock('testlock'),
    myRedis.Exists('testlock'),
    myRedis.FlushDB(),
    );
