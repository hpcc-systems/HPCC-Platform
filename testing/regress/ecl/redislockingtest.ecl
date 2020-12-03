/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems®.

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

//nohthor

IMPORT * FROM lib_redis;
IMPORT Std;

STRING server := '--SERVER=127.0.0.1:6379';
STRING password := 'foobared';
// The database is need to avoid crossover manipulation when multiple redis tests
// executed in parallel
INTEGER4 database := 10;
myRedis := redisServer(server, password);

myFuncStr(STRING key) := FUNCTION
 value := myRedis.GetString(key, database);
 return value;
END;
myFuncUtf8(STRING key) := FUNCTION
 value := myRedis.GetUtf8(key, database);
 return value;
END;
myFuncUni(STRING key) := FUNCTION
 value := myRedis.GetUnicode(key, database);
 return value;
END;

getString(STRING key, STRING key2, myFuncStr func) := FUNCTION
    value := myRedis.GetOrLockString(key, database);
    RETURN IF (LENGTH(value) = 0, myRedis.SetAndPublishString(key, func(key2), database), value);
END;
getUtf8(STRING key, STRING key2, myFuncUtf8 func) := FUNCTION
    value := myRedis.GetOrLockUtf8(key, database);
    RETURN IF (LENGTH(value) = 0, myRedis.SetAndPublishUtf8(key, func(key2), database), value);
END;
getUnicode(STRING key, STRING key2, myFuncUni func) := FUNCTION
    value := myRedis.GetOrLockUnicode(key, database);
    RETURN IF (LENGTH(value) = 0, myRedis.SetAndPublishUnicode(key, func(key2), database), value);
END;

//Test compatibiltiy between locking and normal functions.
SEQUENTIAL(
    myRedis.FlushDB( database),
    myRedis.SetUtf8('utf8', U'אבגדהוזחטיךכלםמןנסעףפץצקרשת', database),
    myRedis.GetUtf8('utf8', database),
    myRedis.GetOrLockUtf8('utf8', database),
    myRedis.FlushDB(database),
    myRedis.SetAndPublishUtf8('utf8-2', U'אבגדהוזחטיךכלםמןנסעףפץצקרשת', database),
    myRedis.GetUtf8('utf8-2', database),
    myRedis.GetOrLockUtf8('utf8-2', database),
    myRedis.FlushDB(database),

    myRedis.SetUnicode('unicode', U'אבגדהוזחטיךכלםמןנסעףפץצקרשת', database),
    myRedis.GetUnicode('unicode', database),
    myRedis.GetOrLockUnicode('unicode', database),
    myRedis.FlushDB(database),
    myRedis.SetAndPublishUnicode('unicode-2', U'אבגדהוזחטיךכלםמןנסעףפץצקרשת', database),
    myRedis.GetUnicode('unicode-2', database),
    myRedis.GetOrLockUnicode('unicode-2', database),
    myRedis.FlushDB(database)
    );

SEQUENTIAL(
    myRedis.FlushDB(database),
    myRedis.SetUtf8('utf8-4', U'אבגדהוזחטיךכלםמןנסעףפץצקרשת', database),
    myRedis.GetUtf8('utf8-4', database),
    myRedis.GetOrLockUtf8('utf8-4', database),
    myRedis.FlushDB(database)
    );

SEQUENTIAL(
    myRedis.FlushDB(database),
    myRedis.SetAndPublishUtf8('utf8-5', U'אבגדהוזחטיךכלםמןנסעףפץצקרשת', database),
    myRedis.GetUtf8('utf8-5', database),
    myRedis.GetOrLockUtf8('utf8-5', database),
    myRedis.FlushDB(database)
    );

SEQUENTIAL(
    myRedis.FlushDB(database),
    myRedis.setUtf8('utf8-6', U'אבגדהוזחטיךכלםמןנסעףפץצקרשת', database),
    getUtf8('utf8', 'utf8-6', myFuncUtf8),
);
SEQUENTIAL(
    myRedis.FlushDB(database),
    myRedis.setUnicode('utf8-7', U'אבגדהוזחטיךכלםמןנסעףפץצקרשת', database),
    getUnicode('utf8', 'utf8-7', myFuncUni),
);

SEQUENTIAL(
    myRedis.FlushDB(database),
    myRedis.setString('Einnie sit', 'Good boy Einnie!', database),
    getString('Einstein', 'Einnie sit', myFuncStr),

    myRedis.setString('Einstein', 'This way kido', database),
    getString('Einstein', 'Einnie sit', myFuncStr),
);

//Test unlock
SEQUENTIAL(
    myRedis.FlushDB(database),
    myRedis.GetOrLockString('testlock', database, 1000),
    myRedis.Exists('testlock', database),
    Std.System.Debug.Sleep(2000),
    myRedis.Exists('testlock', database),

    myRedis.SetString('testlock', 'redis_ecl_lock_blah_blah_blah', database),
    myRedis.Exists('testlock', database),
    myRedis.Unlock('testlock', database),
    myRedis.Exists('testlock', database),
    myRedis.FlushDB(database),
    );

SEQUENTIAL(
    myRedis.FlushDB(database + 1);
    myRedis.SetAndPublishString('testDatabaseExpire1', 'databaseThenExpire', database + 1, 10000);
    myRedis.GetString('testDatabaseExpire1', database + 1);
    myRedis.GetOrLockString('testDatabaseExpire1', database + 1);
    myRedis.FlushDB(database + 1);
    );

SEQUENTIAL(
    myRedis.FlushDB(database + 2);
    myRedis.SetAndPublishUnicode('testDatabaseExpire2', 'databaseThenExpire', database + 2, 10000);
    myRedis.GetUnicode('testDatabaseExpire2', database + 2);
    myRedis.GetOrLockUnicode('testDatabaseExpire2', database + 2);
    myRedis.FlushDB(database + 2);
    );

SEQUENTIAL(
    myRedis.FlushDB(database + 3);
    myRedis.SetAndPublishUtf8('testDatabaseExpire3', 'databaseThenExpire', database + 3, 10000);
    myRedis.GetUtf8('testDatabaseExpire3', database + 3);
    myRedis.GetOrLockUtf8('testDatabaseExpire3', database + 3);
    myRedis.FlushDB(database + 3);
    );

SEQUENTIAL(
    myRedis.FlushDB(database),
    myRedis.SetAndPublishString('t1', 'Good boy Einnie!', database);
    myRedis.GetString('t1', database);

    myRedis.SetAndPublishString('t2', 'Good boy Einnie!', database + 1, 10000);
    myRedis.GetString('t2', database + 1);

    myRedis.SetAndPublishString('t3', 'supercalifragilisticexpialidocious', database);
    myRedis.GetString('t3', database);

    myRedis.SetAndPublishString('t4', 'supercalifragilisticexpialidocious', database + 1, 10000);
    myRedis.GetString('t4', database + 1);

    myRedis.FlushDB(database);
    myRedis.FlushDB(database + 1);
    );

myRedis.FlushDB(database);
myRedis.FlushDB(database + 1);
myRedis.FlushDB(database + 2);
myRedis.FlushDB(database + 3);
