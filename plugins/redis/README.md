ECL Redis Plugin
================

This is the ECL plugin to utilize the persistent key-value cache [Redis](http://redis.io).
It is based upon the C API [hiredis](http://github.com/redis/hiredis).

Installation and Dependencies
----------------------------

To build the redis plugin with the HPCC-Platform, libhiredis-dev is required.
```
sudo apt-get install libhiredis-dev
```

The redis server and client software can be obtained via either - [binaries](http://redis.io/download), [source](https://github.com/antirez/redis) or the preferred method:
```
sudo apt-get redis-server
```

Getting started
---------------

The server can be started by typing `redis-server` within a terminal. To run with with a non-default configuration run as `redis-server redis.conf`, where
redis.conf is the configuration file supplied with the redis-server package.

For example, to require the server to **password authenticate**, locate and copy redis.conf to a desired dir. Then locate and alter the 'requirepass' variable within the file.
Similarly the server **port** can also be altered here. *Note* that the default is 6379 and that if multiple and individual caches are required then they are by definition redis-servers
on different ports.

The **redis-server** package comes with the redis client **redis-cli**. This can be used to send and receive commands to and from the server, invoked by `redis-cli` or, for example,
`redis-cli -p 6380` to connect to the redis-cache on port 6380 (assuming one has been started).

Perhaps on of the most handy uses of **redis-cli** is the ability to monitor all commands issued to the server via the redis command `MONITOR`. `INFO ALL` is also a useful command
for listing the server and cache settings and statistics. *Note* that if **requirepass** is activated **redis-cli** with require you to authenticate via `AUTH <passcode>`.

Further [documentation](http://redis.io/documentation) is available with a full list of redis [commands](http://redis.io/commands).

The Actual Plugin
-----------------

The bulk of this redis plugin for **ECL** is made up of the various `SET` and `GET` commands e.g. `GetString` or `SetReal`. They are accessible via the module `sync` from the redis plugin **ECL** library `lib-redis`. i.e.
```
IMPORT sync FROM lib_redis;
```
Here is a list of the core plugin **functions**.

###Set
```
SetUnicode( CONST VARSTRING key, CONST UNICODE value, CONST VARSTRING options, UNSIGNED database = 0, UNSIGNED4 expire = 0, CONST VARSTRING password = '', UNSIGNED timeout = 1000000)
SetString(  CONST VARSTRING key, CONST STRING value,  CONST VARSTRING options, UNSIGNED database = 0, UNSIGNED4 expire = 0, CONST VARSTRING password = '', UNSIGNED timeout = 1000000)
SetUtf8(    CONST VARSTRING key, CONST UTF8 value,    CONST VARSTRING options, UNSIGNED database = 0, UNSIGNED4 expire = 0, CONST VARSTRING password = '', UNSIGNED timeout = 1000000)
SetBoolean( CONST VARSTRING key, BOOLEAN value,       CONST VARSTRING options, UNSIGNED database = 0, UNSIGNED4 expire = 0, CONST VARSTRING password = '', UNSIGNED timeout = 1000000)
SetReal(    CONST VARSTRING key, REAL value,          CONST VARSTRING options, UNSIGNED database = 0, UNSIGNED4 expire = 0, CONST VARSTRING password = '', UNSIGNED timeout = 1000000)
SetInteger( CONST VARSTRING key, INTEGER value,       CONST VARSTRING options, UNSIGNED database = 0, UNSIGNED4 expire = 0, CONST VARSTRING password = '', UNSIGNED timeout = 1000000)
SetUnsigned(CONST VARSTRING key, UNSIGNED value,      CONST VARSTRING options, UNSIGNED database = 0, UNSIGNED4 expire = 0, CONST VARSTRING password = '', UNSIGNED timeout = 1000000)
SetData(    CONST VARSTRING key, CONST DATA value,    CONST VARSTRING options, UNSIGNED database = 0, UNSIGNED4 expire = 0, CONST VARSTRING password = '', UNSIGNED timeout = 1000000)
```

###Get
```
INTEGER8   GetInteger(CONST VARSTRING key, CONST VARSTRING options, UNSIGNED database = 0, CONST VARSTRING password = '', UNSIGNED timeout = 1000000)
UNSIGNED8 GetUnsigned(CONST VARSTRING key, CONST VARSTRING options, UNSIGNED database = 0, CONST VARSTRING password = '', UNSIGNED timeout = 1000000)
STRING      GetString(CONST VARSTRING key, CONST VARSTRING options, UNSIGNED database = 0, CONST VARSTRING password = '', UNSIGNED timeout = 1000000)
UNICODE    GetUnicode(CONST VARSTRING key, CONST VARSTRING options, UNSIGNED database = 0, CONST VARSTRING password = '', UNSIGNED timeout = 1000000)
UTF8          GetUtf8(CONST VARSTRING key, CONST VARSTRING options, UNSIGNED database = 0, CONST VARSTRING password = '', UNSIGNED timeout = 1000000)
BOOLEAN    GetBoolean(CONST VARSTRING key, CONST VARSTRING options, UNSIGNED database = 0, CONST VARSTRING password = '', UNSIGNED timeout = 1000000)
REAL          GetReal(CONST VARSTRING key, CONST VARSTRING options, UNSIGNED database = 0, CONST VARSTRING password = '', UNSIGNED timeout = 1000000)
DATA          GetData(CONST VARSTRING key, CONST VARSTRING options, UNSIGNED database = 0, CONST VARSTRING password = '', UNSIGNED timeout = 1000000)
```

###Utility
```
BOOLEAN Exists(CONST VARSTRING key, CONST VARSTRING options, UNSIGNED database = 0, CONST VARSTRING password = '', UNSIGNED timeout = 1000000)
FlushDB(CONST VARSTRING options, UNSIGNED database = 0, CONST VARSTRING password = '', UNSIGNED timeout = 1000000)
Del(CONST VARSTRING key, CONST VARSTRING options, UNSIGNED database = 0, CONST VARSTRING password = '', UNSIGNED timeout = 1000000)
Persist(CONST VARSTRING key, CONST VARSTRING options, UNSIGNED database = 0, CONST VARSTRING password = '', UNSIGNED timeout = 1000000)
Expire(CONST VARSTRING key, CONST VARSTRING options, UNSIGNED database = 0, UNSIGNED4 expire, CONST VARSTRING password = '', UNSIGNED timeout = 1000000)
INTEGER DBSize(CONST VARSTRING options, UNSIGNED database = 0, CONST VARSTRING password = '', UNSIGNED timeout = 1000000)
```

The core points to note here are:
   * There is a **SET** and **GET** function associated with each fundamental **ECL** type. These must be used for and with their correct *value* types! Miss-use *should* result in an runtime exception, however, this is only conditional on having the value retrieved from the server fitting into memory of the requested type. E.g. it is possible for a STRING of length 8, set with SetString, being successfully retrieved from the cache via GetInteger without an **ECL** exception being thrown.
   * `CONST VARSTRING options` passes the server **IP** and **port** to the plugin in the *strict* format - `--SERVER=<ip>:<port>`
   * `UNSIGNED timeout` has units micro seconds and has a default value of 1 second.

###The redisServer MODULE
To avoid the combersom and unnecessary need to constantly pass `options` and `password` with each function call, the module `redisServer` can be imported to effectively 
*wrap* the above functions.
```
IMPORT redisServer FROM lib_redis;
myRedis := redisServer('--SERVER=127.0.0.1:6379', 'foobared');
myRedis.SetString('myKey', 'supercalifragilisticexpialidocious');
myRedis.GetString('myKey');
```

###A Redis *Database*
The notion of a *database* within a redis cache is a that of an UNSIGNED INTEGER, effectively partitioning the cache such that it may contain an identical key per database e.g.
```
myRedis.SetString('myKey', 'foo', 0);
myRedis.SetString('myKey', 'bar', 1);

myRedis.GetString('myKey', 'foo', 0);//returns 'foo'
myRedis.GetString('myKey', 'bar', 1);//returns 'bar'
```
*Note* that the default database is 0.


Race Retrieval and Locking Keys
-------------------------------
A common use of external caching systems such as **redis** is for temporarily storing data that may be expensive, computationally or otherwise, to obtain and thus doing so *only once* is paramount. In such a scenario it is possible (in cases usual) for multiple clients/requests to *hit* the cache simultaneously and upon finding that the data requested has not yet been stored, it is desired that only one of such requests obtain the new value and then store it for the others to then also obtain (from the cache). This plugin offers a solution to such a problem via the `locking` MODULE within the `redisServer` MODULE. This module contains only four function categories - each of the above listed `SET` and `GET` functions, a unique `Exists` function and lastly an auxiliary function `Unlock` used to manually unlock locked keys as it be discussed.

The principle here is based around a *cache miss* in which a requested key does not exist, the first requester (*race winner*) 'locks' the key in an atomic fashion. Any other simultaneous requester (*race loser*) finds that the key exists but has been locked and thus **SUBSCRIBES** to the key awaiting a **PUBLICATION** message from the *race-winner* that the value has been set. Such a paradigm is well suited by redis due to its efficiently implemented **PUB-SUB** infrastructure.

###An ECL Example
```
IMPORT redisServer FROM lib_redis
myRedis := redisServer('--SERVER=127.0.0.0:3679');

REAL pi := 3.14159265359;  //Value to externally compute/retrieve from 3rd party vendor. 
getMyReal(STRING key, UNSIGNED database) := FUNCTION  //Function for computing/retrieving a value.
  return myRedis.getReal(key, database);
END;

SEQUENTIAL(
    myRedis.setReal('pi', pi, 3),

    IF (myRedis.locking.Exists('pi'),  //Checks to see if key exists and if so lock it.
        myRedis.locking.GetReal('pi'), //Key already exists so GET.
        myRedis.locking.SetReal('pi', getMyReal('pi', 3))  //Key doesn't exist, retrieve from getMyReal, SET, and PUBLISH to other SUBSCRIBERS that value has been set.
    )
);
```

###Implementation Details
A few notes to point out here:
   * PUB-SUB channels are not disconnected from the keyspace as they are in their native redis usage. The key itself is used as the lock with its value being set as the channel to later
    PUBLISH on or SUBSCRIBE to. This channel is unique to the *server-IP*, *cache-port*, *key*, and *database*. It is in fact the underscore concatenation of all four, prefixed with the string **redis_ecl_lock**.
   * The lock itself is set to expire with a duration equal to the `timeout` value passed to the `locking.Exists(<key>` function (default 1s).
   * It is possible to manually 'unlock' this lock (`DEL` the key) via the `locking.Unlock(<key>)` function. *Note* this function will fail on any communication or reply error however,
   it will **silently fail**, leaving the lock to expire, if the server observes any change to the key during the function call duration.
   * When the *race-winner* publishes, it actually publishes the value itself such that any subscriber may obtain the key in this fashion, not requiring an additional `GET` and possible further race condition in doing so.
   * At present the 'lock' is not as such an actual lock, as only the `locking.Get<type>` functions acknowledge it. By current implementation it is better thought as a flag to `GET` to wait and subscribe. I.e. the locked key can be deleted and re-set just as any other key can be, via the `sync` MODULE functions.
