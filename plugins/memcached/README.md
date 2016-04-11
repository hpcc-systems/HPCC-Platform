ECL Memcached Plugin
================

This is the ECL plugin to utilize the volatile key-value cache [Memcached](http://memcached.org).
It utilises the C API [libmemcached](http://libmemcached.org/libMemcached.html).

Installation and Dependencies
----------------------------

To build the memcached plugin with the HPCC-Platform, libmemcached-dev is required.
```
sudo apt-get install libmemcached-dev
```

The memcached daemon can be obtained via - [source](http://memcached.org/downloads) or the preferred method:
```
sudo apt-get install memcached
```

*Note:* **libmemcached** 1.0.10 or greater is required to use this plugin as intended. It is advised to use the newest version of **memcached** possible to you.


Getting started
---------------

The daemon can be started by typing `memcached -d` within a terminal. To run with with a non-default configuration, for example to listen on another **IP** and **port** -
`memcached -d -l <ip> -p <port>`. When wishing to use a pool of memcached servers, each instance must be started, bound to the **IP** that makes it visible to the other
instances, e.g. the default `memcached -d` on all machines will not work as they will all be bound to the localhost loopback, 127.0.0.1.

This plugin forces the **_binary-only_** communication protocol and therefore **memcached** cannot be started in its ASCII mode, i.e. `memcached -b ascii`.

*Note:* The default memcached **port** is 11211 and that if multiple and individual caches are required then they are by definition memacached instances with different ports.

Further documentation is available [here](https://code.google.com/p/memcached/wiki/NewStart).

The Actual Plugin
-----------------

The bulk of this memcached plugin for **ECL** is made up of the various `SET` and `GET` commands e.g. `GetString` or `SetReal`. They are accessible via the module `memcached`
from the memcached plugin **ECL** library `lib-memcached`. i.e.
```
IMPORT memcached FROM lib_memcached;
```
Here is a list of the core plugin **functions**.

###Set
```
SetUnicode (CONST VARSTRING key, CONST UNICODE value, CONST VARSTRING options, CONST VARSTRING partitionKey = '', UNSIGNED expire = 0)
SetString  (CONST VARSTRING key, CONST STRING value,  CONST VARSTRING options, CONST VARSTRING partitionKey = '', UNSIGNED expire = 0)
SetUtf8    (CONST VARSTRING key, CONST UTF8 value,    CONST VARSTRING options, CONST VARSTRING partitionKey = '', UNSIGNED expire = 0)
SetBoolean (CONST VARSTRING key, BOOLEAN value,       CONST VARSTRING options, CONST VARSTRING partitionKey = '', UNSIGNED expire = 0)
SetReal    (CONST VARSTRING key, REAL value,          CONST VARSTRING options, CONST VARSTRING partitionKey = '', UNSIGNED expire = 0)
SetInteger (CONST VARSTRING key, INTEGER value,       CONST VARSTRING options, CONST VARSTRING partitionKey = '', UNSIGNED expire = 0)
SetUnsigned(CONST VARSTRING key, UNSIGNED value,      CONST VARSTRING options, CONST VARSTRING partitionKey = '', UNSIGNED expire = 0)
SetData    (CONST VARSTRING key, CONST DATA value,    CONST VARSTRING options, CONST VARSTRING partitionKey = '', UNSIGNED expire = 0)
```

###Get
```
INTEGER8   GetInteger(CONST VARSTRING key, CONST VARSTRING options, CONST VARSTRING partitionKey = '')
UNSIGNED8 GetUnsigned(CONST VARSTRING key, CONST VARSTRING options, CONST VARSTRING partitionKey = '')
STRING      GetString(CONST VARSTRING key, CONST VARSTRING options, CONST VARSTRING partitionKey = '')
UNICODE    GetUnicode(CONST VARSTRING key, CONST VARSTRING options, CONST VARSTRING partitionKey = '')
UTF8          GetUtf8(CONST VARSTRING key, CONST VARSTRING options, CONST VARSTRING partitionKey = '')
BOOLEAN    GetBoolean(CONST VARSTRING key, CONST VARSTRING options, CONST VARSTRING partitionKey = '')
REAL          GetReal(CONST VARSTRING key, CONST VARSTRING options, CONST VARSTRING partitionKey = '')
DATA          GetData(CONST VARSTRING key, CONST VARSTRING options, CONST VARSTRING partitionKey = '')
```

###Utility
```
BOOLEAN Exists(CONST VARSTRING key, CONST VARSTRING options, CONST VARSTRING partitionKey = '')
CONST VARSTRING KeyType(CONST VARSTRING key, CONST VARSTRING options, CONST VARSTRING partitionKey = '')
Clear(CONST VARSTRING options)
Delete(CONST VARSTRING key, CONST VARSTRING options, CONST VARSTRING partitionKey = '')
```

The core points to note here are:
   * There is a **SET** and **GET** function associated with each fundamental **ECL** type. These must be used for and with their correct *value* types! Miss-use *should* result
   in an runtime exception, however, this is only conditional on having the value retrieved from the server fitting into memory of the requested type. E.g. it is possible for a
   STRING of length 8, set with SetString, being successfully retrieved from the cache via GetInteger without an **ECL** exception being thrown. A warning is added to the
   local log file when this occurs.
   * `CONST VARSTRING options` passes the server **IP** and **port** to the plugin in the *strict* format - `--SERVER=<ip>:<port>`. Multiple server use simply requires all
   to be specified e.g. `--SERVER=192.168.1.98:11211 --SERVER=192.168.1.97:11211`. In addition a variety of options are passed in with this string e.g **_timeout_ _values_**.
   A full list of possible options exists [here](http://docs.libmemcached.org/libmemcached_configuration.html).
   * `UNSIGNED expire` has units seconds and a default of **0**, i.e. *forever*. *Note:* Anything above 30 days is treated as a unix timestamp.
   * The default timeout is 1 second.
   * `KeyType()` returns the **ECL** type as an **ECL** `VARSTRING`, i.e. `STRING` or `UNSIGNED`, or `UNKNOWN` if the value was set externally from this plugin
   without a type specifier. *c.f.* Behaviour and Implementation Details below for further details.


###An Memcached 'Partition Key'
A *partition key* supplied to a `SET` will be hashed with the key and thus distribute the key-value pair according to this hash, such that all keys with the same
*partition key* are on the same server. The notion of a partition is physical rather than logical. A *partition key* should therefore **_not_** be used with only
a single memcached server. Observer the following two examples with servers started as `memcached -d -l 192.168.1.97` and `memcached -d -l 192.168.1.98`.

```
IMPORT memcached FROM lib_memcached;

STRING server1 := '--SERVER=192.168.1.97:11211';
REAL pi := 3.14159265359;

SEQUENTIAL(
    memcached.SetReal('pi', pi, server1);
    memcached.SetReal('pi', pi*pi, server1, 'pi again');

    memcached.GetReal('pi', server1);                                 //returns 9.869604401090658
    memcached.GetReal('pi', server1, 'pi again');                     //returns 9.869604401090658
    memcached.GetReal('pi', server1, 'you\'d think this would fail'); //returns 9.869604401090658
    memcached.Clear(server1);
    );

STRING servers := server1 + ' --SERVER=192.168.1.98:11211';
SEQUENTIAL(
    memcached.SetReal('pi', pi, servers);
    memcached.SetReal('pi', pi*pi, servers, 'pi again');

    memcached.GetReal('pi', servers);                                 //returns 3.14159265359
    memcached.GetReal('pi', servers, 'pi again');                     //returns 9.869604401090658
    memcached.GetReal('pi', servers, 'you\'d think this would fail'); //returns 3.14159265359
    memcached.Clear(servers);
    );
```

*NOTE:* **libmemcached** uses a separate set of functions when using a *partition key*. When this plugin's default is used, the empty string is not hashed with the key,
instead the non-partition-key functions are used.

Behaviour and Implementation Details
------------------------------------
A few notes to point out here:
   * When a key and value are stored with Set<type>, memcached also allows for a 4byte flag to be stored. This plugin utilizes this space to store an enumeration specifying the
   **ECL** type that is being stored. Care should therefore be taken when using KeyType(<key>) when the key was set from a client other than this plugin.
   * The following libmemcached settings are invoked by default for this plugin, all of which take precedence over any passed in via the `options` string:
   **MEMCACHED_BEHAVIOR_KETAMA** = 1, **MEMCACHED_BEHAVIOR_USE_UDP** = 0, **MEMCACHED_BEHAVIOR_NO_BLOCK** = 0, **MEMCACHED_BEHAVIOR_BUFFER_REQUESTS** = 0,
   **MEMCACHED_BEHAVIOR_BINARY_PROTOCOL** = 1.
