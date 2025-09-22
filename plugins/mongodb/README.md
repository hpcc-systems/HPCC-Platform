# MongoDB plugin for ECL

The MongoDB plugin allows an ECL user to embed MongoDB function calls into their code and run them on the HPCC Platform. It supports inserting a dataset into a database using `insert_many`, and can build ECL datasets from MongoDB result documents returned by the `find`, `update`, `delete`, `aggregate`, and `runCommand` methods.

The embedded script passed to the plugin can be used to create complex BSON documents to support almost every MongoDB command.

Important: Use the same keys that exist in MongoDB when declaring a return type or when creating a BSON document. Otherwise, the plugin will look for a field that may not exist when building the resulting dataset.

## Installation

The plugin uses vcpkg and can be installed by creating a separate build directory from the platform and running the following commands:

```bash
cd ./mongodb-build
cmake -DMONGODBEMBED=ON ../HPCC-Platform
make -j$(nproc)
package
sudo dpkg -i ./hpccsystems-plugin-mongodbembed_\<version\>.deb
```

## Documentation

[Doxygen](https://www.doxygen.nl/index.html) can be used to create HTML documentation for the code (call/caller graphs are also generated if [Graphviz dot](https://www.graphviz.org/download/) is installed and on your path).

```bash
cd plugins/mongodb
doxygen Doxyfile
```

Open `plugins/mongodb/docs/html/index.html` in a browser to view the generated documentation.

## Usage

To start using the plugin in ECL code, import the library:

```ecl
IMPORT MongoDB;
```

Define a function with an `EMBED(mongodb ...)` statement. Only one MongoDB operation per embed statement is supported (for multiple chained operations use an aggregation pipeline).

### Options

To create the URI for the MongoDB connection the ECL user passes the username, password, server name (for clusters) or just the port (for local), plus optional batch size and connection options. The plugin constructs a shared connection instance. Multiple MongoDB clusters (different credentials/options) can be used within the same workunit. Batch size controls the MongoDB cursor fetch size (default 100).

| Option | Description |
| ------ | ----------- |
| `user` | Username with read/write privileges. |
| `password` | Password for the above user. |
| `server` | Server connection string for MongoDB Atlas (e.g. `cluster0.example.mongodb.net`). |
| `port` | Port number for connecting to a local MongoDB server. |
| `database` | Database name (Required). |
| `collection` | Collection name (Required). |
| `batchSize` | Cursor batch size (default 100). |
| `limit` | Limit number of documents returned by `find` (no default limit). |
| `connectionOptions` | Ampersand separated options applied to the connection (one set per workunit). |

#### Connection Options

Specify additional connection options via the `connectionOptions` clause in the embed statement using ampersand separators: `option0&option1`.

Example:
```ecl
getConnection() := EMBED(mongodb : user(user), password(pwd), server(server), database(dbname), collection(collname), batchSize(100), connectionOptions('ssl=true&connectTimeoutMS=1000'))
	find({});
ENDEMBED;
```

When connecting to a MongoDB Cluster (not a local instance) `retryWrites=true` and `w=majority` are already set following MongoDB examples.

#### URI Options

Timeout and other URI-supported options can be appended to the connection string: `&connectTimeoutMS=30000`. Multiple options are separated by `&`. See the [Manual](https://www.mongodb.com/docs/v5.2/reference/connection-string/#connection-string-options).

#### Connecting to a Cluster vs Local

Cluster connection:
```ecl
getConnection() := EMBED(mongodb : user(user), password(pwd), server(server), database(dbname), collection(collname), batchSize(100))
	find({});
ENDEMBED;
```

Local connection (port only):
```ecl
getConnection() := EMBED(mongodb : port(port), database(dbname), collection(collname))
	find({});
ENDEMBED;
```

### Parameters

Function parameters inside the MongoDB script are referenced with a `$` prefix. `$` is also used by MongoDB operators, but they do not conflict unless you use a reserved operator name as a parameter.

```ecl
dataset({STRING _id}) getCount(REAL salary) := EMBED(mongodb : user(user), password(pwd), server(server), database(dbname), collection(collname), batchSize(100))
	find({ salary: { $gt: $salary }});
ENDEMBED;
```

### Limitations

MongoDB Atlas shared cluster transfer limits (7‑day rolling):
* M0: 10 GB in / 10 GB out
* M2: 20 GB in / 20 GB out
* M5: 50 GB in / 50 GB out

See the [Atlas limitations](https://www.mongodb.com/docs/atlas/reference/free-shared-limitations/).

### Type Conversions

| ECL | MongoDB |
| --- | ------- |
| BOOLEAN | b_bool |
| REAL | b_double |
| INTEGER | b_int64 |
| UINTEGER | b_int64 (converted) |
| UTF8 | b_utf8 |
| QSTRING/VSTRING/STRING | b_utf8 |
| DECIMAL | b_decimal128 |

| MongoDB | ECL |
| ------- | --- |
| b_date | STRING or INTEGER |
| b_regex | `{STRING pattern, STRING options}` |
| b_timestamp | `{UNSIGNED t, UNSIGNED i}` |

Unsigned Integers are unsupported in MongoDB and are converted to signed 64-bit integers.

Regex and timestamp helper record layouts are provided in `mongodb.ecllib`.

### Inserting Documents

`insert_one` vs `insert_many` is selected automatically. Passing a dataset triggers `insert_many`; a single record or scalars triggers `insert_one`.

```ecl
dataset(mongodb.insertManyResultRecord) insertMany(dataset(layoutEmployee) employees) := EMBED(mongodb : user(user), password(pwd), server(server), database(db), collection(coll))
	insert({$employees});
ENDEMBED;
insertMany(ds);

insertOne(STRING first, STRING last, REAL salary) := EMBED(mongodb : user(user), password(pwd), server(server), database(db), collection(coll))
	insert({first: $first, last: $last, salary: $salary});
ENDEMBED;
insertOne('John','Doe',12345.6);
```

### Finding Documents

Supports `find_one` and `find` (many). Optional projection.

```ecl
dataset({STRING first, STRING last, REAL salary}) findOne() := EMBED(mongodb : user(user), password(pwd), server(server), database(db), collection(coll))
	find_one({first: 'John', last: 'Doe'});
ENDEMBED;

dataset({STRING first, STRING last, REAL salary}) findMany() := EMBED(mongodb : user(user), password(pwd), server(server), database(db), collection(coll))
	find({first: 'John', last: 'Doe'});
ENDEMBED;
```

Projection example:
```ecl
dataset({STRING first, STRING last, REAL salary}) findMany(INTEGER include) := EMBED(mongodb : user(user), password(pwd), server(server), database(db), collection(coll))
	find({first: 'John', last: 'Doe'}, {first: $include, last: $include, salary: $include});
ENDEMBED;
```

### Updating Documents

Returns counts of matched and modified documents. Supports document or pipeline updates.

```ecl
dataset(mongodb.updateResultRecord) updateOne(REAL raise) := EMBED(mongodb : user(user), password(pwd), server(server), database(db), collection(coll))
	update_one({}, {$inc: {salary: $raise}});
ENDEMBED;

dataset(mongodb.updateResultRecord) updateMany(REAL min, REAL raise) := EMBED(mongodb : user(user), password(pwd), server(server), database(db), collection(coll))
	update_many({salary: {$lte: $min}}, {$inc: {salary: $raise}});
ENDEMBED;
```

### Deleting Documents

`delete_one` and `delete_many` supported.

```ecl
dataset({mongodb.deleteResultRecord}) deleteOne(REAL min) := EMBED(mongodb : user(user), password(pwd), server(server), database(db), collection(coll))
	delete_one({first:'James', salary: {$lte: $min}});
ENDEMBED;

dataset({mongodb.deleteResultRecord}) deleteMany(REAL max) := EMBED(mongodb : user(user), password(pwd), server(server), database(db), collection(coll))
	delete_many({salary: {$gt: $max}});
ENDEMBED;
```

### Aggregation Pipelines

Pass a pipeline array `[{...}, ...]` to `aggregate`.

```ecl
dataset({STRING first, STRING last, REAL salary}) aggregate(REAL max, REAL min, INTEGER order) := EMBED(mongodb : user(user), password(pwd), server(server), database(db), collection(coll), batchSize(100))
	aggregate([{$match: {salary: {$gte: $min, $lt: $max}}}, {$sort: {salary: $order}}]);
ENDEMBED;
```

### runCommand

Run database-level commands with `runCommand` returning operation-specific documents.

```ecl
dataset(layoutEmployee) findInfo(BOOLEAN mybool) := EMBED(mongodb : user(user), password(pwd), server(server), database(db), collection('test1'))
	runCommand({ findAndModify: 'test1', query: {first: 'Anne', last: 'Smith'}, remove: $mybool });
ENDEMBED;
```

### Creating an Index

```ecl
createIndex(INTEGER asc) := EMBED(mongodb : user(user), password(pwd), server(server), database('mydb'), collection('test2'))
	create_index({ first: $asc, last: $asc });
ENDEMBED;

createIndex(INTEGER asc, BOOLEAN myBool) := EMBED(mongodb : user(user), password(pwd), server(server), database('mydb'), collection('test2'))
	create_index({ first: $asc, last: $asc }, { name: 'Person', unique: $myBool });
ENDEMBED;
```

---

This README has been consolidated to remove duplicated sections and to escape any placeholder angle brackets where present. All MongoDB examples use single quotes inside ECL to avoid JSON-like double quote conflicts, and no raw HTML tags are present that would confuse the Vue template parser.
