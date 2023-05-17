# MongoDB plugin for ECL

The MongoDB plugin allows an ECL user to embed MongoDB function calls into their code and run it
on the HPCC Platform. The plugin supports inserting a dataset into a database using `insert_many`, and can 
build ECL datasets from MongoDB result documents returned by the `find`, `update`, `delete`, `aggregate`, and `runCommand` methods.

The embedded script that gets passed to the plugin can be used to create complex documents to support almost every
MongoDB command. 

It is important to use the same keys as the ones in MongoDB when declaring a return type or when creating a BSON document. Otherwise, the plugin will look for a field that might not exist to return when building the resulting dataset.

## Installation

The plugin uses vcpkg and can be installed by creating a separate build directory from the platform and running the following commands:
```
cd ./mongodb-build
cmake -DMONGODBEMBED=ON ../HPCC-Platform
make -j3 package
sudo dpkg -i ./hpccsystems-plugin-mongodbembed_<version>.deb
```

## Documentation

[Doxygen](https://www.doxygen.nl/index.html) can be used to create nice HTML documentation for the code. Call/caller graphs are also generated for functions if you have [dot](https://www.graphviz.org/download/) installed and available on your path.

Assuming `doxygen` is on your path, you can build the documentation via:
```
cd plugins/mongodb
doxygen Doxyfile
```

The Documentation can then be accessed via `mongodb/docs/html/index.html`.

## Usage

To start using the plugin in your ECL code you must import mongodb before you create the EMBED statement.
```
IMPORT MongoDB;
```

You then need to define a function for interfacing with MongoDB. Within the EMBED statement is where you can write code to interact with the MongoDB database or collection that you have passed into the EMBED options. The plugin only supports one line and one operation per EMBED statement. If you wish to chain multiple operations together the aggregate function is supported and can take an aggregation pipeline as long as it is valid BSON.

### Options

To create the uri for the MongoDB connection instance the ECL user needs to pass in the username, password, server name, or just the port to use for connecting to the cluster. The plugin takes all of these and creates a shared connection instance for many threads to have access to the MongoDB databases. The plugin can connect to multiple MongoDB clusters with different connection options and user credentials all from the same workunit. The ECL user can also define the batch size of the result rows.

| Option | Description |
| ------ | ----------- |
| user   | Username of user with read and write priveleges to MongoDB server. |
| password | Password of user with read and write priveleges to MongoDB server. |
| server | Server connection string for connecting to MongoDB Atlas. (cluster0.qdvfhrk.mongodb.net) |
| port | Port number for connecting to a local MongoDB server. |
| database | Name of the database to issue commands to. (Required) |
| collection | Name of the collection to issue commands to. (Required) |
| batchSize | Batch size of cursor to result records. The default batch size is 100 meaning the cursor fetches 100 documents at a time from MongoDB. |
| connectionOptions | A string of connection options used to make the connection to the cluster. Currently only one set of connection options will be used per workunit. |

#### Connection Options 

To specify connection options to the MongoDB cluster use the connectionOptions option in the embed statement. The format for the connection options is ampersand separated options like so: <option>&<option>

**Important note:** when connecting to a MongoDB Cluster and not a local instance the retryWrites=true and w=majority options are already set according to MongoDB examples.

```
connectionOptions('ssl=true&connectTimeoutMS=1000') // For connecting to MongoDB Clusters and local mongod instance
```

#### URI options

For specifying timeout settings and other options supported by MongoDB add them to the end of the connection string like this:
```
&connectTimeoutMS=30000
```
Multiple options are seperated by '&', and more information about additional operations can be found in the [Manual](https://www.mongodb.com/docs/v5.2/reference/connection-string/#connection-string-options).

#### Connecting to the cluster

```
getConnection() := EMBED(mongodb : user(user), password(pwd), server(server), database(dbname),  collection(collname), batchSize(100))
ENDEMBED; 
```

For connecting to a local MongoDB instance you just need to pass in the port number that the server is listening on.

```
getConnection() := EMBED(mongodb : port(port), database(dbname),  collection(collname))
ENDEMBED; 
```

### Parameters

To use function parameters within the MongoDB statement, prefix them with a '\$' inside the embedded script. The '\$' is also reserved in MongoDB, but it does not interfere with the parameters as long as you don't also use a word reserved for a MongoDB command. More information on MongoDB operators can be found in the [Manual](https://www.mongodb.com/docs/manual/reference/operator/query/).

```
dataset({STRING _id}) getCount(REAL salary) := EMBED(mongodb : user(user), password(pwd), server(server), database(dbname),  collection(collname), batchSize(100))
    find({ salary: { $gt: $salary}});
ENDEMBED; 
```

### Limitations

There are a few limitations placed by MongoDB on the amount of data you are allowed to transer if you are using a shared cluster. In a single 7-day rolling period you are allowed to transfer:

* M0: 10 GB in and 10 GB out per period
* M2: 20 GB in and 20 GB out per period
* M5: 50 GB in and 50 GB out per period

For a more detailed list of the limitations on MongoDB clusters: [Manual](https://www.mongodb.com/docs/atlas/reference/free-shared-limitations/)

### Type conversions

Not every ECL or MongoDB datatype translates seemlessly to the other side.

| ECL datatypes | MongoDB equivalent |
| ------------- | ------------------ |
| BOOLEAN | b_bool |
| REAL | b_double |
| INTEGER | b_int64 |
| UINTEGER | b_int64 |
| UTF8 | b_utf8 |
| QSting, VString, String | b_utf8 |
| DECIMAL | b_decimal128|

| MongoDB datatypes | ECL equivalent |
| ----------------- | -------------- |
| b_date | STRING, INTEGER |
| b_regex |  Unsupported |
| b_timestamp | Unsupported |

The MongoDB date datatype can be converted to an integer in MongoDB or it will automatically be converted to a STRING by the plugin. Typically Dates before 1970 get returned by MongoDB as INTEGERS. Also, Unsigned Integers are unsupported in MongoDB. This means that in order to insert UINTEGERs into the database the plugin converts them to b_int64 which is a 64 bit signed integer.

### Inserting Documents

The plugin supports inserting documents one at a time using `insert_one` and inserting multiple documents at once using insert_many. To insert a document you just need to call insert() inside the embedded script and the plugin will call `insert_one` if you pass in a single record or scalar values. If you pass in a dataset the plugin will automatically call `insert_many` for you. For more information on inserting documents: [Manual](https://www.mongodb.com/docs/manual/tutorial/insert-documents/)

```
dataset(mongodb.insertManyResultRecord) insertMany(dataset(layoutEmployee) employees) := EMBED(mongodb : user(user), password(pwd), server(server), database(db),  collection(coll))
    insert({$employees});
ENDEMBED;
insertMany(ds);

insertOne(STRING first, STRING last, REAL salary) := EMBED(mongodb : user(user), password(pwd), server(server), database(db),  collection(coll))
    insert({first: $first, last: $last, salary: $salary}); 
ENDEMBED;
insertOne();
```

The first example inserts a dataset with the layoutEmployee record and the second example creates a bson document and inserts some scalar values into it then adds it to the collection. 

### Finding Documents

The plugin allows the ECL user to choose between find_one and find_many. They both take a single document as the filter for the find operation. For more information on finding documents: [Manual](https://www.mongodb.com/docs/manual/reference/method/db.collection.find/)

```
dataset({STRING first, STRING last, REAL salary}) findOne() := EMBED(mongodb : user(user), password(pwd), server(server), database(db),  collection(coll))
    find_one({first: "John", last: "Doe"});
ENDEMBED;
findOne();

dataset({STRING first, STRING last, REAL salary}) findMany() := EMBED(mongodb : user(user), password(pwd), server(server), database(db),  collection(coll))
    find({first: "John", last: "Doe"});
ENDEMBED;
findMany();
```

There is support for providing a projection to the find function. If no projection is given then MongoDB returns all fields from the document, and the ECL engine must look through every field trying to find the ones declared in the return type by the ECL user.

```
dataset({STRING first, STRING last, REAL salary}) findMany(INTEGER include) := EMBED(mongodb : user(user), password(pwd), server(server), database(db),  collection(coll))
    find({first: "John", last: "Doe"}, {first: $include, last: $include, salary: $include});
ENDEMBED;
findMany(1);
```

### Updating Documents

The plugin can update one or many documents which is specified by the user. it returns a single Record with two fields holding the number of documents that were matched by the filter and another holding the number of documents that were modified by the operation. The update methods can take either two documents or a document and a pipeline. A pipeline follows the structure [{doc}, ... ]. For more information on updating documents: [Manual](https://www.mongodb.com/docs/manual/tutorial/update-documents/)

```
dataset(mongodb.updateResultRecord) updateOne(REAL raise) := EMBED(mongodb : user(user), password(pwd), server(server), database(db),  collection(coll))
    update_one({}, {$inc: {"salary": $raise}});
ENDEMBED;
updateOne(1000);

dataset(mongodb.updateResultRecord) updateMany(REAL min, REAL raise) := EMBED(mongodb : user(user), password(pwd), server(server), database(db),  collection(coll))
    update_many({"salary": {$lte: $min}}, {$inc: {"salary": $raise}});
ENDEMBED;
updateMany(100000, 3000);
```

The first example updates the first document that matches the find filter. In this example the find filter is empty, so all of the documents are returned. The raise argument gets inserted into the document as a real instead of a string.

In the second example a filter is passed that will return all the documents that are below the min argument passed to the function. Then it will increase the salary of every document matching the filter by the raise argument passed into the function.


### Deleting Documents

The plugin can delete one or many documents which is specified by the user. It returns a document with a single field holding the number of deleted documents. The delete function takes a single document as a filter for the delete operation. For more information on removing documents: [Manual](https://www.mongodb.com/docs/manual/tutorial/remove-documents/)


```
dataset({mongodb.deleteResultRecord}) deleteOne(REAL min) := EMBED(mongodb : user(user), password(pwd), server(server), database(db),  collection(coll))
    deleteOne({first:"James", "salary": {$lte: $min}});
ENDEMBED;
delete_one(75000);

dataset({mongodb.deleteResultRecord}) deleteMany(REAL max) := EMBED(mongodb : user(user), password(pwd), server(server), database(db),  collection(coll))
    delete_many({"salary": {$gt: $max}});
ENDEMBED;
deleteMany(200000);
```

### Aggregation pipelines

Aggregation pipelines offer a lot of functionality and there is way too much for me to show you here, but the plugin should work with any kind of aggregation pipeline that is passed in as long as it is valid bson. The aggregate command takes a single pipeline of the form [{doc}, ... ].


```
dataset({STRING first, STRING last, REAL salary}) aggregate(REAl max, REAL min, INTEGER order) := EMBED(mongodb : user(user), password(pwd), server(server), database(db),  collection(coll), batchSize(100))
    aggregate([{$match: { salary: {$gte: $min, $lt: $max}}}, {$sort: {salary: $order}}]);
ENDEMBED;
aggregate(200000, 75000, 1);
```
In this example there are two stages in the pipeline. The first stage is a match operation and will find all the documents that have a salary greater than or equal to the min and less than the max. Then every document that gets returned from the first stage gets sorted by salary in the second stage. The order needs to be an integer and is 1 for ascending and -1 for descending.

More information and examples of MongoDB aggregation operations can be found here: [Manual](https://www.mongodb.com/docs/manual/aggregation/)

### MongoDB runCommand function

The runCommand function allows the user to run some commands at a database level instead of at the collection level. It has a single argument of a bson document. For each runCommand operation there is a unique return value. To get the data from the operation the record structure of the dataset must match the operation specific output of the value document. For more information on runCommand: [Manual](https://www.mongodb.com/docs/manual/reference/method/db.runCommand/)


```
dataset(layoutEmployee) findInfo(BOOLEAN mybool) := EMBED(mongodb : user(user), password(pwd), server(server), port(port), database(db),  collection('test1'))
    runCommand(
        {
            findAndModify: "test1",
            query: {first: "Anne", last: "Smith"},
            remove: $mybool
        }
    );
ENDEMBED;
findInfo(true);
```

In this example the findAndModify command is ran on the test1 collection. Then it removes all the documents matching the query filter if remove is true.

### Creating an Index

Indexes can be useful for speeding up common queries on large collections that have similar fields. For more information on the create_index function: [Manual](https://www.mongodb.com/docs/manual/reference/method/db.collection.createIndex/)


```
createIndex(INTEGER asc) := EMBED(mongodb : user(user), password(pwd), server(server), database('mydb'),  collection('test2'))
    create_index({ "first": $asc, "last": $asc});
ENDEMBED;
createIndex(1);

createIndex(INTEGER asc, BOOLEAN myBool) := EMBED(mongodb : user(user), password(pwd), server(server), database('mydb'),  collection('test2'))
    create_index({ "first": $asc, "last": $asc}, {name: "Person", unique: $myBool});
ENDEMBED;
createIndex(1, true);
```

In this example an index is created with the keys first and last and in ascending order.

