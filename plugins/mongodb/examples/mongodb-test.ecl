Import MongoDB;

server := 'cluster0.nvkdfj5.mongodb.net';

user := 'username';

pwd := 'password';

databaseName := 'mydb';

collectionName := 'test1';

// Records for defining the layout of example datasets
reviewsRec := RECORD
    INTEGER review_scores_cleanliness; 
    INTEGER review_scores_checkin; 
    INTEGER review_scores_communication; 
    INTEGER review_scores_location; 
    INTEGER review_scores_value; 
    INTEGER review_scores_rating; 
    INTEGER review_scores_accuracy;
END;

layoutairbnb := RECORD
    UTF8 name; 
    UTF8 space; 
    UTF8 description; 
    INTEGER beds; 
    INTEGER accommodates;
    SET OF STRING amenities; 
    DATASET(reviewsRec) review_scores;
END;

transactionrec := {
    INTEGER amount,
    STRING transaction_code,
    STRING symbol,
    STRING price,
    STRING total
};

layouttransactions := RECORD
    INTEGER account_id;
    INTEGER transaction_count;
    DATASET(transactionrec) transactions;
END;

layoutFees := RECORD
    DECIMAL32_2 price;
    DECIMAL32_2 security_deposit;
    DECIMAL32_2 cleaning_fee;
    DECIMAL32_2 extra_people;
    REAL guests_included;
END;

layoutDates := {STRING bucket_start_date, STRING bucket_end_date};
layoutEmployee := {INTEGER1 id, STRING25 first, STRING25 last, REAL salary};
layoutperson := {String username, String address, String email};

// Example/Test functions

// Returns the count of every document in the listingsAndReviews collection
integer getCount() := EMBED(mongodb : user(user), password(pwd), server(server), database('sample_airbnb'),  collection('listingsAndReviews'))
    find({});
ENDEMBED; 

INTEGER beds := 3;
INTEGER accommodates := 5;
// Returns a dataset of layoutairbn. Finds all the documents where the beds and accomodates fields are higher than the argument values.
dataset(layoutairbnb) findABInfo(INTEGER beds, INTEGER accommodates) := EMBED(mongodb : user(user), password(pwd), server(server), database('sample_airbnb'),  collection('listingsAndReviews'))
    find({beds: {$gte: $beds}, accommodates: {$gte: $accommodates}});
ENDEMBED;

// Returns a dataset with only the _id and totalquantity fields. First stage matches all the documents that equal that string then groups them by their salary and adds every groups ids together.
dataset({STRING _id, INTEGER totalquantity}) EmployeeAggregate() := EMBED(mongodb : user(user), password(pwd), server(server), database('mydb'),  collection('test1'))
    aggregate([{$match: { first: "Jim                      " }}, {$group: { _id: "$salary", totalQuantity: { $sum: "$id" }}}]);
ENDEMBED;

// In the instance of a NULL field for an integer the engine will return a zero
INTEGER nights := 5;
// Matches all the documents where the string field minimum_nights is greater than the argument value. It then groups them by their bed count and returns the count of eachs group's documents.
dataset({Integer _id, Integer count}) findCount(INTEGER min_nights) := Embed(mongodb : user(user), password(pwd), server(server), database('sample_airbnb'),  collection('listingsAndReviews'))
    aggregate([
        { $match: {"$expr" : {"$gt" : [{"$toInt" : "$minimum_nights"} , $min_nights]}}},
        { $group: { _id: "$beds", count: { $count: { } }}}
    ]);
ENDEMBED;

// Matches all the documents that have a Real Bed. Then it groups them by their bed count and counts the documents in each group.
dataset({Integer _id, Integer count}) findBedType() := Embed(mongodb : user(user), password(pwd), server(server), database('sample_airbnb'),  collection('listingsAndReviews'))
    aggregate([
        { $match: {bed_type : "Real Bed"}},
        { $group: { _id: "$beds", count: { $count: { } }}}
    ]);
ENDEMBED;

STRING houseType := 'House';
INTEGER sortOrder := -1; // -1 for Descending and 1 for Ascending order
// Matches all the documents where the property type matches the argument value. Then it sorts them based and the bed field and the order based on the argument value.
dataset(layoutairbnb) searchPropertySorted(STRING text, INTEGER sortOrder) := Embed(mongodb : user(user), password(pwd), server(server), database('sample_airbnb'),  collection('listingsAndReviews'))
    aggregate(
        [
            { $match: { property_type: $text } },
            { $sort: { beds: $sortOrder } }
        ]
    );
ENDEMBED;
// Groups every document by their property type and counts the documents in each group. Then it sorts them by the count and _id based on the argument value.
dataset({STRING _id, Integer count}) countPropertyTypes(INTEGER cntOrd, INTEGER idOrd) := Embed(mongodb : user(user), password(pwd), server(server), database('sample_airbnb'),  collection('listingsAndReviews'))
    aggregate(
        [
            { $group: { _id: "$property_type", count: { $count: {} } } },
            { $sort: { count: $cntOrd, _id: $idOrd } }
        ]
    );
ENDEMBED;

// Returns every document, but only the fields defined in layouttransactions are returned.
dataset(layouttransactions) findTransactions() := Embed(mongodb : user(user), password(pwd), server(server), database('sample_analytics'),  collection('transactions'))
    find({});
ENDEMBED;

// Returns every document, but only the fields defined in layoutDates are returned.
dataset(layoutDates) findTransactionDates() := Embed(mongodb : user(user), password(pwd), server(server), database('sample_analytics'),  collection('transactions'))
    find({});
ENDEMBED;

// Matches all the documents where the price is greater than or equal to the min argument and less than the max argument. Then sorts the results by price first then extra_people then security_deposit.
dataset(layoutFees) findAndSort(REAL4 max, REAL4 min, INTEGER asc) := Embed(mongodb : user(user), password(pwd), server(server), database('sample_airbnb'),  collection('listingsAndReviews'))
    aggregate([{$match: { price: { $gte: $min, $lt: $max}}}, {$sort: {price: $asc, extra_people: $asc, security_deposit: $asc}}]);
ENDEMBED; 

// Inserts a dataset using insert_many and returns the count of documents that were inserted.
integer insertMany(dataset(layoutEmployee) employees) := Embed(mongodb : user(user), password(pwd), server(server), database('mydb'),  collection('test2'))
    insert({$employees});
ENDEMBED;
employeeDS := DATASET ([{1, 'John', 'Andrews', 101000.5}, {2, 'Anne', 'Smith', 100000.7}, {3, 'Amy', 'Isaac', 103000.1}, {4, 'Kirk', 'Captain', 109000.9}, {5, 'Steve', 'Rogers', 99000.6}, 
            {6, 'Evan', 'Bosch', 104000.5}, {7, 'Jack', 'Adams', 101000.5}, {8, 'Vince', 'Carter', 306000.5}, {9, 'Beth', 'Stevens', 102000.2}, {10, 'Samantha', 'Rogers', 107000.5}], layoutEmployee);

// Creates an Index on the fields "first" and "last" and sorts them in ascending order.
boolean createIndex(INTEGER asc) := Embed(mongodb : user(user), password(pwd), server(server), database('mydb'),  collection('test2'))
    create_index({ "first": $asc, "last": $asc});
ENDEMBED;

REAL salary := 101000;
REAL raise := 1000;
// Updates the documents that have a salary less than the argument value and increases it by the raise argument's value.
dataset(mongodb.updateRecord) updateInfo(REAL salary, REAL raise) := EMBED(mongodb : user(user), password(pwd), server(server), database('mydb'),  collection('test1'))
    update_many({"salary": {$lt: $salary}}, {$inc: {"salary": $raise}});
ENDEMBED;

mybool := true;
// Runs a command on the test1 collection and removes the document if mybool is true and it matches the query.
dataset(layoutEmployee) findInfo(BOOLEAN mybool) := EMBED(mongodb : user(user), password(pwd), server(server), database('mydb'),  collection('test1'))
    runCommand(
        {
            findAndModify: "test1",
            query: {first: "Anne", last: "Smith"},
            remove: $mybool
        }
    );
ENDEMBED;

// $or is not allowed in the M0 tier of MongoDB atlas
INTEGER nights := 5;
INTEGER ppl := 8;
// Matches all the documents that match either expression. Then it groups them by the number of beds they have and counts the number of documents in each group.
dataset({String _id, Integer count}) findCountOR(INTEGER min_nights, INTEGER people) := Embed(mongodb : user(user), password(pwd), server(server), database('sample_airbnb'),  collection('listingsAndReviews'))
    aggregate([{ $match: 
                    { $or: [ 
                        {"$expr" : {"$gt" : [{"$toInt" : "$minimum_nights"} , $min_nights]}}, 
                        {"$expr" : {"$gte" : [$accommodates, $people]}}
                        ]
                    }
                },
                { $group: { _id: "$beds", count: { $sum: 1}}}
            ]);
ENDEMBED;

SEQUENTIAL
(
    OUTPUT(getCount(), NAMED('CountAllDocuments'));
    OUTPUT(EmployeeAggregate(), NAMED('EmployeeAggregate'));
    OUTPUT(findABInfo(beds, accommodates), NAMED('AirbnbInformation'));
    OUTPUT(findCount(nights), NAMED('CountAggregate'));
    OUTPUT(findBedType(), NAMED('CountRealBeds'));
    OUTPUT(searchPropertySorted(houseType, 1), NAMED('SearchPropertiesAsc'));
    OUTPUT(searchPropertySorted(houseType, -1), NAMED('SearchPropertiesAndDec'));
    OUTPUT(countPropertyTypes(-1, 1), NAMED('CountPropertyTypes'));
    OUTPUT(CHOOSEN(findTransactions(), 5), NAMED('FindAllTransactions'));
    OUTPUT(findTransactionDates(), NAMED('TransactionPeriods'));
    OUTPUT(findAndSort(80.75, 20.50, -1), NAMED('FindAndSortByPrice'));
    OUTPUT(updateInfo(salary, raise), NAMED('UpdateMany'));
    OUTPUT(insertMany(employeeDS), NAMED('InsertMany'));
    OUTPUT(createIndex(1), NAMED('CreateIndexOnNames'));
    OUTPUT(findInfo(mybool), NAMED('RemoveOnQuery'));
    OUTPUT(findCountOR(nights,ppl), NAMED('OrCountAggregate'));
    OUTPUT('Done', Named('Status'));
);
