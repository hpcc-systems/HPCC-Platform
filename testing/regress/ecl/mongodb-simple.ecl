/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2022 HPCC Systems®.

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

//class=3rdparty
//class=embedded
//class=mongodb

Import MongoDB;

server := 'cluster0.uniqueserver.mongodb.net';

username := 'hpccuser';

password := 'supersecretpassword';

analyticsrec := RECORD
    INTEGER account_id;
    INTEGER transaction_count;
END;


airbnbrec := RECORD
    UTF8 name; 
    UTF8 space; 
    UTF8 description; 
    INTEGER beds; 
    INTEGER accommodates;
END;

feesrec := RECORD
    DECIMAL32_2 price;
    DECIMAL32_2 security_deposit;
    DECIMAL32_2 cleaning_fee;
    DECIMAL32_2 extra_people;
    REAL guests_included;
END;

// The sample datasets "sample_analytics.transactions" and "sample_airbnb.listingsAndReviews" need to be loaded onto the cluster manually.
// To load the sample datasets onto the cluster navigate to the "Database Deployments" page by clicking "Database" in the taskbar on the left.
// Then click the button with three dots next to the cluster you wish to load them on. Then select "Load Sample Dataset" and wait for the data to be loaded.

// Returns the document matching the account id that is passed in.
dataset(analyticsrec) findTransactionsByID(INTEGER id) := Embed(mongodb : user(username), password(password), server(server), database('sample_analytics'),  collection('transactions'))
    find_one({account_id: $id});
ENDEMBED;

// Returns the document with a transaction count greater than or equal to the number passed in.
dataset(analyticsrec) findTransactionsByCount(INTEGER trans_count, INTEGER id) := EMBED(mongodb : user(username), password(password), server(server), database('sample_analytics'),  collection('transactions'))
    find_one({transaction_count: {$gte: $trans_count}, account_id: $id});
ENDEMBED;

// Returns the document with a transaction count less than the number passed in.
dataset(analyticsrec) findTransactionsBelowCount(INTEGER trans_count, INTEGER id) := EMBED(mongodb : user(username), password(password), server(server), database('sample_analytics'),  collection('transactions'))
    find_one({transaction_count: {$lt: $trans_count}, account_id: $id});
ENDEMBED;

// Returns the document matching the id that is passed in.
dataset(airbnbrec) findABByID(STRING id) := EMBED(mongodb : user(username), password(password), server(server), database('sample_airbnb'),  collection('listingsAndReviews'))
    find_one({_id: $id});
ENDEMBED;

// Returns a dataset. Finds the document where the beds and accomodates fields are higher than the argument values. Matches _id to ensure the same document gets returned.
dataset(airbnbrec) findABBeds(INTEGER beds, INTEGER accommodates, STRING id) := EMBED(mongodb : user(username), password(password), server(server), database('sample_airbnb'),  collection('listingsAndReviews'))
    find_one({beds: {$gte: $beds}, accommodates: {$gte: $accommodates}, _id: $id});
ENDEMBED;

// Returns the document matching the url that is in the embedded script.
dataset(airbnbrec) findABByListing(STRING url) := EMBED(mongodb : user(username), password(password), server(server), database('sample_airbnb'),  collection('listingsAndReviews'))
    find_one({listing_url: $url});
ENDEMBED;

// Returns the document matching the id that is passed in.
dataset(feesrec) findFeeByID(STRING id) := EMBED(mongodb : user(username), password(password), server(server), database('sample_airbnb'),  collection('listingsAndReviews'))
    find_one({_id: $id});
ENDEMBED;

// Returns a dataset. Finds all the documents where the beds and accomodates fields are higher than the argument values. Matches _id to ensure the same document gets returned.
dataset(feesrec) findFeebyBeds(INTEGER beds, INTEGER accommodates, STRING id) := EMBED(mongodb : user(username), password(password), server(server), database('sample_airbnb'),  collection('listingsAndReviews'))
    find_one({beds: {$gte: $beds}, accommodates: {$gte: $accommodates}, _id: $id});
ENDEMBED;

// Returns the document matching the url that is passed in.
dataset(feesrec) findFeeByListing(STRING url) := EMBED(mongodb : user(username), password(password), server(server), database('sample_airbnb'),  collection('listingsAndReviews'))
    find_one({listing_url: $url});
ENDEMBED;

SEQUENTIAL
(
    OUTPUT(findTransactionsByID(716662), NAMED('FindTransactionByID'));
    OUTPUT(findTransactionsByCount(100, 278866), NAMED('FindTransactionByCount'));
    OUTPUT(findTransactionsBelowCount(10, 852986), NAMED('FindTransactionBelowCount'));
    OUTPUT(findABByID('10030955'), NAMED('FindAirBnBByID'));
    OUTPUT(findABBeds(5, 7, '10006546'), NAMED('FindAirBnBByBeds'));
    OUTPUT(findABByListing('https://www.airbnb.com/rooms/10059244'), NAMED('FindAirBnBByListing'));
    OUTPUT(findFeeByID('10030955'), NAMED('FindFeesByID'));
    OUTPUT(findFeeByBeds(5, 7, '10006546'), NAMED('FindFeesByBeds'));
    OUTPUT(findFeeByListing('https://www.airbnb.com/rooms/10059244'), NAMED('FindFeesByListing'));
    OUTPUT('Done', NAMED('Status'));
)