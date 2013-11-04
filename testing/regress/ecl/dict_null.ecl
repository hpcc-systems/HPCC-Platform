ds := dataset([], { string key, string value} );
ds2 := dataset([], { string key, string value} ) : stored('mystery');

dict1 := dictionary(ds, {key => value});
dict2 := dictionary([], {STRING key => STRING value { default('here')} });
dict3 := dictionary(ds2, {key => value});


dict1['s'].value;
exists(dict1);
count(dict1);
's' IN dict1;

dict2['s'].value;
exists(dict2);
count(dict2);
's' IN dict2;

dict3['s'].value;
exists(dict3);
count(dict3);
's' IN dict3;
