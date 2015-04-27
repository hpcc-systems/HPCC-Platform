myuser := 'user' : STORED('db_user'); // DOESN'T WORK
mypass := 'pass';
mydb := 'db';
myserver := 'server';

statusRec := { unsigned id; string status; };
dataset(statusRec) getAllStatus() := EMBED(mysql : user(myuser),password(mypass),database(mydb),server(myserver))
	SELECT doc_id, status FROM etl_status;
ENDEMBED;


getAllStatus();
