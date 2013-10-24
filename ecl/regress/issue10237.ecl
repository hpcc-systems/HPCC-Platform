#option ('applyInstantEclTransformations', true);
IMPORT STD;

PersFile := DATASET('Pers', { string id, string1 Gender, string city, string FirstName, String MiddleName, STRING LastName, STRING BirthDate }, thor);

calculateAge (STRING dateOfBirth) := FUNCTION
    DaysToday := STD.Date.FromGregorianDate(STD.Date.Today()); 
		DaysDOB   := STD.Date.FromGregorianDate((UNSIGNED4)dateOfBirth);
		RETURN (DaysToday - DaysDOB) DIV 365.25;
END;		

inputRecStruct := RECORD
	PersFile.Id;
	STRING50 Name;
	PersFile.Gender;
	PersFile.BirthDate;
	PersFile.City;
	INTEGER8 Age;
	INTEGER2 rankAssigned;
END;

personsWithBirthDate := PersFile(BirthDate <> '');

inputRecStruct populateAge(PersFile L) := TRANSFORM
	SELF.Name := L.FirstName + TRIM(' ' + L.MiddleName) + ' ' + L.LastName;
	SELF.Age := calculateAge(L.BirthDate);
	SELF := L;
	SELF := [];
END;

personsWithAge := PROJECT(personsWithBirthDate, populateAge(LEFT));

personsWithVotableAge := personsWithAge(age >= 18, City <> 'ABBEVILLE');

sortedPeopleList := SORT(personsWithVotableAge,City,-age);

groupedPeopleList := GROUP(sortedPeopleList,City);

inputRecStruct assignRank (inputRecStruct L, INTEGER C) := TRANSFORM
	SELF.rankAssigned := C;
	SELF := L;
END;

listWithRankAssigned := PROJECT(groupedPeopleList,assignRank(LEFT,COUNTER));

//OUTPUT(listWithRankAssigned,,'~RTTEST::GROUPPROJECTtest',overwrite);
OUTPUT(listWithRankAssigned); 