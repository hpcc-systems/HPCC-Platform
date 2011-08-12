/* ******************************************************************************
## Copyright (c) 2011 HPCC Systems.  All rights reserved.
 ******************************************************************************/

IMPORT $ AS Tutorial;

ZipFilter :='33024'; 
FetchPeopleByZip := FETCH(Tutorial.File_TutorialPerson,Tutorial.IDX_PeopleByZIP(zip=ZipFilter),RIGHT.fpos); 
OUTPUT(FetchPeopleByZip);
