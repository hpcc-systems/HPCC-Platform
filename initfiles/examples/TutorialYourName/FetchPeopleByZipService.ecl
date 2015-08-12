/* ******************************************************************************
## HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.  All rights reserved.
 ******************************************************************************/

IMPORT $ AS Tutorial;
STRING10 ZipFilter := '' : STORED('ZIPValue');
resultSet := FETCH(Tutorial.File_TutorialPerson,
                   Tutorial.IDX_PeopleByZIP(zip=ZipFilter),
                   RIGHT.fpos);
                   
OUTPUT(resultSet);


