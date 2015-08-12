/* ******************************************************************************
## HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.  All rights reserved.
 ******************************************************************************/

IMPORT $ AS Tutorial;
export IDX_PeopleByZIP := INDEX(Tutorial.File_TutorialPerson,{zip,fpos},'~tutorial::YN::PeopleByZipINDEX');