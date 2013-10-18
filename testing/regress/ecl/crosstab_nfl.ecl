/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

NFLTeamRecord := RECORD
    STRING20 Team;
    STRING20 Nickname;
    STRING20 Nickname_category;
    STRING20 Conference;
    STRING20 Division;
    REAL Salary_cap := 0;
END;

NFLFile := DATASET(
    [
    {'Buffalo','Bills','Animal','American','East',4.2},
    {'Miami','Dolphins','Animal','American','East',2.62},
    {'New England','Patriots','People','American','East',2},
    {'New York','Jets','Thing','American','East',0.855},
    {'Baltimore','Ravens','Animal','American','North',1.26},
    {'Cincinnati','Bengals','Animal','American','North',5.206},
    {'Cleveland','Browns','People','American','North',4.67},
    {'Pittsburgh','Steelers','People','American','North',2.61},
    {'Houston','Texans','People','American','South',6.45},
    {'Indianapolis','Colts','Animal','American','South',2.18},
    {'Jacksonville','Jaguars','Animal','American','South',5.53},
    {'Tennessee','Titans','People','American','South',2.13},
    {'Denver','Broncos','Animal','American','West',1.17},
    {'Kansas City','Chiefs','People','American','West',2.35},
    {'Oakland','Radiers','People','American','West',1.61},
    {'San Diego','Chargers','People','American','West',1.12},
    {'Dallas','Cowboys','People','National','East',3.26},
    {'New York','Giants','People','National','East',2.75},
    {'Philidelphia','Eagles','Animal','National','East',9.9},
    {'Washington','Redskins','People','National','East',3.34},
    {'Chicago','Bears','Animal','National','North',6.26},
    {'Detroit','Lions','Animal','National','North',4.839},
    {'Green Bay','Packers','People','National','North',3.29},
    {'Minnesota','Vikings','People','National','North',6.64},
    {'Atlanta','Falcons','Animal','National','South',3.36},
    {'Carolina','Panthers','Animal','National','South',0.976},
    {'New Orleans','Saints','People','National','South',2.86},
    {'Tampa Bay','Buccaneers','People','National','South',1.28},
    {'Arizona','Cardnials','Animal','National','West',6.15},
    {'Saint Louis','Rams','Animal','National','West',1.96},
    {'San Francisco','49ers','People','National','West',5.19},
    {'Seattle','Seahawks','Animal','National','West',1.304}
    ],NFLTeamRecord);

NFLConferenceRecord := RECORD
    NFLFile.Team;
    NFLFile.Conference;
    GroupCount  := Count(GROUP);
    GroupSum    := SUM(GROUP,NFLfile.Salary_cap);
    GroupMin    := MIN(GROUP,NFLfile.Salary_cap);
    GroupMax    := MAX(GROUP,NFLfile.Salary_cap);
END;

NFLConferenceTable := TABLE(NFLFile,NFLConferenceRecord,Conference,Team);

OUTPUT(NFLConferenceTable);

NFLDivisionRecord := RECORD
    NFLFile.Team;
    NFLFile.Division;
    GroupCount  := Count(GROUP);
    GroupSum    := SUM(GROUP,NFLfile.Salary_cap);
    GroupMin    := MIN(GROUP,NFLfile.Salary_cap);
    GroupMax    := MAX(GROUP,NFLfile.Salary_cap);
END;

NFLDivisionTable    := TABLE(NFLFile,NFLDivisionRecord,Division,Team);

OUTPUT(NFLDivisionTable);

NFLLeagueConferenceRecord := RECORD
    NFLFile.Conference;
    NFLFile.Division;
    GroupCount  := Count(GROUP);
    GroupSum    := SUM(GROUP,NFLfile.Salary_cap);
    GroupMin    := MIN(GROUP,NFLfile.Salary_cap);
    GroupMax    := MAX(GROUP,NFLfile.Salary_cap);
END;

NFLLeagueDivisionRecord := RECORD
    NFLFile.Division;
    NFLFile.Conference;
    GroupCount  := Count(GROUP);
    GroupSum    := SUM(GROUP,NFLfile.Salary_cap);
    GroupMin    := MIN(GROUP,NFLfile.Salary_cap);
    GroupMax    := MAX(GROUP,NFLfile.Salary_cap);
END;

NFLLeagueConferenceTable := TABLE(NFLFile,NFLLeagueConferenceRecord,Conference,Division);
NFLLeagueDivisionTable := TABLE(NFLFile,NFLLeagueDivisionRecord ,Division,Conference);

OUTPUT(NFLLeagueConferenceTable);
OUTPUT(NFLLeagueDivisionTable);

NFLLeagueCategoryConferenceRecord := RECORD
    NFLFile.NickName_category;
    NFLFile.Conference;
    GroupCount  := Count(GROUP);
    GroupSum    := SUM(GROUP,NFLfile.Salary_cap);
    GroupMin    := MIN(GROUP,NFLfile.Salary_cap);
    GroupMax    := MAX(GROUP,NFLfile.Salary_cap);
END;

NFLLeagueCategoryConferenceTable := TABLE(NFLFile,NFLLeagueCategoryConferenceRecord,Nickname_category,Conference);
OUTPUT(NFLLeagueCategoryConferenceTable);

NFLLeagueCategoryRecord := RECORD
    NFLFile.NickName_category;
    GroupCount  := Count(GROUP);
    GroupSum    := SUM(GROUP,NFLfile.Salary_cap);
    GroupMin    := MIN(GROUP,NFLfile.Salary_cap);
    GroupMax    := MAX(GROUP,NFLfile.Salary_cap);
END;

NFLLeagueCategoryTable := TABLE(NFLFile,NFLLeagueCategoryRecord,Nickname_category);
OUTPUT(NFLLeagueCategoryTable);


