/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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

NFLDivisionRecord := RECORD
    NFLFile.Team;
    NFLFile.Division;
    GroupCount  := Count(GROUP);
    GroupSum    := SUM(GROUP,NFLfile.Salary_cap);
    GroupMin    := MIN(GROUP,NFLfile.Salary_cap);
    GroupMax    := MAX(GROUP,NFLfile.Salary_cap);
END;

NFLDivisionTable    := TABLE(NFLFile,NFLDivisionRecord,Division,Team);

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

output('------ CHOOSEN --- First 5');
output(CHOOSEN(NFLFile,5));
output('------ CHOOSEN --- First 1');
output(CHOOSEN(NFLFile,1));
output('------ CHOOSEN --- All 0');
output(CHOOSEN(NFLFile,0x7fffffff));

output('------ CHOOSESETS --- American, 2');
output(CHOOSESETS(NFLFile,Conference = 'American' => 2));

output('------ CHOOSESETS --- East, 1');
output(CHOOSESETS(NFLFile,Division = 'East' => 1));

output('------ CHOOSESETS --- East, 0 (none)');
output(CHOOSESETS(NFLFile,Division = 'East' => 0));
