//
//  Example code - use without restriction.  
//
IMPORT $;
IMPORT Std;

s1 := $.DeclareData.Accounts(Account[1] = '1');
s2 := $.DeclareData.Accounts(Account[1] = '2');
s3 := $.DeclareData.Accounts(Account[1] = '3');
s4 := $.DeclareData.Accounts(Account[1] IN ['4','5','6','7','8','9']);
 
Rec := $.DeclareData.Layout_Accounts_Link;
RecPlus := {Rec,UNSIGNED8 RecPos{virtual(fileposition)}};
d1 := DATASET($.DeclareData.SubKey1,RecPlus,THOR);
d2 := DATASET($.DeclareData.SubKey2,RecPlus,THOR);
d3 := DATASET($.DeclareData.SubKey3,RecPlus,THOR);
d4 := DATASET($.DeclareData.SubKey4,RecPlus,THOR);

i1 := INDEX(d1,{PersonID},
					  {Account,OpenDate,IndustryCode,AcctType,AcctRate,Code1,Code2,HighCredit,Balance,RecPos},
						$.DeclareData.SubIDX1);
i2 := INDEX(d2,{PersonID},
					  {Account,OpenDate,IndustryCode,AcctType,AcctRate,Code1,Code2,HighCredit,Balance,RecPos},
						$.DeclareData.SubIDX2);
i3 := INDEX(d3,{PersonID},
					  {Account,OpenDate,IndustryCode,AcctType,AcctRate,Code1,Code2,HighCredit,Balance,RecPos},
						$.DeclareData.SubIDX3);
i4 := INDEX(d4,{PersonID},
					  {Account,OpenDate,IndustryCode,AcctType,AcctRate,Code1,Code2,HighCredit,Balance,RecPos},
						$.DeclareData.SubIDX4);
						
BldDat := PARALLEL(
				IF(~Std.File.FileExists($.DeclareData.SubKey1),
					 OUTPUT(s1,
					        {PersonID,Account,OpenDate,IndustryCode,AcctType,AcctRate,Code1,Code2,HighCredit,Balance},
									$.DeclareData.SubKey1)),

				IF(~Std.File.FileExists($.DeclareData.SubKey2),
					 OUTPUT(s2,{PersonID,Account,OpenDate,IndustryCode,AcctType,AcctRate,Code1,Code2,HighCredit,Balance},$.DeclareData.SubKey2)),

				IF(~Std.File.FileExists($.DeclareData.SubKey3),
					 OUTPUT(s3,{PersonID,Account,OpenDate,IndustryCode,AcctType,AcctRate,Code1,Code2,HighCredit,Balance},$.DeclareData.SubKey3)),

				IF(~Std.File.FileExists($.DeclareData.SubKey4),
					 OUTPUT(s4,{PersonID,Account,OpenDate,IndustryCode,AcctType,AcctRate,Code1,Code2,HighCredit,Balance},$.DeclareData.SubKey4)));

BldIDX := PARALLEL(
				IF(~Std.File.FileExists($.DeclareData.SubIDX1),
					 BUILDINDEX(i1)),

				IF(~Std.File.FileExists($.DeclareData.SubIDX2),
					 BUILDINDEX(i2)),

				IF(~Std.File.FileExists($.DeclareData.SubIDX3),
					 BUILDINDEX(i3)),

				IF(~Std.File.FileExists($.DeclareData.SubIDX4),
					 BUILDINDEX(i4)));

SEQUENTIAL(BldDat,BldIDX);


