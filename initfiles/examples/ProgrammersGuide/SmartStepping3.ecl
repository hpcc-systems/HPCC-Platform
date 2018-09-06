//
//  Example code - use without restriction.  
//
LinkRec := RECORD
	UNSIGNED1 Link;
END;
DS_Rec := RECORD(LinkRec)
  STRING10 Name;
  STRING10 Address;
END;
Child1_Rec := RECORD(LinkRec)
  UNSIGNED1 Nbr;
END;
Child2_Rec := RECORD(LinkRec)
  STRING10 Car;
END;
Child3_Rec := RECORD(LinkRec)
  UNSIGNED4 Salary;
END;
Child4_Rec := RECORD(LinkRec)
  STRING10 Domicile;
END;

ds := DATASET([{1,'Fred','123 Main'},{2,'George','456 High'},{3,'Charlie','789 Bank'},{4,'Danielle','246 Front'},{5,'Emily','613 Boca'},
							 {6,'Oscar','942 Frank'},{7,'Felix','777 John'},{8,'Adele','543 Bank'},{9,'Johan','123 Front'},{10,'Ludwig','212 Front'}],DS_Rec);

Child1 := DATASET([{1,5},{2,8},{3,11},{4,14},{5,17},
                   {6,20},{7,23},{8,26},{9,29},{10,32}],Child1_Rec);
Child2 := DATASET([{1,'Ford'},{2,'Ford'},{3,'Chevy'},{4,'Lexus'},{5,'Lexus'},
                   {6,'Kia'},{7,'Mercury'},{8,'Jeep'},{9,'Lexus'},{9,'Ferrari'},{10,'Ford'}],Child2_Rec);
Child3 := DATASET([{1,10000},{2,20000},{3,155000},{4,800000},{5,250000},
                   {6,75000},{7,200000},{8,15000},{9,80000},{10,25000}],Child3_Rec);
Child4 := DATASET([{1,'House'},{2,'House'},{3,'House'},{4,'Apt'},{5,'Apt'},
                   {6,'Apt'},{7,'Apt'},{8,'House'},{9,'Apt'},{10,'House'}],Child4_Rec);

TblRec := RECORD(LinkRec),MAXLENGTH(4096)
	UNSIGNED1 DS;
	UNSIGNED1 Matches := 0;
	UNSIGNED1 LastMatch := 0;
	SET OF UNSIGNED1 MatchDSs := [];
END;

Filter1 := Child1.Nbr % 2 = 0;
Filter2 := Child2.Car IN ['Ford','Chevy','Jeep'];
Filter3 := Child3.Salary < 100000;
Filter4 := Child4.Domicile = 'House';

t1 := PROJECT(Child1(Filter1),TRANSFORM(TblRec,SELF.DS:=1,SELF:=LEFT));
t2 := PROJECT(Child2(Filter2),TRANSFORM(TblRec,SELF.DS:=2,SELF:=LEFT));
t3 := PROJECT(Child3(Filter3),TRANSFORM(TblRec,SELF.DS:=3,SELF:=LEFT));
t4 := PROJECT(Child4(Filter4),TRANSFORM(TblRec,SELF.DS:=4,SELF:=LEFT));

SetDS := [t1,t2,t3,t4];

TblRec XF(TblRec L,DATASET(TblRec) Matches) := TRANSFORM
	SELF.Matches := COUNT(Matches);
	SELF.LastMatch := MAX(Matches,DS);
	SELF.MatchDSs := SET(Matches,DS);
	SELF := L;
END;
j1 := JOIN( SetDS,STEPPED(LEFT.Link=RIGHT.Link),XF(LEFT,ROWS(LEFT)),SORTED(Link));

OUTPUT(j1);

OUTPUT(ds(link IN SET(j1,link)));
