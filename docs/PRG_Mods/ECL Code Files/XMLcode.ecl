//
//  Example code - use without restriction.  
//
IMPORT $;

ds := $.DeclareData.timezonesXML;
OUTPUT(ds);

StripIt(STRING str) := REGEXREPLACE('[\r\n\t]',str,'$1');
					 
RECORDOF(ds) DoStrip(ds L) := TRANSFORM
  SELF.code := StripIt(L.code);
  SELF.state := StripIt(L.state);
  SELF.description := StripIt(L.description);
  SELF.timezone := StripIt(L.timezone);
END;
StrippedRecs := PROJECT(ds,DoStrip(LEFT));

OUTPUT(StrippedRecs);

InterestingRecs := StrippedRecs((INTEGER)code BETWEEN 301 AND 303);
OUTPUT(InterestingRecs,{code,timezone},
	     '~PROGGUIDE::EXAMPLEDATA::OUT::timezones300',
			 XML('area',HEADING('<?xml version=1.0 ...?>\n<timezones>\n',
													'</timezones>')),OVERWRITE);

CRLF := (STRING)x'0D0A';
OutRec := RECORD
  STRING Line;
END;
OutRec DoComplexXML(InterestingRecs L) := TRANSFORM
SELF.Line := '  <area code="' + L.code + '">' + CRLF +
             '    <zone>' + L.timezone + '</zone>'+CRLF+
		 '  </area>';
END;
ComplexXML := PROJECT(InterestingRecs,DoComplexXML(LEFT));
OUTPUT(ComplexXML,,'~PROGGUIDE::EXAMPLEDATA::OUT::Complextimezones301',
       CSV(HEADING('<?xml version=1.0 ...?>'+CRLF+'<timezones>'+CRLF,
                   '</timezones>')),OVERWRITE);					
									 
									 
NewTimeZones := DATASET('~PROGGUIDE::EXAMPLEDATA::OUT::Complextimezones301',
												{STRING	area {XPATH('<>')}},
												XML('timezones/area'));
OUTPUT(NewTimeZones);

{ds.code, ds.timezone} Xform(NewTimeZones L) := TRANSFORM
  SELF.code := XMLTEXT('@code');
  SELF.timezone := XMLTEXT('zone');
END;
ParsedZones := PARSE(NewTimeZones,area,Xform(LEFT),XML('area'));

OUTPUT(ParsedZones);									 

//**********************************************************
CollateralRec := RECORD
  STRING Action        {XPATH('Action')}; 
  STRING Description   {XPATH('Description')}; 
  STRING EffectiveDate {XPATH('EffectiveDate')}; 
END;
PartyRec := RECORD
   STRING   PartyID         {XPATH('@entityId')}; 
   STRING   IsBusiness      {XPATH('IsBusiness')};
   STRING   OrgName         {XPATH('OrgName')};
   STRING   FirstName       {XPATH('FirstName')};
   STRING   LastName        {XPATH('LastName')};
   STRING   Status          {XPATH('Status[1]')};
   STRING   Address1        {XPATH('Address1')};
   STRING   Address2        {XPATH('Address2')};
   STRING   Address3        {XPATH('Address3')};
   STRING   Address4        {XPATH('Address4')};
   STRING   City            {XPATH('City')};
   STRING   State           {XPATH('State')};
   STRING   Zip             {XPATH('Zip')};
   STRING   OrgType         {XPATH('OrgType')};
   STRING   OrgJurisdiction {XPATH('OrgJurisdiction')};
   STRING   OrgID           {XPATH('OrgID')};
   STRING10 EnteredDate     {XPATH('EnteredDate')};
END;
TransactionRec := RECORD
   STRING            TransactionID  {XPATH('@ID')}; 
   STRING10          StartDate      {XPATH('StartDate')};   
   STRING10          LapseDate      {XPATH('LapseDate')};   
   STRING            FormType       {XPATH('FormType')};        
   STRING            AmendType      {XPATH('AmendType')};   
   STRING            AmendAction    {XPATH('AmendAction')};
   STRING10          EnteredDate    {XPATH('EnteredDate')};
   STRING10          ReceivedDate   {XPATH('ReceivedDate')};
   STRING10          ApprovedDate   {XPATH('ApprovedDate')};
   DATASET(PartyRec) Debtors        {XPATH('Debtor')};
   DATASET(PartyRec) SecuredParties {XPATH('SecuredParty')};
   CollateralRec     Collateral     {XPATH('Collateral')}
END;

UCC_Rec := RECORD 
   STRING                  FilingNumber {XPATH('@number')}; 
   DATASET(TransactionRec) Transactions {XPATH('Transaction')};
END; 

UCC := DATASET('~PROGGUIDE::EXAMPLEDATA::XML_UCC',UCC_Rec,XML('UCC/Filing')); 

XactTbl := TABLE(UCC,{INTEGER XactCount := COUNT(Transactions), UCC});
OUTPUT(XactTbl);

Out_Transacts := RECORD
   STRING            FilingNumber;
   STRING            TransactionID; 
   STRING10          StartDate;     
   STRING10          LapseDate;     
   STRING            FormType;         
   STRING            AmendType;     
   STRING            AmendAction;
   STRING10          EnteredDate;
   STRING10          ReceivedDate;
   STRING10          ApprovedDate;
   DATASET(PartyRec) Debtors;
   DATASET(PartyRec) SecuredParties;
   CollateralRec     Collateral;
END;
Out_Transacts Get_Transacts(XactTbl L, INTEGER C) := TRANSFORM
  SELF.FilingNumber  := L.FilingNumber;
  SELF               := L.Transactions[C]; 
END;

Transacts := NORMALIZE(XactTbl,
                       LEFT.XactCount,Get_Transacts(LEFT,COUNTER));
OUTPUT(Transacts);

PartyCounts := TABLE(Transacts,
                     {INTEGER DebtorCount := COUNT(Debtors), 
                      INTEGER PartyCount := COUNT(SecuredParties),
                      Transacts});
OUTPUT(PartyCounts);

Out_Parties := RECORD
   STRING   FilingNumber;
   STRING   TransactionID; 
	 PartyRec;
END;

Out_Parties Get_Debtors(PartyCounts L, INTEGER C) := TRANSFORM
  SELF.FilingNumber  := L.FilingNumber;
  SELF.TransactionID := L.TransactionID; 
  SELF               := L.Debtors[C]; 
END;
TransactDebtors := NORMALIZE( PartyCounts,
					LEFT.DebtorCount,
					Get_Debtors(LEFT,COUNTER));
OUTPUT(TransactDebtors);

Out_Parties Get_Parties(PartyCounts L, INTEGER C) := TRANSFORM
  SELF.FilingNumber  := L.FilingNumber;
  SELF.TransactionID := L.TransactionID; 
  SELF               := L.SecuredParties[C]; 
END;

TransactParties := NORMALIZE(PartyCounts,
                             LEFT.PartyCount,
                             Get_Parties(LEFT,COUNTER));
OUTPUT(TransactParties);
