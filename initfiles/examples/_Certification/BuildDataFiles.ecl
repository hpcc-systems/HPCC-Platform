/* ******************************************************************************
## Copyright (c) 2011 HPCC Systems.  All rights reserved.
 ******************************************************************************/

IMPORT Std;
IMPORT $ AS Certification;

Base := Certification.Setup.NodeMult1;     //max = 20
Mult := Certification.Setup.NodeMult2;     //max = 20
//base * Mult = number of millions of records to generate
//it's normal to generate 1 million recs/node
//maximum dataset size = 18,800,000,000 bytes (47 * 400 million)

MAC_Sequence_Records(infile,idfield,outfile) := macro
//-- Transform that assigns id field
//   Assigns idfield according to node.  
//   Sequential records on a node will have id fields that differ by the total number of nodes.
#uniquename(tra)
typeof(infile) %tra%(infile lef,infile ref) := transform
  self.idfield := if(lef.idfield=0,Std.System.Thorlib.node()+1,lef.idfield+Std.System.Thorlib.nodes());
  self := ref;
  end;

//****** Push infile through transform above
outfile := iterate(infile,%tra%(left,right),local);
  endmacro;


//define data 
fname_i := [
      #IF(BASE>=20) 'DAVID     ', #END #IF(BASE>=19) 'CLARE     ', #END
      #IF(BASE>=18) 'KELLY     ', #END #IF(BASE>=17) 'KIMBERLY  ', #END
      #IF(BASE>=16) 'PAMELA    ', #END #IF(BASE>=15) 'JEFFREY   ', #END
      #IF(BASE>=14) 'MATTHEW   ', #END #IF(BASE>=13) 'LUKE      ', #END
      #IF(BASE>=12) 'JOHN      ', #END #IF(BASE>=11) 'EDWARD    ', #END
      #IF(BASE>=10) 'CHAD      ', #END #IF(BASE>= 9) 'KEVIN     ', #END
      #IF(BASE>= 8) 'KOBE      ', #END #IF(BASE>= 7) 'RICHARD   ', #END
      #IF(BASE>= 6) 'GEORGE    ', #END #IF(BASE>= 5) 'FRED      ', #END
      #IF(BASE>= 4) 'SHELLEY   ', #END #IF(BASE>= 3) 'GWENDOLYN ', #END
      #IF(BASE>= 2) 'JAY       ', #END
                    'DIRK      '];
Lname_i := [
      #IF(MULT>=20) 'BAYLISS   ', #END #IF(MULT>=19) 'DOLSON    ', #END
      #IF(MULT>=18) 'BILLINGTON', #END #IF(MULT>=17) 'SMITH     ', #END
      #IF(MULT>=16) 'JONES     ', #END #IF(MULT>=15) 'ARMSTRONG ', #END
      #IF(MULT>=14) 'LINDHORFF ', #END #IF(MULT>=13) 'SIMMONS   ', #END
      #IF(MULT>=12) 'WYMAN     ', #END #IF(MULT>=11) 'MIDDLETON ', #END
      #IF(MULT>=10) 'MORTON    ', #END #IF(MULT>= 9) 'NOWITZKI  ', #END
      #IF(MULT>= 8) 'WILLIAMS  ', #END #IF(MULT>= 7) 'TAYLOR    ', #END
      #IF(MULT>= 6) 'CHAPMAN   ', #END #IF(MULT>= 5) 'GANN      ', #END
      #IF(MULT>= 4) 'LINDQUIST ', #END #IF(MULT>= 3) 'ORT       ', #END
      #IF(MULT>= 2) 'MOONDHRA  ', #END
                    'BRYANT    '];
PRANGE_i := [1,2,3,4,5,6,7,8,9,10];
street_i:= ['HIGH      ','MILL      ','CITATION  ','25TH      ','ELGIN     ',
            'VICARAGE  ','VICARYOUNG','PEPPERCORN','SILVER    ','KENSINGTON'];
ZIPS_i   := [11,12,13,14,15,16,17,18,19,20];
AGE_i    := [31,32,33,34,35,36,37,38,39,40];
BIRTH_STATE_i := ['FL','GA','SC','NC','TX','AL','MS','TN','KY','CA'];
BIRTH_MONTH_i := ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT'];

//DATASET declarations
BlankSet  := DATASET([{'','',0,'',0,0,'','',1,0}],Certification.Layout_FullFormat);

Certification.Layout_FullFormat norm1(Certification.Layout_FullFormat l, integer c) := transform
  self.fname := fname_i[c];
  self := l;
  end;
base_fn := normalize( BlankSet, Base, norm1(left, counter));

Certification.Layout_FullFormat norm2(Certification.Layout_FullFormat l, integer c) := transform
  self.lname := lname_i[c];
  self := l;
  end;
base_fln := normalize( base_fn, Mult, norm2(left, counter));

Certification.Layout_FullFormat norm3(Certification.Layout_FullFormat l, integer c) := transform
  self.PRANGE := PRANGE_i[c];
  self := l;
  end;
base_flpn := normalize( base_fln, 10, norm3(left, counter));

Certification.Layout_FullFormat norm4(Certification.Layout_FullFormat l, integer c) := transform
  self.street := street_i[c];
  self := l;
  end;
base_flpsn := normalize( base_flpn, 10, norm4(left, counter));

Certification.Layout_FullFormat norm5(Certification.Layout_FullFormat l, integer c) := transform
  self.zips := zips_i[c];
  self := l;
  end;
base_flpszn := normalize( base_flpsn, 10, norm5(left, counter));

Certification.Layout_FullFormat norm6(Certification.Layout_FullFormat l, integer c) := transform
  self.age := age_i[c];
  self := l;
  end;
base_flpszan := normalize( base_flpszn, 10, norm6(left, counter));

Certification.Layout_FullFormat add_b_normalize(base_flpszan l, integer c) := transform
  self.birth_state := BIRTH_STATE_i[c];
  self := l;
  end;
 
dist_base_flpszan := distribute(base_flpszan,hash32(fname,lname, prange,street,zips,age));
base_flpszanb := normalize(dist_base_flpszan, 10, add_b_normalize(left, counter));

Certification.Layout_FullFormat add_m_normalize(base_flpszanb l, integer c) := transform
  self.birth_month := BIRTH_MONTH_i[c];
  self := l;
  end;

dist_base_flpszanb := distribute(base_flpszanb,hash32(fname,lname, prange,street,zips,age,birth_state));
base_flpszanbm := normalize(dist_base_flpszanb, 10, add_m_normalize(left, counter));
dist_base_flpszanbm := distribute(base_flpszanbm,hash32(fname,lname, prange,street,zips,age,birth_state,birth_month));

MAC_Sequence_Records(dist_base_flpszanbm,id,base_flpszanbm_1)

output(base_flpszanbm_1,,Certification.Setup.filename,overwrite);
