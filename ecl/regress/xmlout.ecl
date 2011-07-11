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

//define constants
MaxElement := 2;    //maximum (1 to 16) number of array elements to use building the data records
MaxField   := 2;    //maximum (1 to 8) number of fields to use building the data records
FileOut    := 'RTTEMP::TestFile_'+MaxElement+'_'+MaxField+'_';
//define data atoms
SET OF STRING10 Fnames := ['DAVID','CLARE','KELLY','KIMBERLY','PAMELA','JEFFREY','MATTHEW','LUKE',
                          'JOHN','EDWARD','CHAD','KEVIN','KOBE','RICHARD','GEORGE','DIRK']; //max 16 recs

SET OF STRING10 Lnames := ['BAYLISS','DOLSON','BILLINGTON','SMITH','JONES','ARMSTRONG','LINDHORFF',
                          'SIMMONS','WYMAN','MIDDLETON','MORTON','NOWITZKI','WILLIAMS','TAYLOR','CHAPMAN','BRYANT']; //max 256 recs

SET OF UNSIGNED1 PRANGES := [1,2,3,4,5,6,7,8,
                             9,10,11,12,13,14,15,16]; //max 4096 recs
SET OF STRING10 Streets := ['HIGH','MILL','CITATION','25TH','ELGIN',
                            'VICARAGE','VICARYOUNG','PEPPERCORN','SILVER','KENSINGTON']; //max 65,536 recs

SET OF UNSIGNED1 ZIPS := [101,102,103,104,105,106,107,108,
                          109,110,111,112,113,114,115,116]; //max 1,048,576 recs

SET OF UNSIGNED1 AGES := [31,32,33,34,35,36,37,38,
                          39,40,41,42,43,44,45,56]; //max 16,777,216 recs

SET OF STRING2 STATES := ['FL','GA','SC','NC','TX','AL','MS','TN',
                          'KY','CA','MI','OH','IN','IL','WI','MN'];  //max 268,435,456 recs

SET OF STRING3 MONTHS := ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG',
                          'SEP','OCT','NOV','DEC','ABC','DEF','GHI','JKL'];  //max 4,294,967,296 recs

//record structure
#IF(MaxField=1) 
OutRec := {string10 firstname}; 
BlankSet := dataset([{''}],OutRec);
#end
#IF(MaxField=2) 
OutRec := {string10 firstname; string10  lastname}; 
BlankSet := dataset([{'',''}],OutRec);
#end
#IF(MaxField=3) 
OutRec := {string10 firstname; string10  lastname; unsigned1 prange}; 
BlankSet := dataset([{'','',0}],OutRec);
#end
#IF(MaxField=4) 
OutRec := {string10 firstname; string10  lastname; unsigned1 prange; string10  street}; 
BlankSet := dataset([{'','',0,''}],OutRec);
#end
#IF(MaxField=5) 
OutRec := {string10 firstname; string10  lastname; unsigned1 prange; string10  street; unsigned1 zip}; 
BlankSet := dataset([{'','',0,'',0}],OutRec);
#end
#IF(MaxField=6) 
OutRec := {string10 firstname; string10  lastname; unsigned1 prange; string10  street; unsigned1 zip; unsigned1 age}; 
BlankSet := dataset([{'','',0,'',0,0}],OutRec);
#end
#IF(MaxField=7) 
OutRec := {string10 firstname; string10  lastname; unsigned1 prange; string10  street; unsigned1 zip; unsigned1 age; string2 state}; 
BlankSet := dataset([{'','',0,'',0,0,''}],OutRec);
#end
#IF(MaxField=8) 
OutRec := {string10 firstname; string10  lastname; unsigned1 prange; string10  street; unsigned1 zip; unsigned1 age; string2 state; string3 month}; 
BlankSet := dataset([{'','',0,'',0,0,'',''}],OutRec);
#end

//build records
OutRec norm1(OutRec l, integer c) := transform
  self.firstname := Fnames[c];
  self := l;
  end;
Norm1Recs := normalize( BlankSet, MaxElement, norm1(left, counter));

#if(MaxField >= 2)
OutRec norm2(OutRec l, integer c) := transform
  self.lastname := Lnames[c];
  self := l;
  end;
Norm2Recs := normalize( Norm1Recs, MaxElement, norm2(left, counter));
#end
#if(MaxField >= 3)
OutRec norm3(OutRec l, integer c) := transform
  self.prange := pranges[c];
  self := l;
  end;
Norm3Recs := normalize( Norm2Recs, MaxElement, norm3(left, counter));
#end
#if(MaxField >= 4)
OutRec norm4(OutRec l, integer c) := transform
  self.street := streets[c];
  self := l;
  end;
Norm4Recs := normalize( Norm3Recs, MaxElement, norm4(left, counter));
#end
#if(MaxField >= 5)
OutRec norm5(OutRec l, integer c) := transform
  self.zip := zips[c];
  self := l;
  end;
Norm5Recs := normalize( distribute(Norm4Recs,hash(firstname,lastname)), MaxElement, norm5(left, counter));
#end
#if(MaxField >= 6)
OutRec norm6(OutRec l, integer c) := transform
  self.age := ages[c];
  self := l;
  end;
Norm6Recs := normalize( distribute(Norm5Recs,hash(firstname,lastname)), MaxElement, norm6(left, counter));
#end
#if(MaxField >= 7)
OutRec norm7(OutRec l, integer c) := transform
  self.state := states[c];
  self := l;
  end;
Norm7Recs := normalize( distribute(Norm6Recs,hash(firstname,lastname)), MaxElement, norm7(left, counter));
#end
#if(MaxField >= 8)
OutRec norm8(OutRec l, integer c) := transform
  self.month := months[c];
  self := l;
  end;
Norm8Recs := normalize( distribute(Norm7Recs,hash(firstname,lastname)), MaxElement, norm8(left, counter));
#end

#IF(MaxField=1) output(Norm1Recs,,FileOut+'FLAT',overwrite); #end
#IF(MaxField=2) output(Norm2Recs,,FileOut+'FLAT',overwrite); #end
#IF(MaxField=3) output(Norm3Recs,,FileOut+'FLAT',overwrite); #end
#IF(MaxField=4) output(Norm4Recs,,FileOut+'FLAT',overwrite); #end
#IF(MaxField=5) output(Norm5Recs,,FileOut+'FLAT',overwrite); #end
#IF(MaxField=6) output(Norm6Recs,,FileOut+'FLAT',overwrite); #end
#IF(MaxField=7) output(Norm7Recs,,FileOut+'FLAT',overwrite); #end
#IF(MaxField=8) output(Norm8Recs,,FileOut+'FLAT',overwrite); #end

#IF(MaxField=1) output(Norm1Recs,,FileOut+'CSV',CSV,overwrite); #end
#IF(MaxField=2) output(Norm2Recs,,FileOut+'CSV',CSV,overwrite); #end
#IF(MaxField=3) output(Norm3Recs,,FileOut+'CSV',CSV,overwrite); #end
#IF(MaxField=4) output(Norm4Recs,,FileOut+'CSV',CSV,overwrite); #end
#IF(MaxField=5) output(Norm5Recs,,FileOut+'CSV',CSV,overwrite); #end
#IF(MaxField=6) output(Norm6Recs,,FileOut+'CSV',CSV,overwrite); #end
#IF(MaxField=7) output(Norm7Recs,,FileOut+'CSV',CSV,overwrite); #end
#IF(MaxField=8) output(Norm8Recs,,FileOut+'CSV',CSV,overwrite); #end

#IF(MaxField=1) output(Norm1Recs,,FileOut+'XML',XML,overwrite); #end
#IF(MaxField=2) output(Norm2Recs,,FileOut+'XML',XML,overwrite); #end
#IF(MaxField=3) output(Norm3Recs,,FileOut+'XML',XML,overwrite); #end
#IF(MaxField=4) output(Norm4Recs,,FileOut+'XML',XML,overwrite); #end
#IF(MaxField=5) output(Norm5Recs,,FileOut+'XML',XML,overwrite); #end
#IF(MaxField=6) output(Norm6Recs,,FileOut+'XML',XML,overwrite); #end
#IF(MaxField=7) output(Norm7Recs,,FileOut+'XML',XML,overwrite); #end
#IF(MaxField=8) output(Norm8Recs,,FileOut+'XML',XML,overwrite); #end


