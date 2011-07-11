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

RETURN MODULE

export MAC_Append_Id(infile,link_Field,append_field,patchfile,left_field,right_field,outfile) := macro

//-- Transform used by join below
//   If there is a new ID in the patchfile, replace the old ID.  Otherwise retain it.
#uniquename(tra)
typeof(infile) %tra%(infile l, patchfile rec) := transform
  self.append_field := if(rec.right_field<>0,rec.right_field,l.append_field); 
  self := l;
  end;



export MAC_Patch_Id(infile,patch_Field,patchfile,left_field,right_field,outfile) := macro
#uniquename(MPI)
    %MPI% := 5; // Simply forces macro to behave as such

//****** Call MAC_Append_Id to clean the infile based on patchfile
//       It replaces the old IDs with new IDs
    MAC_Append_Id(infile,patch_field,patch_field,patchfile,left_field,right_field,outfile)

    endmacro;


export integer4 MOB(integer l) := (INTEGER4)MAP ( l > 10000000 => l div 100, l );

import lib_stringlib;
sp(string s,integer1 n) := if( stringlib.stringfind(s,' ',n)=0, length(s), stringlib.stringfind(s,' ',n) );

export string word(string s,integer1 n) := if (n = 1, s[1..sp(s,1)],
                 s[sp(s,n-1)+1..sp(s,n)]);


EXPORT datalibx := 
SERVICE
unsigned integer4 SlidingMatch(string src, string arg) : c, pure, entrypoint='dataSlidingMatch';
unsigned integer4 PositionalMatch(string src, string search) : c, pure, entrypoint='dataPositionalMatch';
string120 CompanyClean(const string scr)  : c, pure, entrypoint='dataCompanyClean', hole;
 unsigned4 NameMatch(const string le_f, const string le_m, const string le_l,
                     const string ri_f,const string ri_m,const string ri_l)  : c, pure, entrypoint='dataNameMatch', hole;
END;

maxi(integer l, integer r) := if ( l > r, l, r );
mini(integer l,integer r) := if ( l > r, r, l);

c3(integer l, integer r, integer s) := (l-r)*2/3+r-s;

c3a(integer l, integer r, integer s) := round(100/l * c3(l,r,s));

export StringSimilar100(string l, string r) := 
c3a( maxi(length(trim(l)),length(trim(r))),
    mini(length(trim(l)),length(trim(r))),
    maxi( datalibx.slidingmatch(trim(l),trim(r)) , 
          datalibx.positionalmatch(trim(l),trim(r)))
);

export StringSimilar(string l, string r) := ROUND(StringSimilar100(l,r)/10);


export date_overlap_first(unsigned8 lf, unsigned8 ll, 
                         unsigned8 rf, unsigned8 rl) :=

  MAP ( lf > rl => 0,
        rf > ll => 0,
        lf > rf => lf,
        rf );

export date_overlap_last(unsigned8 lf, unsigned8 ll, 
                         unsigned8 rf, unsigned8 rl) :=

  MAP ( lf > rl => 0,
        rf > ll => 0,
        ll > rl => rl,
        ll );

export date_overlap(unsigned8 lf, unsigned8 ll, 
                         unsigned8 rf, unsigned8 rl) :=

  MAP( date_overlap_last(lf,ll,rf,rl)=0 => if (date_overlap_first(lf,ll,rf,rl)=0,0,1),
       date_overlap_first(lf,ll,rf,rl)=0 => 1,
       (date_overlap_last(lf,ll,rf,rl) div 100 - date_overlap_first(lf,ll,rf,rl) div 100) * 12 +
date_overlap_last(lf,ll,rf,rl)%100-date_overlap_first(lf,ll,rf,rl) % 100+1);

export Tails(string l, string r) := length(trim(l))>=length(trim(r)) and
l[length(trim(l))-length(trim(r))+1..length(l)] = r;

// Check that two strings are equal or one is blank
export NNEQ(string l, string r) := l='' or r='' or l=r;

export NameMatch(string f1,string m1, string l1, string f2, string m2, string l2 ) := datalibx.namematch(f1,m1,l1,f2,m2,l2);

export CleanCompany(string s) := trim(datalibx.companyclean(s)[1..40])+' '+datalibx.companyclean(s)[41..80];

export MAC_Split_Withdups_Local(infile,infield,thresh,outfile,remainder) := macro
  // similar to remove_withdups but assumes data is local and doesn't destroy that
  #uniquename(r)
%R% := record
  infile;
  integer8 cnt := 0;
end;
  #uniquename(p)
  #uniquename(tra)

%r% %tra%(infile lef) := transform
  self.cnt := 0;
  self := lef;
  end;

%p% := project(infile,%tra%(left));

#uniquename(grp)
%grp% := group(sort(%p%,infield,local),infield,local);

#uniquename(add_cnt)
%r% %add_cnt%(%R% lef,%R% ref) := transform
  self.cnt := lef.cnt+1;
  self := ref;
  end;
#uniquename(i)
%i% := group(sort(group(iterate(%grp%,%add_cnt%(left,right))),infield,-cnt,local),infield,local);

#uniquename(cpy)
%r% %cpy%(%R% lef,%R% ref) := transform
  self.cnt := IF(lef.cnt=0,ref.cnt,lef.cnt);
  self := ref;
  end;
#uniquename(i1)
%i1% := group(iterate(%i%,%cpy%(left,right)));

#uniquename(null_join)
typeof(infile) %null_join%(%r% l) := transform
  self := l;
  end;

outfile := project(%i1%(cnt<thresh),%null_join%(left));
remainder := project(%i1%(cnt>=thresh),%null_join%(left));

  endmacro;

/*  Comments:
Make multi-callable
*/
export MAC_Field_Count_Thresholded(infile,infield,thres,outname = '\'field_count\'',pct_wanted = 'false',hasoutputname = 'false', outputname = 'theoutput') := macro

//-- Record structure that will count the table by infield
#uniquename(give2)
%give2%(real8 a) := round(a * 100) / 100;

#uniquename(r_macro)
%r_macro% := record
  infield;
  integer cnt := count(group);
    #if(pct_wanted)
      real pct := %give2%(100 * count(group) / count(infile));
    #end
  end;

//****** Push infile into table that groups by infield, 
//       producing a count for each value using r_macro above
#uniquename(t_macro)
%t_macro% := table(infile,%r_macro%,infield,few);

//****** Output up to 5000 results to screen
#if(hasoutputname)
    outputname := output(choosen(%t_macro%(cnt>=thres),50000), NAMED(outname));
#else
    output(choosen(%t_macro%(cnt>=thres),50000), NAMED(outname));
#end

  endmacro;

  export MAC_Field_Count(infile,infield,outname = '\'field_count\'',pct_wanted = 'false',hasoutputname = 'false', outputname = 'theoutput') := macro
#uniquename(mac_mf)
%mac_mf% := 0; // turn into a proper macro
Mac_Field_Count_Thresholded(infile,infield,0,outname,pct_wanted,hasoutputname,outputname)
  endmacro;

  export Translate_Suffix(string s) :=
  MAP( (integer)s = 1 => 'I',
       (integer)s = 2 => 'II',
       (integer)s = 3 => 'III',
       (integer)s = 4 => 'IV',
       (integer)s = 5 => 'V',
       (integer)s = 6 => 'VI',
       (integer)s = 7 => 'VII',
       (integer)s = 8 => 'VIII',
       (integer)s = 9 => 'IX',
       s = 'IIII' => 'IV',
       s );

  END;
