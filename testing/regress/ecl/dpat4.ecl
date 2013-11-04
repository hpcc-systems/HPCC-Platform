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

export File_Actors := DATASET([
{'A.V., Subba Rao Chenchu Lakshmi (1958/I)  <10>'},
{' Jayabheri (1959)  <17>'},
{' Madalasa (1948)  <3>'},
{' Mangalya Balam (1958)  <12>'},
{' Mohini Bhasmasura (1938)  <3>'},
{' Palletoori Pilla (1950)  [Kampanna Dora]  <4>'},
{' Peddamanushulu (1954)  <6>'},
{' Sarangadhara (1957)  <12>'},
{' Sri Seetha Rama Kalyanam (1961)  <12>'},
{' Sri Venkateswara Mahatmyam (1960)  [Akasa Raju] <5>'},
{' Vara Vikrayam (1939)  [Judge]  <12>'},
{' Vindhyarani (1948)  <7>'},
{''},
{'Aa, Brynjar Adjo solidaritet (1985)  [Ponker]  <40>'},
{''},
{'Aabel, Andreas Bor Borson Jr. (1938)  [O.G. Hansen] <9>'},
{' Jeppe pa bjerget (1933)  [Enskomakerlaerling]'},
{' Kampen om tungtvannet (1948)  <8>'},
{' Prinsessen som ingen kunne maqlbinde (1932) [Espen Askeladd]  <3>'},
{' Spokelse forelsker seg, Et (1946)  [Et spokelse] <6>'},
{''},
{'Aabel, Hauk (I) Alexander den store (1917)  [Alexander Nyberg]'},
{' Du har lovet mig en kone! (1935)  [Professoren] <6>'},
{' Glad gutt, En (1932)  [Ola Nordistua]  <1>'},
{' Jeppe pa bjerget (1933)  [Jeppe]  <1>'},
{' Morderen uten ansikt (1936)'},
{' Store barnedapen, Den (1931)  [Evensen, kirketjener]  <5>'},
{' Troll-Elgen (1927)  [Piper, direktor]  <9>'},
{' Ungen (1938)  [Krestoffer]  <8>'},
{' Valfangare (1939)  [Jensen Sr.]  <4>'},
{''},
{'Aabel, Per (I) Brudebuketten (1953)  [Hoyland jr.] <3>'},
{' Cafajestes, Os (1962)'},
{' Farlige leken, Den (1942)  [Fredrik Holm, doktor]'},
{' Herre med bart, En (1942)  [Ole Grong, advokat] <1>'},
{' Kjaere Maren (1976)  [Doktor]'},
{' Kjaerlighet og vennskap (1941)  [Anton Schack] <3>'},
{' Ombyte fornojer (1939)  [Gregor Ivanow]  <2>'},
{' Portrettet (1954)  [Per Haug, provisordaj sdk as;lkdja skdj a;slkdja s;lkd ja;slkjda s;lkjas d;laksj dla;ksjdlkajahs dj haslkjdh asljkhdalskjhd alkshda jskhd akjshd aksjdh alksjhd jkas hd] <1>'}],{string inline});

pattern arb := PATTERN('[-!.,\t a-zA-Z0-9]')+;
pattern ws := [' ','\t']*;
pattern number := PATTERN('[0-9]')+;
pattern age := ws opt('(' number OPT('/I') ')');
pattern role := ws opt('[' arb ']');
pattern m_rank := ws opt('<' number '>');
pattern actor := opt(arb opt(ws '(I)' ws) '\t');
pattern line := ws actor arb age role m_rank ws;

layout_actor_movie := record
  string30 ActorName := matchtext(actor/arb);//stringlib.stringfilterout(matchtext(actor/arb),'\t');
  string50 movie_name:=matchtext(arb[2]);
  unsigned2  movie_year := (unsigned)matchtext(age/number);
  string20 movie_role := matchtext(role/arb);
  unsigned1 movie_rank := (unsigned)(matchtext(m_rank/number));
  end;

//Actor_Movie_Init := parse(accurint_test.File_Actors,IMDB_Actor_Desc,line,layout_actor_movie,whole,first);
d := dataset('~thor_data50::in::actors_list',{string inline},csv(separator('')));

Actor_Movie_Init := parse(File_Actors,inline,line,layout_actor_movie,whole,first);

// Iterate to propagate actor name
Layout_Actor_Movie PropagateName(Layout_Actor_Movie L, Layout_Actor_Movie R) := TRANSFORM
SELF.actorname := IF(R.actorname <> '', R.actorname, L.actorname);
SELF := R;
END;

Actor_Movie_Base := ITERATE(Actor_Movie_Init, PropagateName(LEFT, RIGHT));

//OUTPUT(CHOOSEN(Actor_Movie_Base,1000));
//count(Actor_Movie_Base)
OUTPUT(Actor_Movie_Init);
