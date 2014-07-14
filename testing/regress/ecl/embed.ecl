integer c(integer val) := BEGINC++ return val-3; ENDC++;
c(10);

set of any s(set of any d) := BEGINC++
// extern void user2(bool & __isAllResult,size32_t & __lenResult,void * & __result,bool isAllD,size32_t  lenD,void * d) {
{
   rtlSetToSetX(__isAllResult, __lenResult, __result, isAllD, lenD, d);
}
ENDC++;
set of integer s1 := s([1,2,3]);
set of real s2 := s([1.0,2.0,3.0]);
s1;
s2;
