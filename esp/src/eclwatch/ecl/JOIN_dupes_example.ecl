set1 := [1,2,3,4,5,6,7,8,9,10];
set2 := [10,20,30,40,50,60,70,80,90,100];

r1 := {integer1 fred};
r2 := {integer1 fred,integer1 sue};
ds1 := dataset(set1,r1);

ds2 := dataset(set2,r1);

r2 XF(ds1 L, ds2 R) := transform
  self.fred := L.fred;
  self.sue := R.fred;
end; 

j := JOIN(ds1,ds2,right.fred % 2 = 0,XF(left,right),all);

output(j)