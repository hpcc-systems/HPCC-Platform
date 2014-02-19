<Archive build="community_4.2.0-1" eclVersion="4.2.0" legacyMode="0">
 <Query attributePath="_local_directory_.TFR985C"/>
 <Module key="_local_directory_" name="_local_directory_">
  <Attribute key="tfr985c" name="TFR985C" sourcePath="C:\Users\muhareex\AppData\Local\Temp\TFR985C.tmp">
   IMPORT * FROM ML;:
   IMPORT GH;
A := dataset([{1,1,12.0},{2,1,6.0},{3,1,4.0},
              {1,2,6.0},{2,2,167.0},{3,2,24.0},
                {1,3,4.0},{2,3,24.0},{3,3,-41.0}], ML.MAT.Types.Element);

qr_comp := ENUM ( Q = 1,  R = 2 );
DATASET(ML.MAT.Types.Element) QR(DATASET(ML.MAT.Types.Element) matrix) := FUNCTION

    n := ML.MAT.Has(matrix).Stats.XMax;
    loopBody(DATASET( ML.MAT.Types.MUElement) ds, UNSIGNED4 k) := FUNCTION
        Q := ML.MAT.MU.From(ds, qr_comp.Q);
        R0 := ML.MAT.MU.from(ds, qr_comp.R);
        R := R0;//WHEN(R0, output(R0,named('R'), EXTEND));
        t1 := ML.MAT.Vec.FromCol(R,k);
        t1x := t1;//WHEN(t1, output(t1,named('t1'), EXTEND));
        hModule := ML.MAT.Householder(t1x,k,n);
        hM1 := hModule.Reflection(hModule.HTA.Atkinson);
        hM := hM1;//WHEN(hM1, output(hM1,named('HM'), EXTEND));
        R1A := ML.MAT.Mu.To(ML.MAT.Mul(hM,R), qr_comp.R);
        Q1A := ML.MAT.Mu.To(ML.MAT.Mul(Q, hM), qr_comp.Q);
        #if (2)
        R1 := R1A;
        Q1 := Q1A;
        #else
        R1 := GH.Probe(R1A, k, 'R1');
        Q1 := GH.Probe(Q1A, k, 'Q1');
        #end
    RETURN R1+Q1;
  END;
    i1 := ML.MAT.Mu.To(matrix, qr_comp.R);
    i2 := ML.MAT.Mu.To(ML.MAT.Identity(n), qr_comp.Q);
    i := i1 + i2;
    RETURN LOOP(i, NOFOLD(2), loopBody(ROWS(LEFT),COUNTER));
END;

QComp(DATASET(ML.MAT.Types.Element) matrix) := QR(matrix);
Q0 := QComp(A);
OUTPUT(SORT(Q0, no, x, y, value)(value != 0), named(&apos;Q0&apos;));&#9;&#9;&#9;
  </Attribute>
 </Module>
 <Module key="gh" name="GH">
  <Attribute key="probe" name="probe">
  EXPORT probe(a, c, b) := FUNCTIONMACRO
     import std.system.thorlib;
     o := OUTPUT(ds(x=2 and y=2), { ds, cnt := c, node := thorlib.node() }, named(b), EXTEND);
     RETURN WHEN(ds, o);
  ENDMACRO;
  </Attribute>
 </Module>
 <Module key="ml" name="ML">
  <Attribute key="config" name="Config" sourcePath="C:\DATA\Documents\GitHub\ecl-ml\ML\Config.ecl">
   // Some configuration constants; tweaking these numbers to fit your system may help performance
EXPORT Config := MODULE
  EXPORT MaxLookup := 1000000000; // At most 1GB of lookup data
    EXPORT Discrete := 10; // Default number of groups to discretize things into
    EXPORT RoundingError := 0.000000001;
    //EXPORT RoundingError := 0.0001;
END;
  </Attribute>
 </Module>
 <Module key="ml.mat" name="ML.Mat">
  <Attribute key="types" name="Types" sourcePath="C:\DATA\Documents\GitHub\ecl-ml\ML\Mat\Types.ecl">
   EXPORT Types := MODULE

// Note - indices will start at 1; 0 is going to be used as a null
EXPORT t_Index := UNSIGNED4; // Supports matrices with up to 9B as the largest dimension
EXPORT t_value := REAL8;
EXPORT t_mu_no := UNSIGNED2; // Allow up to 64K matrices in one universe

EXPORT Element := RECORD
  t_Index x; // X is rows
    t_Index y; // Y is columns
    t_value value;
END;

EXPORT VecElement := RECORD
  t_Index x; // a vector does not necessarily lay upon any given dimension
    t_Index y; // y will always be 1
    t_value value;
  END;

EXPORT MUElement := RECORD(Element)
    t_mu_no no; // The number of the matrix within the universe
END;

END;
  </Attribute>
  <Attribute key="has" name="Has" sourcePath="C:\DATA\Documents\GitHub\ecl-ml\ML\Mat\Has.ecl">
   // Matrix Properties
IMPORT * FROM $;
EXPORT Has(DATASET(Types.Element) d) := MODULE

r := RECORD
  UNSIGNED NElements := COUNT(GROUP);
    UNSIGNED XMax := MAX(GROUP,d.x);
    UNSIGNED YMax := MAX(GROUP,d.y);
  END;

EXPORT Stats := TABLE(d,r)[1];

// The largest dimension of the matrix
EXPORT Dimension := MAX(Stats.XMax,Stats.YMax);

// The percentage of the sparse matrix that is actually there
EXPORT Density := Stats.NElements / (Stats.XMax*Stats.YMax);

EXPORT Norm := SQRT(SUM(Each.Mul(d,d),value));

r := RECORD
  Types.t_Index x := d.x ;
    Types.t_Index y := 1;
    Types.t_Value value := AVE(GROUP,d.value);
END;

// MeanRow is a column vector containing the mean value of each row.
EXPORT MeanRow := TABLE(d,r,d.x);

r := RECORD
  Types.t_Index x := 1 ;
    Types.t_Index y := d.y;
    Types.t_Value value := AVE(GROUP,d.value);
END;

// MeanCol is a row vector containing the mean value of each column.
EXPORT MeanCol := TABLE(d,r,d.y);

END;
  </Attribute>
  <Attribute key="each" name="Each" sourcePath="C:\DATA\Documents\GitHub\ecl-ml\ML\Mat\Each.ecl">
   // Element-wise matrix operations
IMPORT ML.Mat.Types;
EXPORT Each := MODULE

EXPORT Sqrt(DATASET(Types.Element) d) := FUNCTION
  Types.Element fn(d le) := TRANSFORM
      SELF.value := sqrt(le.value);
      SELF := le;
    END;
    RETURN PROJECT(d,fn(LEFT));
END;

EXPORT Exp(DATASET(Types.Element) d) := FUNCTION
  Types.Element fn(d le) := TRANSFORM
      SELF.value := exp(le.value);
      SELF := le;
    END;
    RETURN PROJECT(d,fn(LEFT));
END;

EXPORT Abs(DATASET(Types.Element) d) := FUNCTION
  Types.Element fn(d le) := TRANSFORM
      SELF.value := Abs(le.value);
      SELF := le;
    END;
    RETURN PROJECT(d,fn(LEFT));
END;

EXPORT Mul(DATASET(Types.Element) l,DATASET(Types.Element) r) := FUNCTION
// Only slight nastiness is that these matrices may be sparse - so either side could be null
Types.Element Multiply(l le,r ri) := TRANSFORM
    SELF.x := le.x ;
    SELF.y := le.y ;
      SELF.value := le.value * ri.value;
  END;
    RETURN JOIN(l,r,LEFT.x=RIGHT.x AND LEFT.y=RIGHT.y,Multiply(LEFT,RIGHT));
END;


// matrix .+ scalar
EXPORT Add(DATASET(Types.Element) d,Types.t_Value scalar) := FUNCTION
  Types.Element add(d le) := TRANSFORM
      SELF.value := le.value + scalar;
      SELF := le;
    END;
    RETURN PROJECT(d,add(LEFT));
END;

/*
 factor ./ matrix    ;
*/
EXPORT Reciprocal(DATASET(Types.Element) d, Types.t_Value factor=1) := FUNCTION
  Types.Element divide(d le) := TRANSFORM
      SELF.value := factor / le.value;
      SELF := le;
    END;
    RETURN PROJECT(d,divide(LEFT));
 END;

END;
  </Attribute>
  <Attribute key="mu" name="MU" sourcePath="C:\DATA\Documents\GitHub\ecl-ml\ML\Mat\MU.ecl">
   IMPORT * FROM $;

EXPORT MU := MODULE

// These fundamental (but trivial) routines move a regular matrix in and out of matrix-universe format
// The matrix universe exists to allow multiple matrices to co-reside inside one dataflow
// This eases passing of them in and out of functions - but also reduces the number of operations required to co-locate elements
EXPORT To(DATASET(Types.Element) d, Types.t_mu_no num) := PROJECT(d, TRANSFORM(Types.MUElement, SELF.no := num, SELF := LEFT));
EXPORT From(DATASET(Types.MUElement) d, Types.t_mu_no num) := PROJECT(d(no=num), TRANSFORM(Types.Element, SELF := LEFT));

  END;
  </Attribute>
  <Attribute key="householder" name="Householder" sourcePath="C:\DATA\Documents\GitHub\ecl-ml\ML\Mat\Householder.ecl">
   IMPORT * FROM $;
EXPORT Householder(DATASET(Types.VecElement) X, Types.t_Index k, Types.t_Index Dim=1) := MODULE

    /*
        HTA - Householder Transformation Algorithm
            Computes the Householder reflection matrix for use with QR decomposition

            Input:  vector X, k an index &lt; length(X)
            Output: a matrix H that annihilates entries in the product H*X below index k
    */
  EXPORT HTA := MODULE
       EXPORT Default := MODULE,VIRTUAL
          EXPORT IdentityM := IF(Dim&gt;Vec.Length(X), Identity(Dim), Identity(Vec.Length(X)));
            EXPORT DATASET(Types.Element) hReflection := DATASET([],Types.Element);

         END;

          // Householder Vector
            HouseV(DATASET(Types.VecElement) X, Types.t_Index k) := FUNCTION
                xk := X(x=k)[1].value;
                alpha := IF(xk&gt;=0, -Vec.Norm(X), Vec.Norm(X));
                vk := IF (alpha=0, 1, SQRT(0.5*(1-xk/alpha)));
                p := - alpha * vk;
                RETURN PROJECT(X, TRANSFORM(Types.VecElement,SELF.value := IF(LEFT.x=k, vk, LEFT.value/(2*p)), SELF :=LEFT));
            END;

         // Source: Atkinson, Section 9.3, p. 611
         EXPORT Atkinson := MODULE(Default)
                hV0 := HouseV(X(x&gt;=k),k);
                hV := hV0;//WHEN(hV0, output(hV0,named('HV0'), EXTEND));
                houseVec0 := Vec.ToCol(hV, 1);
                houseVec := WHEN(houseVec0, output(houseVec0,named('houseVec'), EXTEND));
                thouse0 := Trans(houseVec);
                thouse := WHEN(thouse0, output(thouse0,named('thouse'), EXTEND));
                m0 := MulXXX(houseVec,thouse);
                m := WHEN(m0, output(sort(m0,x,y),named('m'), EXTEND));
                value0 := Scale(m, 2);
                value := WHEN(value0, output(sort(value0,x,y),named('value'), EXTEND));
                EXPORT DATASET(Types.Element) hReflection := Sub(IdentityM, value);
         END;

         // Source: Golub and Van Loan, &quot;Matrix Computations&quot; p. 210
         EXPORT Golub := MODULE(Default)
                VkValue := X(x=k)[1].value;
                VkPlus := X(x&gt;k);
                sigma := Vec.Dot(VkPlus, VkPlus);

                mu := SQRT(VkValue*VkValue + sigma);
                newVkValue := IF(sigma=0,1,IF(VkValue&lt;=0, VkValue-mu, -sigma/(VkValue+mu) ));
                beta := IF( sigma=0, 0, 2*(newVkValue*newVkValue)/(sigma + (newVkValue*newVkValue)));

                newVkElem0 := X[1];
                newVkElem := PROJECT(newVkElem0,TRANSFORM(Types.Element,SELF.x:=k,SELF.y:=1,SELF.value := newVkValue));

                hV := PROJECT(newVkElem + VkPlus,TRANSFORM(Types.Element,SELF.value:=LEFT.value/newVkValue, SELF := LEFT));
                EXPORT DATASET(Types.Element) hReflection := Sub(IdentityM, Scale(Mul(hV,Trans(hV)),Beta));
         END;

    END;

    EXPORT Reflection(HTA.Default Control = HTA.Golub) := FUNCTION
        RETURN Control.hReflection;
    END;

END;
  </Attribute>
  <Attribute key="vec" name="Vec" sourcePath="C:\DATA\Documents\GitHub\ecl-ml\ML\Mat\Vec.ecl">
   IMPORT * FROM $;
// This module exists to handle a special sub-case of a matrix; the Vector
// The vector is really just a matrix with only one dimension
EXPORT Vec := MODULE

// Create a vector from &apos;thin air&apos; - with N entries each of value def
EXPORT From(Types.t_Index N,Types.t_value def = 1.0) := FUNCTION
    seed := DATASET([{0,0,0}], Types.VecElement);
  PerCluster := ROUNDUP(N/CLUSTERSIZE);
    Types.VecElement addNodeNum(Types.VecElement L, UNSIGNED4 c) := transform
    SELF.x := (c-1)*PerCluster;
        SELF.y := 1;
        SELF.value := DEF;
  END;
    // Create eventual vector across all nodes (in case it is huge)
    one_per_node := DISTRIBUTE(NORMALIZE(seed, CLUSTERSIZE, addNodeNum(LEFT, COUNTER)), x DIV PerCluster);

    Types.VecElement fillRow(Types.VecElement L, UNSIGNED4 c) := TRANSFORM
        SELF.x := l.x+c;
        SELF.y := 1;
        SELF.value := def;
    END;
    // Now generate on each node; filter &apos;clips&apos; of the possible extra &apos;1&apos; generated on some nodes
    m := NORMALIZE(one_per_node, PerCluster, fillRow(LEFT,COUNTER))(x &lt;= N);
    RETURN m;

END;

// The &apos;To&apos; routines - used to create a matrix from a vector
// Fill in the leading diagonal of a matrix starting with the vector element N
EXPORT ToDiag(DATASET(Types.VecElement) v, Types.t_index N=1) := PROJECT( v(x&gt;=N), TRANSFORM(Types.Element,SELF.x:=LEFT.x-N+1,SELF.y:=LEFT.x-N+1,SELF := LEFT));

// Fill in the upper diagonal of a matrix starting with the vector element N
EXPORT ToLowerDiag(DATASET(Types.VecElement) v, Types.t_index N=1) := PROJECT( v(x&gt;=N), TRANSFORM(Types.Element,SELF.x:=LEFT.x-N+2,SELF.y:=LEFT.x-N+1,SELF := LEFT));

// Fill in the Lower diagonal of a matrix starting with the vector element N
EXPORT ToUpperDiag(DATASET(Types.VecElement) v, Types.t_index N=1) := PROJECT( v(x&gt;=N), TRANSFORM(Types.Element,SELF.x:=LEFT.x-N+1,SELF.y:=LEFT.x-N+2,SELF := LEFT));

// Fill in a column
EXPORT ToCol(DATASET(Types.VecElement) v,Types.t_index N) := PROJECT( v, TRANSFORM(Types.Element,SELF.x:=LEFT.x,SELF.y:=N,SELF := LEFT));

// Fill in a row
EXPORT ToRow(DATASET(Types.VecElement) v,Types.t_index N) := PROJECT( v, TRANSFORM(Types.Element,SELF.y:=LEFT.x,SELF.x:=N,SELF := LEFT));

// The &apos;Rep&apos; routines - used to replace part of a matrix with a vector

EXPORT RepDiag(DATASET(Types.Element) M, DATASET(Types.VecElement) v, Types.t_index N=1) := M(X&lt;&gt;Y)+ ToDiag(v, N);

EXPORT RepCol(DATASET(Types.Element) M,DATASET(Types.VecElement) v,Types.t_index N) := M(Y&lt;&gt;N)+ToCol(v, N);

EXPORT RepRow(DATASET(Types.Element) M,DATASET(Types.VecElement) v,Types.t_index N) := M(X&lt;&gt;N)+ToRow(v, N);

// The &apos;From&apos; routines - extract a vector from part of a matrix
// FromDiag returns a vector formed from the elements of the Kth diagonal of M
EXPORT FromDiag(DATASET(Types.Element) M, INTEGER4 K=0) := PROJECT( M(x=y-k), TRANSFORM(Types.VecElement,SELF.x:=IF(K&lt;0,LEFT.y,LEFT.x),SELF.y:=1,SELF := LEFT));

EXPORT FromCol(DATASET(Types.Element) M,Types.t_index N) := PROJECT( M(Y=N), TRANSFORM(Types.VecElement,SELF.x:=LEFT.x,SELF.y:=1,SELF := LEFT));

EXPORT FromRow(DATASET(Types.Element) M,Types.t_index N) := PROJECT( M(X=N), TRANSFORM(Types.VecElement,SELF.x:=LEFT.y,SELF.y:=1,SELF := LEFT));

// Vector math
// Compute the dot product of two vectors
EXPORT Dot(DATASET(Types.VecElement) X,DATASET(Types.VecElement) Y) := FUNCTION
  J := JOIN(x,y,LEFT.x=RIGHT.x,TRANSFORM(Types.VecElement,SELF.x := LEFT.x, SELF.value := LEFT.value*RIGHT.value, SELF:=LEFT));
    RETURN SUM(J,value);
END;

EXPORT Norm(DATASET(Types.VecElement) X) := SQRT(Dot(X,X));

EXPORT Length(DATASET(Types.VecElement) X) := Has(X).Dimension;

END;
  </Attribute>
  <Attribute key="identity" name="Identity" sourcePath="C:\DATA\Documents\GitHub\ecl-ml\ML\Mat\Identity.ecl">
   IMPORT * FROM $;

EXPORT Identity(UNSIGNED4 dimension) := Vec.ToDiag( Vec.From(dimension,1.0) );
  </Attribute>
  <Attribute key="sub" name="Sub" sourcePath="C:\DATA\Documents\GitHub\ecl-ml\ML\Mat\Sub.ecl">
   IMPORT * FROM ML.Mat;
EXPORT Sub(DATASET(Types.Element) l,DATASET(Types.Element) r) := FUNCTION
StatsL := Has(l).Stats;
StatsR := Has(r).Stats;
SizeMatch := ~Strict OR (StatsL.XMax=StatsR.XMax AND StatsL.YMax=StatsR.YMax);

// Only slight nastiness is that these matrices may be sparse - so either side could be null
Types.Element Su(l le,r ri) := TRANSFORM
    SELF.x := IF ( le.x = 0, ri.x, le.x );
    SELF.y := IF ( le.y = 0, ri.y, le.y );
      SELF.value := le.value - ri.value; // Fortuitously; 0 is the null value
  END;

assertCondition := ~(Debug AND ~SizeMatch);
checkAssert := ASSERT(assertCondition, &apos;Sub FAILED - Size mismatch&apos;, FAIL);
result := IF(SizeMatch,
                JOIN(l,r,LEFT.x=RIGHT.x AND LEFT.y=RIGHT.y,Su(LEFT,RIGHT),FULL OUTER),
                DATASET([], Types.Element));
    RETURN WHEN(result, checkAssert);

END;
  </Attribute>
  <Attribute key="strict" name="Strict" sourcePath="C:\DATA\Documents\GitHub\ecl-ml\ML\Mat\Strict.ecl">
   EXPORT Strict := FALSE;
  </Attribute>
  <Attribute key="debug" name="Debug" sourcePath="C:\DATA\Documents\GitHub\ecl-ml\ML\Mat\Debug.ecl">
   EXPORT Debug := FALSE;
  </Attribute>
  <Attribute key="scale" name="Scale" sourcePath="C:\DATA\Documents\GitHub\ecl-ml\ML\Mat\Scale.ecl">
   IMPORT * FROM $;
EXPORT Scale(DATASET(Types.Element) d,Types.t_Value factor) := FUNCTION
  Types.Element mul(d le) := TRANSFORM
      SELF.value := le.value * factor;
      SELF := le;
    END;
    RETURN PROJECT(d,mul(LEFT));
  END;
  </Attribute>
  <Attribute key="mul" name="mul" sourcePath="C:\DATA\Documents\GitHub\ecl-ml\ML\Mat\Mul.ecl">
   IMPORT * FROM ML.Mat;
IMPORT Config FROM ML;

MulMethod := ENUM ( Default = 1, SymmetricResult  = 2 );
Mul_Default(DATASET(Types.Element) l,DATASET(Types.Element) r) := FUNCTION

    Types.Element Mu(l le,r ri) := TRANSFORM
        SELF.x := le.x;
        SELF.y := ri.y;
        SELF.value := le.value * ri.value;
    END;

  J := JOIN(l,r,LEFT.y=RIGHT.x,Mu(LEFT,RIGHT),MANY LOOKUP); // Form all of the intermediate computations

    Inter := RECORD
        J.x;
        J.y;
        Types.t_value value := SUM(GROUP,J.value);
    END;

    // Combine all the parts back into a matrix - note if your matrices fit in memory on 1 node - FEW will help
    T := IF(    Has(l).Stats.XMax*Has(r).Stats.YMax*sizeof(Types.Element)&gt;Config.MaxLookup,
                TABLE(J,Inter,x,y,MERGE),
                TABLE(J,Inter,x,y,FEW));

    RETURN PROJECT( T , TRANSFORM( Types.Element, SELF := LEFT ) ); // Cast back into matrix type

END;

Mul_SymmetricResult(DATASET(Types.Element) l,DATASET(Types.Element) r) := FUNCTION

    Types.Element Mu(l le,r ri) := TRANSFORM
        SELF.x := le.x;
        SELF.y := ri.y;
        SELF.value := le.value * ri.value;
    END;

    // Form all of the intermediate computations below diagonal
  J := JOIN(l,r,LEFT.y=RIGHT.x AND LEFT.x&gt;=RIGHT.y,Mu(LEFT,RIGHT));

    Inter := RECORD
        J.x;
        J.y;
        Types.t_value value := SUM(GROUP,J.value);
    END;

    // Combine all the parts back into a matrix - note if your matrices fit in memory on 1 node - FEW will help
    T := IF(    Has(l).Stats.XMax*Has(r).Stats.YMax*sizeof(Types.Element)&gt;Config.MaxLookup,
                TABLE(J,Inter,x,y,MERGE),
                TABLE(J,Inter,x,y,FEW));

    mT := PROJECT( T , TRANSFORM( Types.Element, SELF := LEFT ) ); // Cast back into matrix type

    // reflect the matrix
    Types.Element ReflectM(Types.Element le, UNSIGNED c) := TRANSFORM, SKIP (c=2 AND le.x=le.y)
        SELF.x := IF(c=1,le.x,le.y);
        SELF.y := IF(c=1,le.y,le.x);
        SELF := le;
    END;

    RETURN NORMALIZE(mT,2,ReflectM(LEFT,COUNTER));

END;

EXPORT Mul(DATASET(Types.Element) l,DATASET(Types.Element) r, MulMethod method=MulMethod.Default) := FUNCTION
        StatsL := Has(l).Stats;
        StatsR := Has(r).Stats;
        SizeMatch := ~Strict OR (StatsL.YMax=StatsR.XMax);

        assertCondition := ~(Debug AND ~SizeMatch);
        checkAssert := ASSERT(assertCondition, &apos;Mul FAILED - Size mismatch&apos;, FAIL);
        result := IF(SizeMatch, IF(method=MulMethod.Default, Mul_Default(l,r), Mul_SymmetricResult(l,r)),DATASET([], Types.Element));
        RETURN WHEN(result, checkAssert);
END;&#9;&#9;
  </Attribute>
  <Attribute key="mulXXX" name="mulxxx" sourcePath="C:\DATA\Documents\GitHub\ecl-ml\ML\Mat\Mul.ecl">
   IMPORT * FROM ML.Mat;
IMPORT Config FROM ML;

MulMethod := ENUM ( Default = 1, SymmetricResult  = 2 );
EXPORT MulXXX(DATASET(Types.Element) l,DATASET(Types.Element) r) := FUNCTION

    Types.Element Mu(l le,r ri) := TRANSFORM
        SELF.x := le.x;
        SELF.y := ri.y;
        SELF.value := le.value * ri.value;
    END;

  J0 := JOIN(l,r,LEFT.y=RIGHT.x,Mu(LEFT,RIGHT),MANY LOOKUP); // Form all of the intermediate computations
                J := WHEN(J0, output(sort(J0,x,y,value),named('J0'), EXTEND));

    Inter := RECORD
        J.x;
        J.y;
        Types.t_value value := SUM(GROUP,J.value);
    END;

    // Combine all the parts back into a matrix - note if your matrices fit in memory on 1 node - FEW will help
    T := IF(    Has(l).Stats.XMax*Has(r).Stats.YMax*sizeof(Types.Element)&gt;Config.MaxLookup,
                TABLE(J,Inter,x,y,MERGE),
                TABLE(J,Inter,x,y,FEW));

    RETURN PROJECT( T , TRANSFORM( Types.Element, SELF := LEFT ) ); // Cast back into matrix type

END;

  </Attribute>
  <Attribute key="trans" name="Trans" sourcePath="C:\DATA\Documents\GitHub\ecl-ml\ML\Mat\Trans.ecl">
   IMPORT * FROM $;
EXPORT Trans(DATASET(Types.Element) d) := PROJECT(d,TRANSFORM(Types.Element, SELF.x := LEFT.y, SELF.y := LEFT.x, SELF := LEFT));&#10;
  </Attribute>
 </Module>
 <Module key="std.system" name="std.system">
  <Attribute key="thorlib" name="thorlib" sourcePath="C:\Program Files (x86)\HPCCSystems\4.0.0\clienttools\share\ecllibrary\std\system\Thorlib.ecl">
   /*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.  All rights reserved.
############################################################################## */

/*
 * Internal functions for accessing system information relating to execution on the thor engine.
 *
 * This module is currently treated as internal, and subject to change without notice.
 */

externals :=
    SERVICE
unsigned integer4 node() : ctxmethod, entrypoint=&apos;getNodeNum&apos;;
unsigned integer4 nodes() : ctxmethod, entrypoint=&apos;getNodes&apos;;
varstring l2p(const varstring name, boolean create=false) : ctxmethod, entrypoint=&apos;getFilePart&apos;;
unsigned integer getFileOffset(const varstring lfname) : ctxmethod, entrypoint=&apos;getFileOffset&apos;;
varstring daliServer() : once, ctxmethod, entrypoint=&apos;getDaliServers&apos;;
varstring cluster() : once, ctxmethod, entrypoint=&apos;getClusterName&apos;;
varstring getExpandLogicalName(const varstring name) : pure, ctxmethod, entrypoint=&apos;getExpandLogicalName&apos;;
varstring group() : once, ctxmethod, entrypoint=&apos;getGroupName&apos;;
varstring platform() : pure ,ctxmethod, entrypoint=&apos;getPlatform&apos;;
    END;

RETURN MODULE

/*
 * Returns the index of the slave node this piece of code is executing on.  Zero based.
 */

export node() := externals.node();

/*
 * Converts a logical filename to a physical filename.
 *
 * @param name          The logical filename to be converted.
 * @param create        True if creating a new file, false if reading an existing file.
 */

export logicalToPhysical(const varstring name, boolean create=false) := externals.l2p(name, create);

/*
 * How many nodes in the cluster that this code will be executed on.
 */

export nodes() := CLUSTERSIZE;

/*
 * Returns the dali server this thor is connected to.
 */

export daliServer() := externals.daliServer();

/*
 * Returns which thor group the job is currently executing on.
 */

export group() := externals.group();

/*
 * Converts a logical filename to a physical filename.
 */

export getExpandLogicalName(const varstring name) := externals.getExpandLogicalName(name);

/*
 * Returns the name of the cluster the query is currently executing on.
 */

export cluster() := externals.cluster();

/*
 * Returns the platform the query is currently executing on.
 */

export platform() := externals.platform();

/*
 * The following are either unused, or should be replaced with a different syntax.

export getenv(const varstring name, const varstring defaultValue) := externals.getenv(name, defaultValue);
- use getenv() built in command instead.
export getFileOffset(const varstring lfname) := externals.getFileOffset(lfname);

*/

END;&#13;&#10;
  </Attribute>
 </Module>
</Archive>
