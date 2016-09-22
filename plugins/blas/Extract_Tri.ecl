//Extract the upper triangular matrix or the lower triangular matrix
//from a composite.  Composites are produced by some factorizations.
//
IMPORT $.Types;
matrix_t := Types.matrix_t;
dimension_t := Types.dimension_t;
Triangle := Types.Triangle;
Diagonal := Types.Diagonal;
/**
 * Extract the upper or lower triangle.  Diagonal can be actual or implied
 * unit diagonal.
 * @param m number of rows
 * @param n number of columns
 * @param tri Upper or Lower specifier, Triangle.Lower or Triangle.Upper
 * @param dt Use Diagonal.NotUnitTri or Diagonal.UnitTri
 * @param a Matrix, usually a composite from factoring
 * @return the triangle
 */
EXPORT matrix_t Extract_Tri(dimension_t m, dimension_t n,
                            Triangle tri, Diagonal dt, matrix_t a) := BEGINC++
  #define UPPER 1
  #define UNIT_TRI 1
  #body
  int cells = m * n;
  __isAllResult = false;
  __lenResult = lenA;
  double *new_a = (double*) rtlMalloc(lenA);
  unsigned int r, c;    //row and column
  for (int i=0; i<cells; i++) {
    r = i % m;
    c = i / m;
    if (r==c) new_a[i] = (dt==UNIT_TRI) ? 1.0  : ((double*)a)[i];
    else if (r > c) { // lower part
      new_a[i] = (tri==UPPER) ? 0.0  : ((double*)a)[i];
    } else {          // upper part
      new_a[i] = (tri==UPPER) ? ((double*)a)[i]  : 0.0;
    }
  }
  __result = (void*) new_a;
ENDC++;