//Make a diagonal matrix from a vector or a single value
//If the vector is present the diagonal is the product
IMPORT $.Types;
matrix_t := Types.matrix_t;
dimension_t := Types.dimension_t;
value_t := Types.value_t;
/**
 * Generate a diagonal matrix.
 * @param m number of diagonal entries
 * @param v option value, defaults to 1
 * @param X optional input of diagonal values, multiplied by v.
 * @return a diagonal matrix
 */
EXPORT matrix_t make_diag(dimension_t m, value_t v=1.0, matrix_t X=[]) := BEGINC++
#body
  int cells = m * m;
  __isAllResult = false;
  __lenResult = cells * sizeof(double);
  double *diag = (double*) rtlMalloc(__lenResult);
  const double *in_x = (double*)x;
  unsigned int r, c;    //row and column
  for (int i=0; i<cells; i++) {
    r = i % m;
    c = i / m;
    diag[i] = (r==c)?  (lenX!=0 ? v*in_x[r] : v) : 0.0;
  }
  __result = (void*) diag;
ENDC++;