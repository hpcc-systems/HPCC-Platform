// The trace of a square matrix
IMPORT $ AS BLAS;
IMPORT BLAS.Types AS Types;
matrix_t := Types.matrix_t;
dimension_t := Types.dimension_t;
value_t := Types.value_t;
extract_diag := BLAS.extract_diag;

/**
 * The trace of the input matrix
 * @param m number of rows
 * @param n number of columns
 * @param x the matrix
 * @return the trace (sum of the diagonal entries)
 */
EXPORT value_t trace(dimension_t m, dimension_t n, matrix_t x)
    := SUM(extract_diag(m,n,x));
