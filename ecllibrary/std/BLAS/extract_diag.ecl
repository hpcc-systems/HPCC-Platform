// The diagonal of a matrix
IMPORT $ AS BLAS;
matrix_t := BLAS.Types.matrix_t;
dim_t := BLAS.Types.dimension_t;
value_t := BLAS.Types.value_t;
/**
 * Extract the diagonal of he matrix
 * @param m number of rows
 * @param n number of columns
 * @param x matrix from which to extract the diagonal
 * @return diagonal matrix
 */
EXPORT matrix_t extract_diag(dim_t m, dim_t n, matrix_t x) := FUNCTION
  cell := {value_t v};
  cell ext(cell v, UNSIGNED pos) := TRANSFORM
    r := ((pos-1) % m) + 1;
    c := ((pos-1) DIV m) + 1;
    SELF.v := IF(r=c AND r<=m AND c<=n, v.v, SKIP);
  END;
  diag := SET(PROJECT(DATASET(x, cell), ext(LEFT, COUNTER)), v);
  RETURN diag;
END;
