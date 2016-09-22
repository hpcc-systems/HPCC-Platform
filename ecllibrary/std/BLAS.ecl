IMPORT lib_blas;

EXPORT BLAS := MODULE
  // Types for the Block Basic Linear Algebra Sub-programs support
  // WARNING: attributes can not be changed without making
  //corresponding changes to the C++ attributes.
  EXPORT Types := MODULE
    EXPORT dimension_t  := UNSIGNED4;     // WARNING: type used in C++ attributes
    EXPORT value_t      := REAL8;         // Warning: type used in C++ attribute
    EXPORT matrix_t     := SET OF REAL8;  // Warning: type used in C++ attribute
    EXPORT Triangle     := ENUM(UNSIGNED1, Upper=1, Lower=2); //Warning
    EXPORT Diagonal     := ENUM(UNSIGNED1, UnitTri=1, NotUnitTri=2);  //Warning
    EXPORT Side         := ENUM(UNSIGNED1, Ax=1, xA=2);  //Warning
  END;

  /**
   * Function prototype for Apply2Cell.
   * @param v the value
   * @param r the row ordinal
   * @param c the column ordinal
   * @return the updated value
   */
  EXPORT Types.value_t ICellFunc(Types.value_t v,
                                 Types.dimension_t r,
                                 Types.dimension_t c) := v;

  /**
   * Iterate matrix and apply function to each cell
   *@param m number of rows
   *@param n number of columns
   *@param x matrix
   *@param f function to apply
   *@return updated matrix
   */
  EXPORT Types.matrix_t Apply2Cells(Types.dimension_t m,
                                    Types.dimension_t n,
                                    Types.matrix_t x,
                                    ICellFunc f) := FUNCTION
    Cell := {Types.value_t v};
    Cell applyFunc(Cell v, UNSIGNED pos) := TRANSFORM
      r := ((pos-1)  %  m) + 1;
      c := ((pos-1) DIV m) + 1;
      SELF.v := f(v.v, r, c);
    END;
    dIn := DATASET(x, Cell);
    dOut := PROJECT(dIn, applyFunc(LEFT, COUNTER));
    RETURN SET(dOut, v);
  END;

  /**
   * Extract the diagonal of he matrix
   * @param m number of rows
   * @param n number of columns
   * @param x matrix from which to extract the diagonal
   * @return diagonal matrix
   */
  EXPORT Types.matrix_t extract_diag(Types.dimension_t m,
                                     Types.dimension_t n,
                                     Types.matrix_t x) := FUNCTION
    cell := {Types.value_t v};
    cell ext(cell v, UNSIGNED pos) := TRANSFORM
      r := ((pos-1) % m) + 1;
      c := ((pos-1) DIV m) + 1;
      SELF.v := IF(r=c AND r<=m AND c<=n, v.v, SKIP);
    END;
    diag := SET(PROJECT(DATASET(x, cell), ext(LEFT, COUNTER)), v);
    RETURN diag;
  END;

  Cell := RECORD
    Types.value_t v;
  END;
  Cell makeCell(Types.value_t v) := TRANSFORM
    SELF.v := v;
  END;
  vec_dataset(Types.dimension_t m, Types.value_t v) := DATASET(m, makeCell(v));

  /**
   * Make a vector of dimension m
   * @param m number of elements
   * @param v the values, defaults to 1
   * @return the vector
   */
  EXPORT Types.matrix_t
      make_vector(Types.dimension_t m,
                  Types.value_t v=1.0) := SET(vec_dataset(m, v), v);

  /**
   * The trace of the input matrix
   * @param m number of rows
   * @param n number of columns
   * @param x the matrix
   * @return the trace (sum of the diagonal entries)
   */
  EXPORT Types.value_t
      trace(Types.dimension_t m,
            Types.dimension_t n,
            Types.matrix_t x)
      := SUM(extract_diag(m,n,x));
END;
