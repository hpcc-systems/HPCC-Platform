IMPORT Std.DataPatterns;

EXPORT TestBenford := MODULE

    SHARED TestData := DATASET
        (
            [39.27, 38.0, 36.06, 37.75, 37.4, 35.5, 36.25, 35.41, 35.76, 34.56,
             35.05, 33.5, 34.53, 34.11, 33.55, 33.74, 25.2, 24.4, 23.87, 22.73,
             22.92, 22.33, 23.72, 21.95, 21.53, 21.01, 21.3, 21.16, 21.75, 22.51,
             22.15, 22.75, 22.9, 21.94, 21.75, 21.5, 21.84, 21.77, 22.44, 23.15, 22.6,
             22.4, 23.1, 23.0, 23.94, 23.63, 23.46, 23.75, 23.75, 23.75, 23.0, 23.27, 23.82,
             23.56, 23.5, 22.9, 22.74, 22.13, 22.13, 22.21, 22.35, 22.35, 22.35,
             22.15, 20.86, 20.7, 20.6, 21.02, 21.0, 21.76, 20.99, 22.0, 22.04, 22.45,
             22.61, 23.15, 23.41, 21.39, 21.25, 21.09, 18.89, 17.71, 17.13, 17.12,
             16.99, 16.56, 16.5, 15.71, 15.75, 17.13, 16.6, 16.53, 16.87, 18.09, 18.1,
             17.83, 18.25, 17.9, 17.91, 18.11],
            {DECIMAL9_2 n}
        );

    SHARED BenfordResults1 := DataPatterns.Benford(TestData, 'n', digit := 1);
    SHARED TestBenford1 := [
        ASSERT(BenfordResults1[1].zero = -1D),
        ASSERT(BenfordResults1[1].one = 30.1D),
        ASSERT(BenfordResults1[1].two = 17.6D),
        ASSERT(BenfordResults1[1].three = 12.5D),
        ASSERT(BenfordResults1[1].four = 9.7D),
        ASSERT(BenfordResults1[1].five = 7.9D),
        ASSERT(BenfordResults1[1].six = 6.7D),
        ASSERT(BenfordResults1[1].seven = 5.8D),
        ASSERT(BenfordResults1[1].eight = 5.1D),
        ASSERT(BenfordResults1[1].nine = 4.6D),
        ASSERT(BenfordResults1[1].chi_squared = 20.09D),
        ASSERT(BenfordResults1[1].num_values = 100),
        ASSERT(BenfordResults1[2].zero = -1D),
        ASSERT(BenfordResults1[2].one = 20D),
        ASSERT(BenfordResults1[2].two = 64D),
        ASSERT(BenfordResults1[2].three = 16D),
        ASSERT(BenfordResults1[2].four = 0D),
        ASSERT(BenfordResults1[2].five = 0D),
        ASSERT(BenfordResults1[2].six = 0D),
        ASSERT(BenfordResults1[2].seven = 0D),
        ASSERT(BenfordResults1[2].eight = 0D),
        ASSERT(BenfordResults1[2].nine = 0D),
        ASSERT(BenfordResults1[2].chi_squared = 166.496D),
        ASSERT(BenfordResults1[2].num_values = 100)
    ];

    SHARED BenfordResults2 := DataPatterns.Benford(TestData, digit := 2);
    SHARED TestBenford2 := [
        ASSERT(BenfordResults2[1].zero = 12D),
        ASSERT(BenfordResults2[1].one = 11.4D),
        ASSERT(BenfordResults2[1].two = 10.9D),
        ASSERT(BenfordResults2[1].three = 10.4D),
        ASSERT(BenfordResults2[1].four = 10D),
        ASSERT(BenfordResults2[1].five = 9.7D),
        ASSERT(BenfordResults2[1].six = 9.3D),
        ASSERT(BenfordResults2[1].seven = 9D),
        ASSERT(BenfordResults2[1].eight = 8.8D),
        ASSERT(BenfordResults2[1].nine = 8.5D),
        ASSERT(BenfordResults2[1].chi_squared = 21.666D),
        ASSERT(BenfordResults2[1].num_values = 100),
        ASSERT(BenfordResults2[2].zero = 4D),
        ASSERT(BenfordResults2[2].one = 17D),
        ASSERT(BenfordResults2[2].two = 23D),
        ASSERT(BenfordResults2[2].three = 21D),
        ASSERT(BenfordResults2[2].four = 4D),
        ASSERT(BenfordResults2[2].five = 7D),
        ASSERT(BenfordResults2[2].six = 8D),
        ASSERT(BenfordResults2[2].seven = 9D),
        ASSERT(BenfordResults2[2].eight = 6D),
        ASSERT(BenfordResults2[2].nine = 1D),
        ASSERT(BenfordResults2[2].chi_squared = 44.371D),
        ASSERT(BenfordResults2[2].num_values = 100)
    ];

    SHARED BenfordResults3 := DataPatterns.Benford(TestData, digit := 3);
    SHARED TestBenford3 := [
        ASSERT(BenfordResults3[1].zero = 10.2D),
        ASSERT(BenfordResults3[1].one = 10.1D),
        ASSERT(BenfordResults3[1].two = 10.1D),
        ASSERT(BenfordResults3[1].three = 10.1D),
        ASSERT(BenfordResults3[1].four = 10D),
        ASSERT(BenfordResults3[1].five = 10D),
        ASSERT(BenfordResults3[1].six = 9.9D),
        ASSERT(BenfordResults3[1].seven = 9.9D),
        ASSERT(BenfordResults3[1].eight = 9.9D),
        ASSERT(BenfordResults3[1].nine = 9.8D),
        ASSERT(BenfordResults3[1].chi_squared = 21.666D),
        ASSERT(BenfordResults3[1].num_values = 100),
        ASSERT(BenfordResults3[2].zero = 12D),
        ASSERT(BenfordResults3[2].one = 14D),
        ASSERT(BenfordResults3[2].two = 7D),
        ASSERT(BenfordResults3[2].three = 6D),
        ASSERT(BenfordResults3[2].four = 8D),
        ASSERT(BenfordResults3[2].five = 13D),
        ASSERT(BenfordResults3[2].six = 5D),
        ASSERT(BenfordResults3[2].seven = 18D),
        ASSERT(BenfordResults3[2].eight = 7D),
        ASSERT(BenfordResults3[2].nine = 10D),
        ASSERT(BenfordResults3[2].chi_squared = 15.646D),
        ASSERT(BenfordResults3[2].num_values = 100)
    ];

    SHARED BenfordResults4 := DataPatterns.Benford(TestData, digit := 4);
    SHARED TestBenford4 := [
        ASSERT(BenfordResults4[1].zero = 10D),
        ASSERT(BenfordResults4[1].one = 10D),
        ASSERT(BenfordResults4[1].two = 10D),
        ASSERT(BenfordResults4[1].three = 10D),
        ASSERT(BenfordResults4[1].four = 10D),
        ASSERT(BenfordResults4[1].five = 10D),
        ASSERT(BenfordResults4[1].six = 10D),
        ASSERT(BenfordResults4[1].seven = 10D),
        ASSERT(BenfordResults4[1].eight = 10D),
        ASSERT(BenfordResults4[1].nine = 10D),
        ASSERT(BenfordResults4[1].chi_squared = 21.666D),
        ASSERT(BenfordResults4[1].num_values = 100),
        ASSERT(BenfordResults4[2].zero = 24D),
        ASSERT(BenfordResults4[2].one = 11D),
        ASSERT(BenfordResults4[2].two = 5D),
        ASSERT(BenfordResults4[2].three = 11D),
        ASSERT(BenfordResults4[2].four = 7D),
        ASSERT(BenfordResults4[2].five = 22D),
        ASSERT(BenfordResults4[2].six = 9D),
        ASSERT(BenfordResults4[2].seven = 5D),
        ASSERT(BenfordResults4[2].eight = 0D),
        ASSERT(BenfordResults4[2].nine = 6D),
        ASSERT(BenfordResults4[2].chi_squared = 51.8D),
        ASSERT(BenfordResults4[2].num_values = 100)
    ];

    SHARED BenfordResults5 := DataPatterns.Benford(TestData, digit := 5);
    SHARED TestBenford5 := [
        ASSERT(BenfordResults5[1].zero = 10D),
        ASSERT(BenfordResults5[1].one = 10D),
        ASSERT(BenfordResults5[1].two = 10D),
        ASSERT(BenfordResults5[1].three = 10D),
        ASSERT(BenfordResults5[1].four = 10D),
        ASSERT(BenfordResults5[1].five = 10D),
        ASSERT(BenfordResults5[1].six = 10D),
        ASSERT(BenfordResults5[1].seven = 10D),
        ASSERT(BenfordResults5[1].eight = 10D),
        ASSERT(BenfordResults5[1].nine = 10D),
        ASSERT(BenfordResults5[1].chi_squared = 21.666D),
        ASSERT(BenfordResults5[1].num_values = 100),
        ASSERT(BenfordResults5[2].zero = 100D),
        ASSERT(BenfordResults5[2].one = 0D),
        ASSERT(BenfordResults5[2].two = 0D),
        ASSERT(BenfordResults5[2].three = 0D),
        ASSERT(BenfordResults5[2].four = 0D),
        ASSERT(BenfordResults5[2].five = 0D),
        ASSERT(BenfordResults5[2].six = 0D),
        ASSERT(BenfordResults5[2].seven = 0D),
        ASSERT(BenfordResults5[2].eight = 0D),
        ASSERT(BenfordResults5[2].nine = 0D),
        ASSERT(BenfordResults5[2].chi_squared = 900D),
        ASSERT(BenfordResults5[2].num_values = 100)
    ];

    SHARED RegressionResults1 := DataPatterns.Benford(DATASET([0.0012], {DECIMAL5_4 n}), digit := 2);
    SHARED TestRegression1 := [
        ASSERT(RegressionResults1[2].two = 100D),
        ASSERT(RegressionResults1[2].num_values = 1)
    ];

    EXPORT Main := [
        EVALUATE(TestBenford1),
        EVALUATE(TestBenford2),
        EVALUATE(TestBenford3),
        EVALUATE(TestBenford4),
        EVALUATE(TestBenford5),
        EVALUATE(TestRegression1)
    ];

END;
