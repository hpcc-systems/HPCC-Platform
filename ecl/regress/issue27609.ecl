IMPORT Python AS Python;

anyInput := 5;

UNSIGNED testFunc(UNSIGNED ANYinput) := EMBED(Python)

    return ANYinput + 1

ENDEMBED;

result := testFunc(1);

OUTPUT(result);
