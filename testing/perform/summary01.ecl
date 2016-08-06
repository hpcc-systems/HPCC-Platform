
import std.System.Workunit AS Wu;

wuRecord := RECORD
    STRING wuid;
    STRING job;
END;

timingRecord := wu.TimingRecord OR wuRecord;

resultRec := RECORD
    unsigned minValue;
    unsigned maxValue;
    unsigned aveValue;
    unsigned medValue;
    STRING8 date;
    STRING job;
    STRING statname;
END;

generateSummary(string searchCluster) := FUNCTION

   completedWorkUnits := Wu.WorkunitList('', cluster := searchCluster, state := 'completed');

   regressSuiteWu := completedWorkunits(REGEXFIND('^[0-9]+[a-z][a-z]_', job));

   RETURN PROJECT(regressSuiteWu, TRANSFORM(wuRecord, SELF.wuid := TRIM(LEFT.wuid); SELF.job := TRIM(LEFT.job)));
END;

expandTimings(DATASET(wuRecord) wus) := FUNCTION

    timingRecord t(wuRecord l, wu.TimingRecord r) := TRANSFORM
        SELF := l;
        SELF := r;
    END;

    RETURN NORMALIZE(wus, Wu.WorkunitTimings(LEFT.wuid), t(LEFT, RIGHT));
END;

allWorkunits := generateSummary('thor');
allTimings := expandTimings(allWorkunits);

filteredTimings := allTimings(name = 'Process');

groupedByJobDate := GROUP(filteredTimings, job, wuid[1..9], name, ALL);

sortByDuration := SORT(groupedByJobDate, duration);

resultRec combineResults(timingRecord l, DATASET(timingRecord) timings) := TRANSFORM
    SELF := l;
    SELF.date := l.wuid[2..9];
    SELF.statName := l.name;
    SELF.minValue := MIN(timings, duration);
    SELF.maxValue := MAX(timings, duration);
    SELF.aveValue := AVE(timings, duration);
    SELF.medValue := timings[(COUNT(timings)+1) DIV 2].duration;
    SELF := [];
END;

summarised := ROLLUP(sortByDuration, GROUP, combineResults(LEFT, ROWS(LEFT)));

interesting := summarised(maxValue != 0);

output(interesting);

uniqueDates := SORT(TABLE(dedup(interesting, date, HASH), { STRING8 x := date }), x);
uniqueJobs := SORT(TABLE(dedup(interesting, job, HASH), { STRING y := job }), y);
uniqueStats := SORT(TABLE(dedup(interesting, statName, HASH), { STRING stat := statName }), stat);

xValues := ROW(TRANSFORM({ STRING8 x }, SELF.x := '')) & uniqueDates;
yValues := ROW(TRANSFORM({ STRING y }, SELF.y := '')) & uniqueJobs;

crossProduct := SORT(JOIN(xValues, yValues, true, ALL), y, x);

xyValueRec := { STRING8 x; STRING y; UNICODE text };

xyValueRec extractResult(crossProduct l, interesting r) := TRANSFORM
    SELF := l;
    SELF.text := MAP(l.x = '' => l.y,
                      l.y = '' => l.x,
                      IF(r.date = '', '', (string)r.aveValue));
END;

values := JOIN(crossProduct, interesting, LEFT.x = RIGHT.date AND LEFT.y = RIGHT.job, extractResult(LEFT, RIGHT), LEFT OUTER, MANY LOOKUP);


output(xValues);
output(yValues);

createHtmlTable(DATASET(xyValueRec) values) := FUNCTION

    import CellFormatter.HTML;

    xyValueRec toHTML(xyValueRec l) := TRANSFORM
        SELF.text := IF(l.y = '', HTML.TableHeader(l.text), HTML.TableCell(l.text));
        SELF := l;
    END;

    concatRows1(GROUPED DATASET(xyValueRec) Values) :=
        AGGREGATE(values, xyValueRec, TRANSFORM(xyValueRec, SELF.text := RIGHT.text + LEFT.text; SELF := LEFT));

    concatRows2(DATASET(xyValueRec) Values) :=
        AGGREGATE(values, xyValueRec, TRANSFORM(xyValueRec, SELF.text := RIGHT.text + LEFT.text; SELF := LEFT));

    HtmlCells := PROJECT(values, toHTML(LEFT));

    byRow := concatRows1(GROUP(HtmlCells, y));

    AddRows := PROJECT(byRow, TRANSFORM(xyValueRec, SELF.text := HTML.TableRow(LEFT.text); SELF := LEFT));

    byAll := concatRows2(AddRows);

    RETURN TABLE(byAll, { UNICODE text__html := HTML.Table(text, TRUE); });
END;


output(values);
output(createHtmlTable(values));

output(uniqueDates,,NAMED('Dates'));
output(uniqueJobs, NAMED('Jobs'));
output(dedup(allTimings, name, ALL), { name }, NAMED('Statistics'));


import CellFormatter, CellFormatter.HTML;

htmlRecord := RECORD
    UNICODE Summary__html;
END;
htmlDataset := DATASET([
    {HTML.Table(
        HTML.TableRow(HTML.TableHeader('Column 1') + HTML.TableHeader('Column 2')) +
        HTML.TableRow(HTML.TableCell('Cell 1, 1') + HTML.TableCell('cell 1, 2')) +
        HTML.TableRow(HTML.TableCell('Cell 2, 1') + HTML.TableCell(u'Unicode Text:非常によい編集者であ非る非常によい編集者である非常によい編集者である'))
    , TRUE)}
], htmlRecord);

OUTPUT(htmlDataset, NAMED('SummaryTimings'));
