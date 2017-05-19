CREATE OR REPLACE TEMPORARY VIEW testData AS SELECT * FROM VALUES
(1, "1", "11"), (2, "2", "22"), (3, "3", "33"), (4, "4", "44"), (5, "5", "55"), (6, "6", "66")
AS testData(key, value1, value2);

CREATE OR REPLACE TEMPORARY VIEW testData2 AS SELECT * FROM VALUES
(1, 1, 1, 2), (1, 2, 1, 2), (2, 1, 2, 3), (2, 2, 2, 3), (3, 1, 3, 4), (3, 2, 3, 4)
AS testData2(a, b, c, d);

-- AnalysisException
SELECT `(a)?+.+` FROM testData2 WHERE a = 1;
SELECT t.`(a)?+.+` FROM testData2 t WHERE a = 1;
SELECT `(a|b)` FROM testData2 WHERE a = 2;
SELECT `(a|b)?+.+` FROM testData2 WHERE a = 2;

set spark.sql.parser.quotedRegexColumnNames=true;

-- Regex columns
SELECT `(a)?+.+` FROM testData2 WHERE a = 1;
SELECT t.`(a)?+.+` FROM testData2 t WHERE a = 1;
SELECT `(a|b)` FROM testData2 WHERE a = 2;
SELECT `(a|b)?+.+` FROM testData2 WHERE a = 2;
SELECT p.`(key)?+.+`, b, testdata2.`(b)?+.+` FROM testData p join testData2 ON p.key = testData2.a WHERE key < 3;
