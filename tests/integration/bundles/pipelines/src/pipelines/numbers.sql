CREATE MATERIALIZED VIEW numbers
COMMENT "Three literal rows, so that an update of the pipeline is quick."
AS SELECT n FROM VALUES (1), (2), (3) AS t(n);
