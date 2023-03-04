DROP VIEW if exists ${schema:raw}.vw_total_population CASCADE;

CREATE OR REPLACE VIEW ${schema:raw}.vw_total_population AS
SELECT SUM(
	CAST(jsonb_extract_path_text(doc_record, 'Population') AS INTEGER)
) as total
FROM ${schema:raw}.api_data
WHERE doc_record->>'Year' >= '2018' AND doc_record->>'Year' <= '2020';
