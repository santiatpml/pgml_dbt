{{
    config(
        materialized='incremental'
    )
}}
WITH document as (
	select id, text
	from {{ source('test_collection_1', 'documents') }}
),
splitter as (
	select id, name, parameters
	from {{ source('test_collection_1', 'splitters') }}
	where id = {{ var('splitter_id') }}
),
chunks as (
    select document.id document_id, {{var('splitter_id')}} splitter_id, chunk_index, chunk from document, splitter, pgml.chunk(splitter.name, document.text, splitter.parameters)
)
select * from chunks
