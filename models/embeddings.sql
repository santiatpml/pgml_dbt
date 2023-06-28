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
	where name = '{{ var('splitter_name')}}' and parameters = '{{ tojson(var('splitter_parameters'))}}'::jsonb
),
chunks as (
    select document.id document_id, splitter.id splitter_id, chunk_index, chunk from document, splitter, pgml.chunk(splitter.name, document.text, splitter.parameters)
)
select * from chunks
