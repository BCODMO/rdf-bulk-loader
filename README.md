# rdf-bulk-loader
Load data into RDF data stores

docker exec bcodmo_virtuoso isql-v -U dba -P xxxxxx exec="SPARQL CLEAR GRAPH <http://www.bco-dmo.org/>; ld_dir('dumps/bcodmo/void', '*', 'http://www.bco-dmo.org/'); rdf_loader_run(); select * from DB.DBA.load_list WHERE ll_file LIKE 'dumps/bcodmo/void/%'; delete from DB.DBA.LOAD_LIST;"
