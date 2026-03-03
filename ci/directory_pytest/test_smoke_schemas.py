def test_bbmri_graphql_responds(emx2, schema_names):
    print("test_bbmri_graphql_responds: entered")
    schema = schema_names["bbmri"]
    data = emx2(schema, "query { __typename }")
    assert data["__typename"]


def test_ontologies_graphql_responds(emx2, schema_names):
    print("test_ontologies_graphql_responds: entered")
    schema = schema_names["onto"]
    data = emx2(schema, "query { __typename }")
    assert data["__typename"]