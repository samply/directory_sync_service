import pytest


@pytest.mark.parametrize(
    "schema_key",
    [
        pytest.param("bbmri", id="BBMRI"),
        pytest.param("onto", id="Ontologies"),
    ],
)
def test_graphql_responds(emx2, schema_names, schema_key):
    schema = schema_names[schema_key]

    print(f"Testing GraphQL endpoint for schema '{schema}'")

    data = emx2(schema, "query { __typename }")

    assert data["__typename"]
