def test_directoryontologies_has_countries(emx2, schema_names):
    print("test_directoryontologies_has_countries: entered")
    onto = schema_names["onto"]
    data = emx2(onto, "query { Countries { code label } }")
    assert len(data["Countries"]) > 0, "DirectoryOntologies.Countries appears empty"


def test_bbmri_country_field_query_works(emx2, schema_names):
    """
    This is a lightweight 'integration' check: query a few Biobanks with country.
    Country name will be returned.
    We just assert the query succeeds and returns rows.
    """
    print("test_bbmri_country_field_query_works: entered")
    bbmri = schema_names["bbmri"]
    data = emx2(
        bbmri,
        """
        query {
          Biobanks {
            id
            country { name }
          }
        }
        """,
    )
    assert len(data["Biobanks"]) > 0