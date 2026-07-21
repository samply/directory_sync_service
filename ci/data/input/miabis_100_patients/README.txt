Test dataset using the MIABIS on FHIR profiles.

Does not test any edge cases.

Number of patients: 100
Number of specimens: 206

Here are some jq commands to extract various useful things from the FHIR JSON file.

Count Patient resources:

jq '[.entry[] | select(.resource.resourceType == "Patient")] | length' miabis_100_patients.json

Count Specimen resources:

jq '[.entry[] | select(.resource.resourceType == "Specimen")] | length' miabis_100_patients.json

Count Condition resources:

jq '[.entry[] | select(.resource.resourceType == "Condition")] | length' miabis_100_patients.json

List storage-temperature codes from Specimen processing extensions:

jq -r '
  [
    .entry[]
    | select(.resource.resourceType == "Specimen")
    | .resource.processing[]?
    | .extension[]?
    | select(
        .url ==
        "https://fhir.bbmri-eric.eu/StructureDefinition/miabis-sample-storage-temperature-extension"
      )
    | .valueCodeableConcept.coding[]?
    | .code
  ]
  | unique[]
' miabis_100_patients.json

List specimen type codes:

jq -r '
  [
    .entry[]
    | select(.resource.resourceType == "Specimen")
    | .resource.type.coding[]?
    | .code
  ]
  | unique[]
' miabis_100_patients.json

List the unique ICD-10 condition codes:

jq -r '
  [
    .entry[]
    | select(.resource.resourceType == "Condition")
    | .resource.code.coding[]?
    | select(.system == "http://hl7.org/fhir/sid/icd-10")
    | {code, display}
  ]
  | unique_by(.code)
  | .[]
  | "\(.code)\t\(.display)"
' miabis_100_patients.json

Count the number of unique ICD-10 condition codes:

jq '
  [
    .entry[]
    | select(.resource.resourceType == "Condition")
    | .resource.code.coding[]?
    | select(.system == "http://hl7.org/fhir/sid/icd-10")
    | .code
  ]
  | unique
  | length
' miabis_100_patients.json

