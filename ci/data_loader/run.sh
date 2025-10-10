echo Uploading data to Blaze Store 
blazectl --server ${FHIR_STORE_URL:-http://store:8080/fhir} upload /app/sample/ 
blazectl --server ${FHIR_STORE_URL:-http://store:8080/fhir} count-resources 
echo Done 
