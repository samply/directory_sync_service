#!/usr/bin/bash

# Compare the results of a Directory sync run with stored reference values.
# Usage:
#
# compare_test_results_with_reference.sh <Reference>
#
# You can find the needed Reference by looking at the docker-compose.yml
# file and checking the the volumes line in the data-loader definition.
#
# Example value for Reference: crc_full
#
# This argument is mandatory.

# Read in reference.
if [[ $# -lt 1 ]]; then
    echo "Error: Missing required argument 'reference'"
    echo "Usage: $0 <reference>"
    exit 1
fi
REFERENCE="$1"
# Check if REFERENCE a valid directory under data/output
if [[ ! -d "data/output/$REFERENCE" ]]; then
    echo "Error: '$REFERENCE' is not a valid directory under data/input"
    echo "Available options are:"
    ls -1 data/input
    exit 1
fi

# Find somewhere to put intermediate files
if [ -e /tmp ] ; then
	TMP=/tmp
else
	mkdir -p ./tmp
	TMP=./tmp
fi

# Preprocess reference fact table data
cat data/output/$REFERENCE/DirectoryFactTables.csv | \
# Remove column 8 (last_update), since this is is never the same.
awk -F';' 'BEGIN {OFS=";"} {for(i=1;i<=NF;i++) if(i!=8) printf "%s%s", $i, (i<NF ? OFS : ""); printf "\n"}' | \
# Correct ICD code
sed 's/urn:miriam:icd:urn:miriam:icd/urn:miriam:icd/' | \
# Remove arbitrary patient ID
sed 's/Cohort[^;]*;/Cohort;/' | \
# Remove quotes
#sed 's/"//g' | \
# Remove header row
grep -v "sample_type;number_of_donors;number_of_samples" | \
sort > $TMP/crc-cohort_facts.processed.csv

# Preprocess generated output fact table data
cat output/DirectoryFactTables.csv | \
# Remove column 10 (national_node), which is not in Petr's fact tables.
#awk -F';' 'BEGIN {OFS=";"} {for(i=1;i<=NF;i++) if(i!=10) printf "%s%s", $i, (i<NF ? OFS : ""); printf "\n"}' | \
# Remove the left-over semicolon at the end of each line
sed 's/;$//' | \
# Remove column 8 (last_update), since this is is never the same.
awk -F';' 'BEGIN {OFS=";"} {for(i=1;i<=NF;i++) if(i!=8) printf "%s%s", $i, (i<NF ? OFS : ""); printf "\n"}' | \
# Remove arbitrary patient ID
sed 's/Cohort[^;]*;/Cohort;/' | \
sed 's/_collection_/:collection:/' | \
# Remove header row
grep -v "sample_type;number_of_donors;number_of_samples" | \
sort > $TMP/DirectoryFactTables.processed.csv

# Compare fact tables
if diff $TMP/crc-cohort_facts.processed.csv $TMP/DirectoryFactTables.processed.csv; then
    echo "Fact table output matches against reference."
else
    echo "Fact table output does not match against reference."
fi

# Preprocess reference collection data
cat data/output/$REFERENCE/DirectoryCollections.csv | \
# Remove column 7 (last_update), since this is is never the same.
awk -F';' 'BEGIN {OFS=";"} {for(i=1;i<=NF;i++) if(i!=7) printf "%s%s", $i, (i<NF ? OFS : ""); printf "\n"}' | \
# Remove header row
grep -v "order_of_magnitude" | \
sort > $TMP/crc-cohort_collections.processed.csv

# Preprocess generated output collection data
cat output/DirectoryCollections.csv | \
# Remove column 7 (last_update), since this is is never the same.
awk -F';' 'BEGIN {OFS=";"} {for(i=1;i<=NF;i++) if(i!=7) printf "%s%s", $i, (i<NF ? OFS : ""); printf "\n"}' | \
# Remove header row
grep -v "order_of_magnitude" | \
sort > $TMP/DirectoryCollections.processed.csv

# Compare collections
if diff $TMP/crc-cohort_collections.processed.csv $TMP/DirectoryCollections.processed.csv; then
    echo "Collections output matches against reference."
else
    echo "Collections output does not match against reference."
fi

