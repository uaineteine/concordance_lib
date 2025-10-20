# CONCORDANCE LIB

### ENVIORNMENT Variables Required

* CONCLIB_PATH - the path variable for all the resource maps, spine files and linkage results
* CONCLIB_PRD_FMT - the output product filename convention

# The 2 packages

## conclib
The `conclib` package provides the ability to extract an entity map and linkage results from a common container or path. It allows for loading a spine concordance and releasing a new one based on a set of project IDs.

## rkeylib
The `rkeylib` package is designed to process existing data frames and use pre-hashed lists to coalesce a record key. This key can be either a synthetic ID or a spine, depending on the presence of null values in the data.
