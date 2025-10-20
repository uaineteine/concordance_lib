# CONCORDANCE LIB

### ENVIORNMENT Variables Required

* CONCLIB_PATH - the path variable for all the resource maps, spine files and linkage results
* CONCLIB_PRD_FMT - the output product filename convention

# The 2 packages

## conclib
The `conclib` package provides the ability to extract an entity map and linkage results from a common container or path. It allows for loading a spine concordance and releasing a new one based on a set of project IDs.

### Example Usage
```python
from conclib import load_ent_map, create_spine_conc, SpineProduct

# Load an entity map
entity_map = load_ent_map(id_group_num=1, sparkSession=spark)

# Create a spine concordance
spine_product = SpineProduct(spine_version=4)
spine_concordance = create_spine_conc(idGroup=1, projectIDs=[101, 102], spine_prd=spine_product, sparkSession=spark)
```

## rkeylib
The `rkeylib` package is designed to process existing data frames and use pre-hashed lists to coalesce a record key. This key can be either a synthetic ID or a spine, depending on the presence of null values in the data.

### Example Usage
```python
from rkeylib import create_rkeys

# Create rkeys
df_with_rkeys = create_rkeys(df, asset_name="example_asset", spine_version_min=1, spine_version_max=3)
```

# Running build.bat
To build the project, you can use the `build.bat` script. This script will package the library and prepare it for distribution.

### Steps:
1. Open a terminal and navigate to the project directory.
2. Run the following command:
   ```
   .\build.bat
   ```
3. The script will generate the necessary build artifacts in the `dist` folders for each package.
