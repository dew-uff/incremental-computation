
## Overview

The program's main objective is to simulate the evolution of a database of establishments with  20 different dates, interval by 5 days - **** dates.xlsx can be consulted to check the dates used ****. 
This evolution consists of updates applied in proportions (1 by 1 percent, accumulating up to 20% of modified records). Each update stage is chronological, meaning the 1% changes are applied first, followed by more 1% changes, and so on, accumulating the previously applied changes. These updates include generating inclusions (30%), exclusions (10%), and modifications (605) to records, notably inserting a new version of a record using the field `COD_VERSAO_ESTAB = 2`.

The program uses techniques such as random shuffling, chronological manipulation of records, and rigorous validations to avoid inconsistencies in the database. The outputs include updated CSV files, organized by batches, that can be used for further analyses. Detailed logs accompany the entire process, ensuring auditability and traceability.

## Dependencies

- Please check the python script header for the required libraries.

## Inputs

- **CSV Files**: The program processes CSV files with the following fields:
  - `COD_SEQ`, `COD_VERSAO_ESTAB`, `UFMUN`, `NATJUR`, `POAM`, `POTM`, `SALM`, `COD_CNAE`, `DATA_INI`, `DATA_FIM`

## Outputs

- **Output CSV Files**: Updated files containing reorganized records, with new calculated values and applied changes.
  - Generated names: `saida2_evol<i>.csv`, where `<i>` ranges from 1 to 8.
- **Execution Log**: A file detailing each processing step, including execution times, validations, and detected inconsistencies.

## Key Features

### Auxiliary Functions

#### `print_tempo_execucao(start_time, message)`

- **Description**: Logs and displays the execution time of a specific task.
- **Parameters**:
  - `start_time`: Timestamp marking the start of the task.
  - `message`: Descriptive message of the task.
- **Output**: Log and console message with the execution time.

#### `mix(df, s)`

- **Description**: Randomly shuffles records in a DataFrame, using a seed to ensure reproducibility.
- **Parameters**:
  - `df`: DataFrame to be shuffled.
  - `s`: Seed for randomness.
- **Output**: DataFrame reorganized with a new record order.

#### `mixpart(df1, ini0, end0, columns, s)`

- **Description**: Partially shuffles columns in a DataFrame over a specific range.
- **Parameters**:
  - `df1`: Original DataFrame.
  - `ini0`, `end0`: Indices defining the range to be shuffled.
  - `columns`: List of columns to be affected.
  - `s`: Seed for randomness.
- **Output**: DataFrame with specified columns reorganized within the defined range.

### Main Function

#### `processar_arquivo(input_file, output_file, seed)`

- **Description**: Processes an input CSV file and writes the transformed result to an output file.
- **Parameters**:
  - `input_file`: Path to the input file.
  - `output_file`: Path to the output file.
  - `seed`: Seed for controlling randomness.
- **Steps**:

1. **File Reading and Preparation**:
   - Reads the CSV, assigns data types to columns, and converts dates to the appropriate format.
   - Renames and reorders columns to align with the expected standard.

2. **Variable Definitions**:
   - Auxiliary variables are calculated to determine record ranges and update proportions.

3. **Record Shuffling and Organization**:
   - Applies `mix` to randomly reorder the database records.

4. **Date Distribution**:
   - Allocates validity dates (`DFIM`) in controlled proportions.

5. **Subset Creation**:
   - Extracts subsets from the database and applies additional transformations to simulate inclusions and updates.
   - Uses `mixpart` to shuffle specific columns over defined ranges.

6. **Updates and Exclusions**:
   - Records with `COD_VERSAO_ESTAB = 2` are added to simulate data modifications.
   - Fields like `DFIM` and `DINI` are adjusted to reflect the chronology of changes.

7. **Validations and Adjustments**:
   - Checks for inconsistencies, such as duplicate or divergent values between related records.



## Generated Statistics

- **Counts**:
  - Number of records per category (`DFIM`, `DINI`, `CODVER`).
  - Total inconsistencies detected and adjusted.
- **Proportions**:
  - The updates follow defined proportions: 10% exclusions, 30% inclusions, and 60% updates.

## Key Program Objectives and Behaviors

- **Chronological Updates**:
 

- Updates are simulated progressively over twenty dates, affecting 1% of records for each date, 20% cumulatively. All inclusions, deletions and updates are applied in this order.

- **Simulated Operations**:
  - ~10% of records are excluded by setting an `DFIM` value (20 valid dates).
  - ~30% of records are inclusions simulated by creating new entries with unique keys (20 valid dates, simulating new registers with mixed old data from input file).
  - ~60% of records are updates simulated by creating new entries with `COD_VERSAO_ESTAB = 2` and adjusted values (20 valid dates).

- **Statistical Validations**:
  - To avoid inconsistencies where randomization might result in unchanged values, validations are implemented. Logs and statistics are generated to monitor and address such issues.

This detailed documentation ensures clarity on the program's objectives, processes, and expected outcomes.

