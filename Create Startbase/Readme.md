This Python program was developed to generate an individualized synthetic database from aggregated data extracted from SIDRA (IBGE Automatic Recovery System), specifically from tables of the Central Business Registry (Cempre) of the Brazilian Institute of Geography and Statistics (IBGE). The original data is organized at the municipal level and by the National Classification of Economic Activities (CNAE) divisions. These divisions, represented by two digits, include aggregated information such as the number of companies, total employed personnel (POT), salaried employed personnel (POA) and total salaries (SAL). To create the individualized database, the program creates individualized records, distributes the values ​​reported in the aggregates through statistical distributions (Beta and Dirichlet) and introduces noise between variables to avoid exact correlations.

In addition, it distributes the differences between the overall totals reported by the survey in the de-identified values ​​("X") of the file, simulating up to two individualized records. The "X" values ​​indicate confidential data related to less than three companies, which are not explicitly disclosed in SIDRA for confidentiality reasons. The Beta distribution simulates economic characteristics, such as asymmetry and frequency disparity and Dirichlet complements the proportional distribution of multiple values. The program also disaggregates the two-digit CNAE divisions (present in the original file) into five-digit classes, assigning these values ​​hierarchically randomly and consistently according proportions. Additional treatment is given to the legal nature, linking proportions between economic activities and types of legal nature based on an auxiliary table. The classification of legal nature used in the program incorporates only the first digit of this hierarchy, differentiating groups such as public administration, non-profit entities and commercial companies.

The synthetic records maintain consistent characteristics and proportions of the POAM, POTM and SALM variables with the original SIDRA data when reaggregated by municipality, legal nature and economic activity. The file also preserves the numerical totals and the relationships between variables when compared to the real statistics.

---

### Input Files

The program uses the following input files:

1. **AGREG.xlsx**: File containing aggregated data by municipality (UFMUN) and CNAE division (CNAE_DIV). This file, extracted from SIDRA, https://sidra.ibge.gov.br/tabela/9418 CNAE division, Municipalities, 2022 has columns that include:
- **UF**: Federative Unit (hierarchy higher than the municipality).
- **UFMUN**: Municipality code.
- **CNAE_DIV**: CNAE Division (two digits - second-level hierarchy of the CNAE classification).
- **NULS**: Number of records (companies) associated with the aggregate.
- **POA**: Salaried employed personnel.
- **POT**: Total employed personnel.
- **SAL**: Total salaries.
- Some records contain "X" instead of numeric values, indicating that the information is omitted due to confidentiality but related to less than three companies.

2. **CNAE.xlsx**: File containing CNAE categories with five-digit codes. This file is used to randomly assign classes to two-digit CNAE divisions, allowing for disaggregation at the most detailed level.

3. **pnatjur.xlsx**: File containing proportions between types of legal nature (hierarchy of the first digit of the legal nature), such as public administration, non-profit entities, and commercial companies associated with CNAE divisions for each federative unit (UF). This file ensures the synthetic data respects the original proportions between economic activities and legal nature.

### Statistical Distributions Used

The program uses specific statistical distributions to simulate and disaggregate aggregated data:

1. **Beta Distribution**:

We proportionally distribute `POA` (salaried employed personnel) values ​​among companies (NULS) using Beta distribution, which is ideally flexible and can model characteristics present in economic variables, such as asymmetry and disproportionality. We normalized and adjusted the values to ensure that the sum equals the aggregated total.

2. **Dirichlet Distribution**:

We distribute the difference between `POT` and `POA` (non-salaried employed personnel) proportionally among companies using Dirichlet distribution, a generalization of Beta for multiple proportions, ensuring that the sum of the parts correctly distributes the total. It ensures that the aggregate total coincides with the sum of the distribution of values.

3. **Normal Distribution with Noise**:

- **Objective**: Add controlled noise to the wage distribution (`SAL`) to simulate realistic variations. Noise or error is used to establish causality between two or more variables without a fixed proportionality between them, thus allowing variations that approximate reality.

4. **Adjustments**:

After applying the distributions, iterative adjustments are made to correct minor discrepancies (e.g., rounding) and ensure that the total sum of the synthetic values ​​corresponds precisely to the aggregated values.

---

### Processing Flow

1. **Data Loading and Preprocessing**:
- The `AGREG.xlsx` file is loaded and processed to replace missing values ​​(`-`) with zero and separate the records with "X." The records with "X" indicate confidential data that needs to be filled in later based on the residuals.

2. **Processing of Numerical Records**:
- The aggregated values ​​of `POA,` `POT,` and `SAL` are disaggregated proportionally using the mentioned statistical distributions, respecting the number of records (`NULS`).

- **POAM** is generated using the Beta distribution applied to the aggregated total of `POA,` ensuring a consistent proportion between records.
- **POTM** is calculated by adding the **POAM** values ​​to the difference between `POT` and `POA,` distributed proportionally using the Dirichlet distribution.

3. **Distribution of Residual Values**:
- Residuals (unassigned values ​​in records with "X") are calculated and distributed proportionally among these records. This step ensures that the totals of the synthetic dataset are aligned with the overall values ​​provided by SIDRA.

4. **Data Enrichment**:
- **CNAE Class Assignment**: We disaggregate CNAE divisions into five-digit classes, randomly assigned based on the codes in the `CNAE.xlsx` file.
- **Proportion of Legal Nature**: Types of legal nature are assigned respecting the proportions defined in the `pnatjur.xlsx` file.

5. **Finalization and Export**:
- Additional columns include unique identifiers, dates (`DATA_INI,` `DATA_FIM`), and other necessary variables.
- Data is logically ordered and exported to CSV files, generating eight batches with different random seeds to simulate data variations.

---
### Final File Structure

The final files have the following variables:
1. **IDENTIFIER**: Unique code.
2. **UF**: Federative Unit.
3. **UFMUN**: Municipality code.
4. **CNAE_DIV**: CNAE Division (two digits).
5. **POAM**: Salaried employed personnel disaggregated proportionally using Beta distribution.
6. **POTM**: Using Dirichlet distribution, total employed personnel disaggregated, derived from **POAM** and the difference between `POT` and `POA.`
7. **SALM**: Total disaggregated salaries.
8. **COD_CNAE**: CNAE Class (five digits).
9. **DATA_INI**: Start date (2024-01-01 12:00:00).
10. **DATA_FIM**: End date (empty).
11. **NATJUR**: Legal nature assigned based on the `pnatjur.xlsx` file.

---

This program ensures that the data generated are synthetic, consistent with the original aggregated values ​​, and sufficiently detailed for more specific analyses, preserving the essential proportions and characteristics of the SIDRA database. The explanation of the use of statistical distributions reinforces the accuracy and validity of the adopted model.