import pandas as pd
from scipy.stats import shapiro

# Load the input file
df = pd.read_csv('input.csv', sep='\t')
df.columns = df.columns.str.strip().str.upper()

# Convert TOTAL_TIME to float and drop rows with missing values
df['TOTAL_TIME'] = pd.to_numeric(df['TOTAL_TIME'], errors='coerce')
df = df.dropna(subset=['TOTAL_TIME'])

# Store test results
results = []

# Group by TIPO and COD_VER_EXEC
grouped = df.groupby(['TIPO', 'COD_VER_EXEC'])

for (tipo, cod_ver), group in grouped:
    if len(group) >= 3:  # Shapiro-Wilk requires at least 3 observations
        stat, p = shapiro(group['TOTAL_TIME'])
        result = 'REJECT H0 (not normal)' if p < 0.05 else 'DO NOT REJECT H0 (normal)'
        results.append({
            'TIPO': tipo,
            'COD_VER_EXEC': cod_ver,
            'N': len(group),
            'W-Statistic': stat,
            'p-Value': p,
            'Result': result
        })

# Convert results to DataFrame and print all rows/columns
results_df = pd.DataFrame(results)
with pd.option_context('display.max_rows', None, 'display.max_columns', None):
    print(results_df)

# Export to CSV using semicolon as separator
results_df.to_csv('report_shapiro_wilk.csv', sep=';', index=False)
