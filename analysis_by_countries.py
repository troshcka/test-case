from gdrive_processors import GSheet
import pandas as pd
from constants import DATASET


def retrieve_dataset(sheet_name: str) -> list:
    gsheet = GSheet()
    return gsheet.get_all_data(sheet_name)


dataset = retrieve_dataset(DATASET)

df = pd.DataFrame(dataset)

# Extract the 'Country' from the 'Campaign name' using a lambda function
df['Country'] = df['Campaign name'].apply(lambda x: x.split(',')[0].strip())

# Convert the 'Reporting ends' column to datetime objects
df['Reporting ends'] = pd.to_datetime(df['Reporting ends'])

# Extract the 'Reporting ends' month as a string in 'YYYY-MM' format
df['Month'] = df['Reporting ends'].dt.strftime('%Y-%m')

# Convert the 'App installs' column from strings to integers and
# replace empty strings with NaN
df['App installs'] = pd.to_numeric(df['App installs'], errors='coerce')

# Convert the 'Mobile app registrations completed' column from strings
# to integers and replace empty strings with NaN
df['Mobile app registrations completed'] = pd.to_numeric(
    df['Mobile app registrations completed'],
    errors='coerce')

# Convert the 'Purchases' column from strings to integers and
# replace empty strings with NaN
df['Purchases'] = pd.to_numeric(df['Purchases'],
                                errors='coerce')


# Convert the 'Unique purchases' column from strings to integers and
# replace empty strings with NaN
df['Unique purchases'] = pd.to_numeric(df['Unique purchases'],
                                       errors='coerce')


df['Revenue'] = df['Purchases'] * df['Revenue per purchase (EUR)']


def format_numbers_with_separator(x):
    return "{:,.1f}".format(x).replace(",", " ")


# Pivot the DataFrame to create the pivot table
pivot_table_impressions_per_country = (
    df.groupby(['Country', 'Month'])['Impressions']
    .sum()
    .unstack(fill_value=0)
    .applymap(format_numbers_with_separator)
)

pivot_table_installs_per_country = (
    df.groupby(['Country', 'Month'])['App installs']
    .sum()
    .unstack(fill_value=0)
    .applymap(format_numbers_with_separator)
)

pivot_table_registrations_per_country = (
    df.groupby(['Country', 'Month'])['Mobile app registrations completed']
    .sum()
    .unstack(fill_value=0)
    .applymap(format_numbers_with_separator)
)

pivot_table_purchases_per_country = (
    df.groupby(['Country', 'Month'])['Purchases']
    .sum()
    .unstack(fill_value=0)
    .applymap(format_numbers_with_separator)
)

pivot_table_unique_purchases_per_country = (
    df.groupby(['Country', 'Month'])['Unique purchases']
    .sum()
    .unstack(fill_value=0)
    .applymap(format_numbers_with_separator)
)

pivot_table_cost_per_country = (
    df.groupby(['Country', 'Month'])['Amount spent (EUR)']
    .sum()
    .unstack(fill_value=0)
    .applymap(format_numbers_with_separator)
)

pivot_table_revenue_per_country = (
    df.groupby(['Country', 'Month'])['Revenue']
    .sum()
    .unstack(fill_value=0)
    .applymap(format_numbers_with_separator)
)


def remove_non_numeric_char(value):
    # Remove any non-numeric characters from the value
    value = ''.join(filter(str.isdigit, str(value)))
    return float(value) if value else 0


def calculate_change_mom(pivot_table):
    # Calculate the dynamic change per month
    change_mom = pivot_table.pct_change(axis=1) * 100

    # Reformat the dynamic_change DataFrame to add percentage symbols
    change_mom = change_mom.iloc[:, 1:]
    change_mom = change_mom.applymap(lambda x: f'{x:.1f}%'
                                     if not pd.isnull(x) else '')
    return change_mom


impressions_change_mom = calculate_change_mom(
    pivot_table_impressions_per_country.
    applymap(remove_non_numeric_char))
installs_change_mom = calculate_change_mom(
    pivot_table_installs_per_country.
    applymap(remove_non_numeric_char))
regs_change_mom = calculate_change_mom(
    pivot_table_registrations_per_country.
    applymap(remove_non_numeric_char))
purchases_change_mom = calculate_change_mom(
    pivot_table_purchases_per_country.
    applymap(remove_non_numeric_char))
unique_purchases_change_mom = calculate_change_mom(
    pivot_table_unique_purchases_per_country.
    applymap(remove_non_numeric_char))
ad_spent_change_mom = calculate_change_mom(
    pivot_table_cost_per_country.
    applymap(remove_non_numeric_char))
revenue_change_mom = calculate_change_mom(
    pivot_table_revenue_per_country.
    applymap(remove_non_numeric_char))

cpm = (pivot_table_cost_per_country.
       applymap(remove_non_numeric_char) * 1_000 /
       pivot_table_impressions_per_country.
       applymap(remove_non_numeric_char))

cr_installs_to_registrations = (pivot_table_registrations_per_country.
                                applymap(remove_non_numeric_char) /
                                pivot_table_installs_per_country.
                                applymap(remove_non_numeric_char))

# Convert CR to a formatted string
cr_installs_to_registrations = (cr_installs_to_registrations.
                                applymap(lambda x: f'{x:.2%}'))


cr_registrations_to_purchases = (pivot_table_purchases_per_country.
                                 applymap(remove_non_numeric_char) /
                                 pivot_table_registrations_per_country.
                                 applymap(remove_non_numeric_char))

cr_registrations_to_purchases = (cr_registrations_to_purchases.
                                 applymap(lambda x: f'{x:.2%}'))


# Group by 'Country' and 'Month' and calculate the total amount spent and
# app installs per month for each country
grouped_df = df.groupby(['Country', 'Month']).agg(
    {'Amount spent (EUR)': 'sum', 'App installs': 'sum'}
).reset_index()

# Calculate the 'Amount spent/App installs' ratio
grouped_df['CPI'] = (grouped_df['Amount spent (EUR)'] /
                     grouped_df['App installs'])

pivot_table_cpi_by_country = grouped_df.pivot_table(values='CPI',
                                                    index='Country',
                                                    columns='Month',
                                                    fill_value=0)

# Group by 'Country' and 'Month' and calculate the total spend and
# mobile app registrations complete per month for each country
grouped_df = df.groupby(['Country', 'Month']).agg(
    {'Amount spent (EUR)': 'sum', 'Mobile app registrations completed': 'sum'}
).reset_index()

# Calculate CPR ('Amount spent/Mobile app registrations complete')
grouped_df['CPR'] = (grouped_df['Amount spent (EUR)'] /
                     grouped_df['Mobile app registrations completed'])

pivot_table_cpr_by_country = grouped_df.pivot_table(values='CPR',
                                                    index='Country',
                                                    columns='Month',
                                                    fill_value=0)

# Group by 'Country' and 'Month' and calculate the total spend and
# purchases per month for each country
grouped_df = df.groupby(['Country', 'Month']).agg(
    {'Amount spent (EUR)': 'sum', 'Purchases': 'sum'}
).reset_index()

grouped_df['CPO'] = (grouped_df['Amount spent (EUR)'] /
                     grouped_df['Purchases'])

pivot_table_cpo_by_country = grouped_df.pivot_table(values='CPO',
                                                    index='Country',
                                                    columns='Month',
                                                    fill_value=0)

grouped_df = df.groupby(['Country', 'Month']).agg(
    {'Revenue': 'sum', 'Purchases': 'sum'}
).reset_index()

grouped_df['AOV'] = (grouped_df['Revenue'] /
                     grouped_df['Purchases'])

pivot_table_aov_by_country = grouped_df.pivot_table(values='AOV',
                                                    index='Country',
                                                    columns='Month',
                                                    fill_value=0)


def save_pivot_table(pivot_table,
                     cell_for_col_names,
                     cell_for_row_names,
                     cell_for_values,
                     cell_for_table_name,
                     table_name) -> None:
    gsheet = GSheet()
    values = pivot_table.values.tolist()
    col_names = pivot_table.columns.tolist()
    row_names = pivot_table.index.tolist()

    # Insert the column names as the first row in the worksheet
    gsheet.update_row('pivot tables - countries data', cell_for_col_names,
                      [col_names])

    # Insert the row names as the first column in the worksheet
    # (starting from the second row)
    gsheet.update_row('pivot tables - countries data', cell_for_row_names,
                      [[row_name] for row_name in row_names])

    # Insert the data starting from the second row and second column
    gsheet.update_row('pivot tables - countries data', cell_for_values,
                      values)

    # Insert the data starting from the second row and second column
    gsheet.update_row('pivot tables - countries data', cell_for_table_name,
                      table_name)


save_pivot_table(pivot_table_impressions_per_country,
                 'B1', 'A2', 'B2', 'A1', 'Impressions, #')
save_pivot_table(pivot_table_installs_per_country,
                 'B5', 'A6', 'B6', 'A5', 'Installs, #')
save_pivot_table(pivot_table_registrations_per_country,
                 'B9', 'A10', 'B10', 'A9',
                 'Mobile app registrations completed, #')
save_pivot_table(pivot_table_purchases_per_country,
                 'B13', 'A14', 'B14', 'A13', 'Purchases, #')
save_pivot_table(pivot_table_unique_purchases_per_country,
                 'B17', 'A18', 'B18', 'A17', 'Unique purchases, #')
save_pivot_table(pivot_table_cost_per_country,
                 'B21', 'A22', 'B22', 'A21', 'Amount spent (EUR)')
save_pivot_table(pivot_table_revenue_per_country,
                 'B25', 'A26', 'B26', 'A25', 'Revenue (EUR)')
save_pivot_table(impressions_change_mom,
                 'G1', 'F2', 'G2', 'F1', 'Impressions Change MoM')
save_pivot_table(installs_change_mom,
                 'G5', 'F6', 'G6', 'F5', 'Installs Change MoM')
save_pivot_table(regs_change_mom,
                 'G9', 'F10', 'G10', 'F9', 'Regs Change MoM')
save_pivot_table(purchases_change_mom,
                 'G13', 'F14', 'G14', 'F13', 'Purchases Change MoM')
save_pivot_table(unique_purchases_change_mom,
                 'G17', 'F18', 'G18', 'F17', 'Unique Purchases Change MoM')
save_pivot_table(ad_spent_change_mom,
                 'G21', 'F22', 'G22', 'F21', 'Amount Spent Change MoM')
save_pivot_table(revenue_change_mom,
                 'G25', 'F26', 'G26', 'F25', 'Revenue Change MoM')
save_pivot_table(cpm,
                 'K1', 'J2', 'K2', 'J1', 'CPM')
save_pivot_table(cr_installs_to_registrations,
                 'K5', 'J6', 'K6', 'J5', 'CR Installs 2 Registrations')
save_pivot_table(cr_registrations_to_purchases,
                 'K9', 'J10', 'K10', 'J9', 'CR Registrations 2 Purchases')
save_pivot_table(pivot_table_cpi_by_country,
                 'K13', 'J14', 'K14', 'J13', 'CPI (EUR)')
save_pivot_table(pivot_table_cpr_by_country,
                 'K17', 'J18', 'K18', 'J17', 'CPRegistration (EUR)')
save_pivot_table(pivot_table_cpo_by_country,
                 'K21', 'J22', 'K22', 'J21', 'CPO (EUR)')
save_pivot_table(pivot_table_aov_by_country,
                 'K25', 'J26', 'K26', 'J25', 'AOV (EUR)')
