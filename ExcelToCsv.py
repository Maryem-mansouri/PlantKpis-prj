import pandas as pd
import os

# Define the paths
week33 = r'C:\Users\TE582412\Desktop\Plant KPIS\Automotive\New data history\MFG AUT FW33.xlsx'
week34 = r'C:\Users\TE582412\Desktop\Plant KPIS\Automotive\New data history\MFG AUT FW34 27.05.xlsx'
week35 = r'C:\Users\TE582412\Desktop\Plant KPIS\Automotive\New data history\MFG AUT FW35 03.06.24.xlsx'

# New paths for the CSV files
output_dir = r'C:\Users\TE582412\Desktop\Plant KPIS\Automotive\New data history\Csv Data'

# Convert function
def convert_excel_to_csv(excel_path, output_dir, sheet_name=None):
    # Read the Excel file
    xls = pd.ExcelFile(excel_path, engine='openpyxl')
    
    if sheet_name:
        sheet_names = [sheet_name]
    else:
        sheet_names = xls.sheet_names
    
    for sheet in sheet_names:
        # Read the sheet with dtype specified for Material Number
        df = pd.read_excel(xls, sheet_name=sheet, dtype={'Material Number': str})
        
        # Define the CSV path
        base_name = os.path.basename(excel_path).replace('.xlsx', '')
        csv_path = os.path.join(output_dir, f'CSV_{base_name}_{sheet}.csv')
        
        # Ensure the directory for the CSV path exists
        os.makedirs(os.path.dirname(csv_path), exist_ok=True)
        
        # Save as CSV
        df.to_csv(csv_path, index=False)
        print(f"Converted {excel_path} sheet '{sheet}' to {csv_path}")

# Convert each file with the new paths
convert_excel_to_csv(week33, output_dir, sheet_name='Inventory')  # specify the sheet name if needed
convert_excel_to_csv(week34, output_dir, sheet_name='Inventory')  # specify the sheet name if needed
convert_excel_to_csv(week35, output_dir, sheet_name='Report 1')  # specify the sheet name if needed
