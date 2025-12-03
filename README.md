# Compare & Sync Sheets

A PySide6 Desktop application to compare a Source (Google Sheet or TSV) with a Target (Google Sheet), visualize differences, and sync data.

## Features

- **Compare**: Check for data differences based on a key column.
- **Sync**: Update Target values to match Source.
- **Two-Way Marker Update**: Automatically update a "Status" or "Date" marker column in both Source and Target when a sync occurs.
- **Coloring**: Highlight differences in Google Sheets with custom colors.
- **Dimming**: Fade existing colors to visually track history of changes.
- **State Saving**: Remembers your configuration per Target sheet.

## Installation

1. Clone the repository.
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Run the application:
   ```bash
   python main.py
   ```
   
## Run the application

1. **Configuration**: Load your Service Account JSON.
2. **Source**: Select a Google Sheet tab or a TSV file.
3. **Target**: Select a Google Sheet tab.
4. **Keys & Columns**:
   - Click "Load Headers".
   - Select the unique **Row Key** (e.g., ID).
   - Select the **Update Marker Column** (optional) to update on sync.
   - Check the columns you want to compare.
5. **Main Actions**:
   - **Color Management**: Clear or Dim colors in Source/Target to prepare for a new pass.
   - **Check Diffs**: Generates a text report of data differences.
   - **Check Colors**: Checks if colored cells match the data differences (and compares Source vs Target colors).
   - **Color Diffs**: Highlights data differences in the sheet.
   - **Sync & Color**: Updates data in Target and highlights changes.