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
3. **bash**: python -m src.main
   
## Run the application

1. Configuration: Load your Service Account JSON.
3. Source: Select a Google Sheet tab or a TSV file.
4. Target: Select a Google Sheet tab.
5. Keys & Columns:
   - Click "Load Headers".
   - Select the unique Row Key (e.g., ID).
   - Select the Marker Column to update on sync.
   - Check the columns you want to compare.
6. Actions:
   - Check Diffs: Generates a text report.
   - Color Diffs: Highlights changes in the sheet.
   - Sync & Color: Updates data and highlights changes.