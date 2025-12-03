# -*- coding: utf-8 -*-
"""
PySide6 UI for CompareAndSyncSheets.
"""
import os
import json
import re
import datetime as dt
from datetime import datetime
from typing import Optional, List, Any, Tuple

from PySide6.QtWidgets import (
    QApplication, QWidget, QLabel, QLineEdit, QPushButton, QVBoxLayout, QHBoxLayout,
    QFileDialog, QComboBox, QTextEdit, QMessageBox, QGroupBox, QFormLayout, QSplitter,
    QProgressBar, QListWidget, QListWidgetItem, QFrame
)
from PySide6.QtCore import Qt, Signal, QTimer, QSize
from PySide6.QtGui import QPixmap, QColor

from src.logic import (
    SheetsClient, TSVClient, compare_two_sheets, check_color_status, compare_sheet_colors,
    WHITE, DIM_BLEND_FACTOR, rgb_to_hsv, hsv_to_rgb, is_white
)

# --- Constants ---
DEFAULT_CREDENTIALS_PATH = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")
CONFIG_PATH = "D:\\Resolve\\Data\\_google_sync"
LOG_PATH = "D:\\Resolve\\Data\\_google_sync\\logs"
LAST_SESSION_CONFIG = os.path.join(CONFIG_PATH, "last_session.json")

BRIGHT_COLORS = {
    "Yellow": {"red": 1.0, "green": 1.0, "blue": 0.0},
    "Orange": {"red": 1.0, "green": 0.6, "blue": 0.0},
    "Magenta": {"red": 1.0, "green": 0.0, "blue": 1.0},
    "Blue": {"red": 0.0, "green": 0.6, "blue": 1.0},
    "Cyan": {"red": 0.0, "green": 1.0, "blue": 1.0},
    "Lime": {"red": 0.6, "green": 1.0, "blue": 0.0},
}
DEFAULT_COLOR_NAME = "Yellow"
DEFAULT_COMPARE_HEADERS = ["BIDDING", "STATUS", "META", "TYPE", "TASK", "NOTES_SUP", "AI", "ALPHA", "ON-SET", "PLATE-PULL", "ASSETS"]

class CompareSyncUI(QWidget):
    busy = Signal(bool)

    def __init__(self):
        super().__init__()
        self.setWindowTitle("Google Sheets / TSV — Compare & Sync")
        self.client: Optional[SheetsClient] = None
        self.tsv = TSVClient()
        
        # State holders for loaded preferences before headers are actually loaded
        self.saved_compare_headers: List[str] = []
        self.saved_marker_col: str = ""

        self._init_ui()
        self._wire_signals()
        self._refresh_source_visibility(self.src_type.currentText())
        
        # Defer auto-load so UI shows first
        QTimer.singleShot(200, self._startup_load)

    def _startup_load(self):
        """Called after UI is visible to load previous session."""
        self._load_last_session_on_startup()

    def _init_ui(self):
        os.makedirs(CONFIG_PATH, exist_ok=True)
        os.makedirs(LOG_PATH, exist_ok=True)

        # Widgets
        self.cred_edit = QLineEdit(DEFAULT_CREDENTIALS_PATH)
        self.cred_btn, self.connect_btn = QPushButton("Browse…"), QPushButton("Connect")
        self.load_all = QPushButton("Load All")
        self.save_all = QPushButton("Save All")
        
        self.src_type = QComboBox()
        self.src_type.addItems(["Google Sheet", "TSV File"])
        self.src_id, self.src_file = QLineEdit(), QLineEdit()
        self.src_file_btn = QPushButton("…")
        self.src_list = QComboBox()
        self.load_src_btn = QPushButton("Load Source Tabs")
        
        self.tgt_id, self.tgt_list = QLineEdit(), QComboBox()
        self.load_tgt_btn = QPushButton("Load Target Tabs")
        
        self.key_header = QComboBox()
        self.refresh_headers_btn = QPushButton("Load Headers")
        
        # ListWidget as Grid
        self.compare_list = QListWidget()
        self.compare_list.setMaximumHeight(75)
        self.compare_list.setFlow(QListWidget.LeftToRight) # Enable flow
        self.compare_list.setWrapping(True) # Enable wrapping to form grid
        self.compare_list.setResizeMode(QListWidget.Adjust)
        self.compare_list.setSpacing(4)
        
        self.update_marker_combo = QComboBox()
        
        self.base_color_combo = QComboBox()
        self.base_color_combo.addItems(BRIGHT_COLORS.keys())
        
        self.src_clear_colors_btn = QPushButton("Clear Source Colors")
        self.src_dim_colors_btn = QPushButton("Dim Source Colors")
        self.tgt_clear_colors_btn = QPushButton("Clear Target Colors")
        self.tgt_dim_colors_btn = QPushButton("Dim Target Colors")
        
        self.check_btn = QPushButton("Check Diffs")
        self.check_color_btn = QPushButton("Check Colors")
        self.highlight_btn = QPushButton("Color all Diffs")
        self.sync_btn = QPushButton("Sync Data and Color")
        
        self.report = QTextEdit(readOnly=True)
        self.report_clear_btn = QPushButton("Clear")
        self.log_load_btn = QPushButton("Load Log")
        self.status_icon = QLabel()
        self.status_icon.setFixedSize(14, 14)
        self._set_status_color("grey")
        self.progress = QProgressBar(visible=False, textVisible=False)
        
        # Layout
        root_layout, config_box = QVBoxLayout(self), QGroupBox("Configuration")
        config_layout = QFormLayout(config_box)
        cred_row = QHBoxLayout()
        cred_row.addWidget(self.cred_edit)
        cred_row.addWidget(self.cred_btn)
        cred_row.addWidget(self.connect_btn)
        config_layout.addRow("Credentials JSON:", cred_row)
        root_layout.addWidget(config_box)

        main_splitter = QSplitter(Qt.Horizontal)
        root_layout.addWidget(main_splitter)
        
        # Source Box
        src_box = QGroupBox("Source")
        src_layout = QFormLayout(src_box)
        src_layout.addRow("Type:", self.src_type)
        self.src_sheet_widgets = QWidget()
        sheet_layout = QFormLayout(self.src_sheet_widgets)
        sheet_layout.setContentsMargins(0,0,0,0)
        sheet_layout.addRow("Spreadsheet ID:", self.src_id)
        sheet_layout.addRow("Worksheet:", self.src_list)
        sheet_layout.addRow(self.load_src_btn)
        src_layout.addRow(self.src_sheet_widgets)
        
        self.src_file_widgets = QWidget()
        file_layout = QFormLayout(self.src_file_widgets)
        file_layout.setContentsMargins(0,0,0,0)
        file_row = QHBoxLayout()
        file_row.addWidget(self.src_file)
        file_row.addWidget(self.src_file_btn, 0)
        file_layout.addRow("TSV File:", file_row)
        src_layout.addRow(self.src_file_widgets)
        main_splitter.addWidget(src_box)

        # Target Box
        tgt_box = QGroupBox("Target (Google Sheet)")
        tgt_layout = QFormLayout(tgt_box)
        tgt_layout.addRow("Spreadsheet ID:", self.tgt_id)
        tgt_layout.addRow("Worksheet:", self.tgt_list)
        tgt_layout.addRow(self.load_tgt_btn)
        main_splitter.addWidget(tgt_box)
        
        # Keys & Columns Box
        key_box = QGroupBox("Key & Columns")
        key_layout = QVBoxLayout(key_box) # Changed to VBox for custom rows
        
        # Combined Header Row
        header_ctrl_layout = QHBoxLayout()
        header_ctrl_layout.addWidget(QLabel("Row Key Header:"))
        header_ctrl_layout.addWidget(self.key_header, 2) # Give key header more space
        header_ctrl_layout.addStretch(1)
        header_ctrl_layout.addWidget(QLabel("Update Marker Column:"))
        header_ctrl_layout.addWidget(self.update_marker_combo, 2)
        header_ctrl_layout.addWidget(self.refresh_headers_btn)
        header_ctrl_layout.addStretch(1)
        header_ctrl_layout.addWidget(self.load_all)
        header_ctrl_layout.addWidget(self.save_all)
        key_layout.addLayout(header_ctrl_layout)
        
        # Columns Grid
        key_layout.addWidget(self.compare_list)
        
        root_layout.addWidget(key_box)
        
        # Actions
        main_action_box = QGroupBox("Main Actions")
        main_action_layout = QVBoxLayout(main_action_box)
        
        # Row 1: Color Management (Clear/Dim)
        color_mgmt_row = QHBoxLayout()
        color_mgmt_row.addWidget(self.src_clear_colors_btn)
        color_mgmt_row.addWidget(self.src_dim_colors_btn)
        color_mgmt_row.addStretch(1) # Spacer between Source and Target buttons
        color_mgmt_row.addWidget(self.tgt_clear_colors_btn)
        color_mgmt_row.addWidget(self.tgt_dim_colors_btn)
        main_action_layout.addLayout(color_mgmt_row)

        # Row 2: Main Operations
        ops_row = QHBoxLayout()
        ops_row.addWidget(QLabel("Base Color:"))
        ops_row.addWidget(self.base_color_combo)
        ops_row.addStretch(1)
        ops_row.addWidget(self.check_btn)
        ops_row.addWidget(self.check_color_btn)
        ops_row.addWidget(self.highlight_btn)
        ops_row.addWidget(self.sync_btn)
        main_action_layout.addLayout(ops_row)
        
        root_layout.addWidget(main_action_box)

        # Report
        report_box = QGroupBox("Report")
        report_layout = QVBoxLayout(report_box)
        status_row = QHBoxLayout()
        status_row.addWidget(QLabel("Status:"))
        status_row.addWidget(self.status_icon, 0)
        status_row.addStretch(1)
        status_row.addWidget(self.log_load_btn)
        status_row.addWidget(self.report_clear_btn)
        report_layout.addLayout(status_row)
        report_layout.addWidget(self.report)
        report_layout.addWidget(self.progress)
        root_layout.addWidget(report_box, 1)

    def _wire_signals(self):
        self.busy.connect(self._set_busy)
        self.cred_btn.clicked.connect(self._pick_credentials)
        self.load_all.clicked.connect(self._load_all_data)
        self.save_all.clicked.connect(self._save_ui_state)
        self.connect_btn.clicked.connect(self._connect)
        self.src_type.currentTextChanged.connect(self._refresh_source_visibility)
        self.load_src_btn.clicked.connect(self._load_src_tabs)
        self.src_file_btn.clicked.connect(lambda: self._pick_file(self.src_file))
        self.load_tgt_btn.clicked.connect(self._load_tgt_tabs)
        self.refresh_headers_btn.clicked.connect(self._populate_key_headers)
        self.src_clear_colors_btn.clicked.connect(lambda: self._clear_colors("source"))
        self.tgt_clear_colors_btn.clicked.connect(lambda: self._clear_colors("target"))
        self.src_dim_colors_btn.clicked.connect(lambda: self._dim_colors("source"))
        self.tgt_dim_colors_btn.clicked.connect(lambda: self._dim_colors("target"))
        
        self.check_btn.clicked.connect(self._check_only)
        self.check_color_btn.clicked.connect(self._check_color_only)
        self.highlight_btn.clicked.connect(lambda: self._run(sync=False))
        self.sync_btn.clicked.connect(lambda: self._run(sync=True))
        
        self.report_clear_btn.clicked.connect(self.report.clear)
        self.log_load_btn.clicked.connect(self._load_log_file)
        self.tgt_list.currentTextChanged.connect(self._on_target_change)

    def _set_status_color(self, color: str):
        pix = QPixmap(14, 14)
        pix.fill(QColor(color))
        self.status_icon.setPixmap(pix)
        
    def _set_busy(self, busy: bool):
        self.progress.setVisible(busy)
        self._set_status_color("yellow" if busy else "green")
        for w in self.findChildren(QPushButton): 
            w.setEnabled(not busy)
        # Also disable inputs during busy state
        self.compare_list.setEnabled(not busy)
        self.update_marker_combo.setEnabled(not busy)
        QApplication.processEvents()
        
    def _refresh_source_visibility(self, text: str):
        is_sheet = text == "Google Sheet"
        self.src_sheet_widgets.setVisible(is_sheet)
        self.src_file_widgets.setVisible(not is_sheet)

    def _ensure_client(self) -> bool:
        if self.client is None: self._connect()
        return self.client is not None

    def _pick_credentials(self):
        fn, _ = QFileDialog.getOpenFileName(self, "Select Service Account JSON", "", "*.json")
        if fn: self.cred_edit.setText(fn)

    def _connect(self):
        path = self.cred_edit.text().strip()
        if not path or not os.path.isfile(path):
            QMessageBox.warning(self, "Missing Credentials", "Select a valid service account JSON file.")
            return
        self.busy.emit(True)
        try:
            self.client = SheetsClient(path)
            self._set_status_color("green")
            QMessageBox.information(self, "Connected", "Google Sheets client authorized.")
        except Exception as e:
            self.client = None
            self._set_status_color("red")
            QMessageBox.critical(self, "Auth Failed", f"Could not connect: {e}")
        finally:
            self.busy.emit(False)
    
    def _pick_file(self, edit: QLineEdit):
        fn, _ = QFileDialog.getOpenFileName(self, "Select TSV File", "", "*.tsv *.txt")
        if fn: edit.setText(fn)

    def _load_tabs(self, id_w: QLineEdit, list_w: QComboBox, kind: str):
        if not self._ensure_client(): return
        sid = id_w.text().strip()
        if not sid:
            QMessageBox.warning(self, "Missing ID", f"Enter {kind} Spreadsheet ID.")
            return
        self.busy.emit(True)
        try:
            tabs = self.client.list_worksheets(sid)
            current_tab = list_w.currentText()
            list_w.clear()
            list_w.addItems(tabs)
            if current_tab in tabs: list_w.setCurrentText(current_tab)
            self.report.append(f"Loaded {len(tabs)} {kind} tabs for sheet ID: {sid}")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to list {kind} tabs: {e}")
            self._set_status_color("red")
        finally:
            self.busy.emit(False)

    def _load_src_tabs(self):
        self._load_tabs(self.src_id, self.src_list, "Source")
        
    def _load_tgt_tabs(self):
        self._load_tabs(self.tgt_id, self.tgt_list, "Target")

    def _populate_key_headers(self):
        self.busy.emit(True)
        try:
            s_headers, _ = self._load_table("source")
            if not s_headers:
                QMessageBox.warning(self, "No Headers", "Source has no headers.")
                return
            
            # Preserve selections if possible
            current_key = self.key_header.currentText()
            
            # Populate Key Header
            self.key_header.clear()
            self.key_header.addItems([h for h in s_headers if h])
            if current_key in s_headers:
                self.key_header.setCurrentText(current_key)
                
            # Populate Update Marker Combo
            self.update_marker_combo.clear()
            self.update_marker_combo.addItem("") # Allow empty selection
            self.update_marker_combo.addItems([h for h in s_headers if h])
            if self.saved_marker_col and self.saved_marker_col in s_headers:
                 self.update_marker_combo.setCurrentText(self.saved_marker_col)

            # Populate Columns to Compare (Checkboxes)
            self.compare_list.clear()
            for h in s_headers:
                if not h: continue
                item = QListWidgetItem(h)
                item.setFlags(item.flags() | Qt.ItemIsUserCheckable)
                
                # Check by default if in saved list, or if no saved list, check defaults
                should_check = False
                if self.saved_compare_headers:
                    should_check = h in self.saved_compare_headers
                else:
                    should_check = h in DEFAULT_COMPARE_HEADERS
                
                item.setCheckState(Qt.Checked if should_check else Qt.Unchecked)
                self.compare_list.addItem(item)
            
            self.report.append(f"Loaded {len(s_headers)} headers from source.")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to load headers: {e}")
            self._set_status_color("red")
        finally:
            self.busy.emit(False)

    def _load_table(self, kind: str) -> Tuple[List[str], List[List[Any]]]:
        if kind == "source":
            if self.src_type.currentText() == "Google Sheet":
                if not self._ensure_client(): return [], []
                sid, stab = self.src_id.text().strip(), self.src_list.currentText()
                if not sid or not stab: raise ValueError("Source Spreadsheet/Worksheet not set.")
                return self.client.fetch_values(sid, stab)
            filepath = self.src_file.text().strip()
            if not filepath: raise ValueError("Source TSV file path not set.")
            return self.tsv.fetch_values(filepath)
        else: # target
            if not self._ensure_client(): return [], []
            tid, ttab = self.tgt_id.text().strip(), self.tgt_list.currentText()
            if not tid or not ttab: raise ValueError("Target Spreadsheet/Worksheet not set.")
            return self.client.fetch_values(tid, ttab)

    def _get_run_params(self):
        key = self.key_header.currentText().strip()
        sid, stab = self.src_id.text().strip(), self.src_list.currentText()
        tid, ttab = self.tgt_id.text().strip(), self.tgt_list.currentText()
        is_source_sheet = self.src_type.currentText() == "Google Sheet"
        update_marker_col = self.update_marker_combo.currentText().strip()

        if not key: raise ValueError("Key header must be selected.")
        if not tid or not ttab: raise ValueError("Target sheet and tab must be specified.")
        if is_source_sheet and (not sid or not stab):
            raise ValueError("Source sheet and tab must be specified for this operation.")
            
        included = []
        for i in range(self.compare_list.count()):
            item = self.compare_list.item(i)
            if item.checkState() == Qt.Checked:
                included.append(item.text())
        
        if not included:
             raise ValueError("No columns selected for comparison.")

        return key, included, sid, stab, tid, ttab, is_source_sheet, update_marker_col

    def _check_only(self):
        self.busy.emit(True)
        try:
            key, included, _, _, _, _, _, _ = self._get_run_params()
            s_h, s_r = self._load_table("source")
            t_h, t_r = self._load_table("target")
            result = compare_two_sheets(s_h, s_r, t_h, t_r, key, included)
            self.report.append(result.to_report())
            QMessageBox.information(self, "Check Complete", "Comparison finished. No changes made.")
            self._save_ui_state()
        except Exception as e:
            QMessageBox.critical(self, "Error", str(e))
            self._set_status_color("red")
        finally:
            self.busy.emit(False)

    def _check_color_only(self):
        """Calculates differences and checks if they are colored in Target."""
        self.busy.emit(True)
        try:
            key, included, _, _, tid, ttab, _, _ = self._get_run_params()
            
            # 1. Calculate differences (Data comparison)
            s_h, s_r = self._load_table("source")
            t_h, t_r = self._load_table("target")
            result = compare_two_sheets(s_h, s_r, t_h, t_r, key, included)
            
            # 2. Fetch current colors from Target
            current_formats = self.client.fetch_formats(tid, ttab)
            
            # 3. Compare actual colors vs expected colors
            # Passing 'included' columns so we ignore colors in other unrelated columns
            color_report = check_color_status(result, current_formats, t_h, included)
            
            # NEW: 4. If Source is Sheet, compare Source Colors vs Target Colors
            if self.src_type.currentText() == "Google Sheet":
                 sid, stab = self.src_id.text().strip(), self.src_list.currentText()
                 src_formats = self.client.fetch_formats(sid, stab)
                 # Compare src_formats vs current_formats (target)
                 color_diff_report = compare_sheet_colors(result, src_formats, current_formats, s_h, t_h, included)
                 if color_diff_report:
                     color_report.append("\n--- Source vs Target Color Mismatches ---")
                     color_report.extend(color_diff_report)

            # 4. Report
            self.report.append("\n=== Color Check Report ===")
            self.report.append("\n".join(color_report))
            self.report.append("==========================\n")
            
            QMessageBox.information(self, "Color Check Complete", "Check the report window for details.")
            self._save_ui_state()
            
        except Exception as e:
            QMessageBox.critical(self, "Error", str(e))
            self._set_status_color("red")
        finally:
            self.busy.emit(False)

    def _run(self, sync: bool):
        self.busy.emit(True)
        try:
            key, included, sid, stab, tid, ttab, is_source_sheet, update_marker_col = self._get_run_params()
            s_h, s_r = self._load_table("source")
            t_h, t_r = self._load_table("target")
            result = compare_two_sheets(s_h, s_r, t_h, t_r, key, included)
            self.report.append(result.to_report())

            if not result.differences:
                QMessageBox.information(self, "No Differences", "No data differences found to update.")
                self._save_ui_state()
                return

            base_color = BRIGHT_COLORS[self.base_color_combo.currentText()]
            value_updates = []
            src_value_updates = []
            log_entries = []

            tgt_sh = self.client._open_sheet(tid)
            tgt_ws = tgt_sh.worksheet(ttab)
            tgt_sheet_id = tgt_ws.id
            tgt_color_reqs = []
            tgt_header_map = {h: i for i, h in enumerate(t_h)}

            src_color_reqs = []
            src_header_map = {h: i for i, h in enumerate(s_h)}
            
            src_sh, src_ws, src_sheet_id = None, None, None
            if is_source_sheet:
                src_sh = self.client._open_sheet(sid)
                src_ws = src_sh.worksheet(stab)
                src_sheet_id = src_ws.id

            for key_val, diffs in result.differences.items():
                row_log_entries = []
                s_row_idx = diffs[0][3]
                t_row_idx = diffs[0][4]
                
                for h, s_val, t_val, _, _, s_col, t_col in diffs:
                    if sync:
                        value_updates.append((t_row_idx - 1, t_col, s_val))
                        row_log_entries.append(f"  - {h}: '{t_val}' -> '{s_val}'")

                    tgt_color_reqs.append(self._create_color_request(tgt_sheet_id, t_row_idx - 1, t_col, base_color))

                    if is_source_sheet:
                        src_color_reqs.append(self._create_color_request(src_sheet_id, s_row_idx - 1, s_col, base_color))
                
                # Handle Update Marker
                if update_marker_col:
                    marker_text = f"UPDATE {datetime.now().strftime('%y%m%d')}"
                    
                    # 1. Target Update
                    if update_marker_col in tgt_header_map:
                        marker_col_idx = tgt_header_map[update_marker_col]
                        marker_row_0 = t_row_idx - 1
                        
                        if sync:
                            value_updates.append((marker_row_0, marker_col_idx, marker_text))
                            row_log_entries.append(f"  - {update_marker_col} (Target): Set to '{marker_text}'")
                        
                        tgt_color_reqs.append(self._create_color_request(tgt_sheet_id, marker_row_0, marker_col_idx, base_color))

                    # 2. Source Update (Decoupled from target check)
                    if is_source_sheet and update_marker_col in src_header_map:
                        marker_col_idx = src_header_map[update_marker_col]
                        marker_row_0 = s_row_idx - 1
                        
                        if sync:
                            src_value_updates.append((marker_row_0, marker_col_idx, marker_text))
                            row_log_entries.append(f"  - {update_marker_col} (Source): Set to '{marker_text}'")
                            
                        src_color_reqs.append(self._create_color_request(src_sheet_id, marker_row_0, marker_col_idx, base_color))

                if row_log_entries:
                    log_entries.append(f"Row [{key_val}]:\n" + "\n".join(row_log_entries))

            if sync: 
                self.client.batch_update_values(tid, ttab, value_updates)
                if src_value_updates:
                    self.client.batch_update_values(sid, stab, src_value_updates)

            if tgt_color_reqs: self.client.batch_update(tid, {'requests': tgt_color_reqs})
            if src_color_reqs: self.client.batch_update(sid, {'requests': src_color_reqs})

            action = "Synced & Colored" if sync else "Colored"
            summary_message = f"{action} {len(result.differences)} rows with differences."
            QMessageBox.information(self, "Run Complete", summary_message)
            
            if sync and log_entries:
                log_header = f"Ran 'Sync' on {len(log_entries)} rows."
                self._write_log(f"{log_header}\n{'\n'.join(log_entries)}")
            else:
                 self._write_log(summary_message)
                 
            self._save_ui_state()
        except Exception as e:
            QMessageBox.critical(self, "Run Error", str(e))
            self._set_status_color("red")
        finally:
            self.busy.emit(False)

    def _clear_colors(self, kind: str):
        if not self._ensure_client(): return
        self.busy.emit(True)
        try:
            sid, stab = self.src_id.text().strip(), self.src_list.currentText()
            tid, ttab = self.tgt_id.text().strip(), self.tgt_list.currentText()

            if kind == "source":
                if self.src_type.currentText() != "Google Sheet":
                    raise ValueError("Cannot clear colors on a non-sheet source.")
                sheet_id, sheet_tab = sid, stab
            else:
                sheet_id, sheet_tab = tid, ttab

            if not sheet_id or not sheet_tab:
                raise ValueError(f"The {kind} sheet/tab is not specified.")
                
            sh = self.client._open_sheet(sheet_id)
            ws = sh.worksheet(sheet_tab)
            req = {"repeatCell": {"range": {"sheetId": ws.id, "startRowIndex": 1}, "cell": {"userEnteredFormat": {"backgroundColor": WHITE}}, "fields": "userEnteredFormat.backgroundColor"}}
            self.client.batch_update(sheet_id, {"requests": [req]})
            QMessageBox.information(self, "Colors Cleared", f"Background colors cleared in {kind} sheet '{sheet_tab}'.")
            self._write_log(f"Cleared all background colors in {kind} sheet.")
        except Exception as e:
            QMessageBox.critical(self, "Error", str(e))
            self._set_status_color("red")
        finally:
            self.busy.emit(False)

    def _create_color_request(self, sheet_id, row_idx, col_idx, color):
        return {"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": row_idx, "endRowIndex": row_idx + 1, "startColumnIndex": col_idx, "endColumnIndex": col_idx + 1},
                               "cell": {"userEnteredFormat": {"backgroundColor": color}}, "fields": "userEnteredFormat.backgroundColor"}}

    def _dim_colors(self, kind: str):
        if not self._ensure_client(): return
        self.busy.emit(True)
        try:
            sid, stab = self.src_id.text().strip(), self.src_list.currentText()
            tid, ttab = self.tgt_id.text().strip(), self.tgt_list.currentText()

            if kind == "source":
                if self.src_type.currentText() != "Google Sheet":
                    raise ValueError("Cannot dim colors on a non-sheet source.")
                sheet_id, sheet_tab = sid, stab
            else:
                sheet_id, sheet_tab = tid, ttab

            if not sheet_id or not sheet_tab:
                raise ValueError(f"The {kind} sheet/tab is not specified.")

            sh = self.client._open_sheet(sheet_id)
            ws = sh.worksheet(sheet_tab)
            worksheet_api_id = ws.id
            
            all_row_data = self.client.fetch_formats(sheet_id, sheet_tab)
            if not all_row_data:
                QMessageBox.information(self, "No Data", f"Could not retrieve format data from {kind}.")
                return

            color_requests = []
            for r_idx, row_data in enumerate(all_row_data, start=1):
                if 'values' not in row_data: continue
                for c_idx, cell_data in enumerate(row_data['values']):
                    if not cell_data or 'effectiveFormat' not in cell_data: continue
                    color = cell_data['effectiveFormat'].get('backgroundColor')
                    if not is_white(color):
                        r, g, b = color.get('red', 0.0), color.get('green', 0.0), color.get('blue', 0.0)
                        h, s, v = rgb_to_hsv(r, g, b)
                        new_s = max(0.0, s - DIM_BLEND_FACTOR)
                        if s > 0.85: new_v = v - 0.18
                        elif s > 0.45: new_v = v - 0.05
                        elif s > 0.15: new_v = v + 0.05
                        else: new_v = min(1.0, v + (0.15 - new_s) * 1.8)
                        new_v = max(0.0, min(1.0, new_v))
                        new_r, new_g, new_b = hsv_to_rgb(h, new_s, new_v)
                        new_color = {"red": new_r, "green": new_g, "blue": new_b}
                        if new_s < 0.01: new_color = WHITE
                        color_requests.append(self._create_color_request(worksheet_api_id, r_idx, c_idx, new_color))
            
            if not color_requests:
                QMessageBox.information(self, "No Colors", f"No colored cells found to dim in {kind} sheet.")
                return
                
            self.client.batch_update(sheet_id, {"requests": color_requests})
            QMessageBox.information(self, "Dim Complete", f"Successfully dimmed {len(color_requests)} cells.")
            self._write_log(f"Dimmed {len(color_requests)} colored cells in {kind} sheet.")

        except Exception as e:
            QMessageBox.critical(self, "Dim Error", str(e))
            self._set_status_color("red")
        finally:
            if self.progress.isVisible(): self.busy.emit(False)

    # --- State Management ---
    def _get_target_base_filename(self) -> Optional[str]:
        tid, ttab = self.tgt_id.text().strip(), self.tgt_list.currentText()
        if not tid or not ttab: return None
        tid_safe = re.sub(r'[\\/*?:"<>|]', "_", tid)
        ttab_safe = re.sub(r'[\\/*?:"<>|]', "_", ttab)
        return f"gspread__{tid_safe}__{ttab_safe}"
    
    def _on_target_change(self, current_text: str):
        if not current_text: return
        self._load_ui_state()
        
    def _load_all_data(self):
        if not self._ensure_client(): return
        self._load_src_tabs()
        self._load_tgt_tabs()
        self._populate_key_headers()
        
    def _save_ui_state(self):
        basename = self._get_target_base_filename()
        if not basename: return

        # Collect headers from list widget
        compare_headers = []
        for i in range(self.compare_list.count()):
            item = self.compare_list.item(i)
            if item.checkState() == Qt.Checked:
                compare_headers.append(item.text())

        state_file_path = os.path.join(CONFIG_PATH, f"{basename}.json")
        data = {
            "base_color_name": self.base_color_combo.currentText(),
            "compare_headers": compare_headers,
            "update_marker_col": self.update_marker_combo.currentText(),
            "ui_snapshot": {
                "src_type": self.src_type.currentText(),
                "src_id": self.src_id.text(),
                "src_tab": self.src_list.currentText(),
                "src_file": self.src_file.text(),
                "key_header": self.key_header.currentText(),
                "tgt_id": self.tgt_id.text(),
                "tgt_tab": self.tgt_list.currentText(),
            }
        }
        try:
            with open(state_file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2)
            
            last_session_data = {
                "last_credentials_path": self.cred_edit.text(),
                "last_target_config_basename": f"{basename}.json"
            }
            with open(LAST_SESSION_CONFIG, 'w', encoding='utf-8') as f:
                json.dump(last_session_data, f, indent=2)

        except Exception as e:
            print(f"Warning: Could not save UI state: {e}")

    def _load_ui_state(self, config_basename: Optional[str] = None):
        if not config_basename:
            basename = self._get_target_base_filename()
            if not basename: return
            config_basename = f"{basename}.json"

        state_file = os.path.join(CONFIG_PATH, config_basename)
        if not os.path.isfile(state_file): return

        try:
            with open(state_file, 'r', encoding='utf-8') as f:
                data = json.load(f)

            self.base_color_combo.setCurrentText(data.get("base_color_name", DEFAULT_COLOR_NAME))
            
            # Store these preferences to be applied when headers are loaded
            self.saved_compare_headers = data.get("compare_headers", DEFAULT_COMPARE_HEADERS)
            self.saved_marker_col = data.get("update_marker_col", "")

            snap = data.get("ui_snapshot", {})
            self.tgt_id.setText(snap.get("tgt_id", ""))
            self.src_id.setText(snap.get("src_id", ""))
            self.src_file.setText(snap.get("src_file", ""))
            self.src_type.setCurrentText(snap.get("src_type", "Google Sheet"))

            if saved_tgt_tab := snap.get("tgt_tab"): self.tgt_list.setCurrentText(saved_tgt_tab)
            if saved_src_tab := snap.get("src_tab"): self.src_list.setCurrentText(saved_src_tab)
            if saved_key_header := snap.get("key_header"): self.key_header.setCurrentText(saved_key_header)

            self.report.append(f"\nLoaded saved state from {os.path.basename(state_file)}")
            self._set_status_color("green")

        except Exception as e:
            QMessageBox.warning(self, "State Load Error", f"Could not load state from {state_file}: {e}")

    def _load_last_session_on_startup(self):
        if not os.path.isfile(LAST_SESSION_CONFIG): return
        try:
            with open(LAST_SESSION_CONFIG, 'r', encoding='utf-8') as f:
                data = json.load(f)
            if creds_path := data.get("last_credentials_path"): self.cred_edit.setText(creds_path)
            if data.get("last_target_config_basename"):
                self._load_ui_state(data["last_target_config_basename"])
                self._load_all_data()
            self.report.append("Loaded settings from last session.")
        except Exception as e:
            QMessageBox.warning(self, "Last Session Load Error", f"Could not load last session settings: {e}")

    def _write_log(self, message: str):
        basename = self._get_target_base_filename()
        if not basename: return
        log_file = os.path.join(LOG_PATH, f"{basename}.log")
        timestamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        src_info = ""
        if self.src_type.currentText() == "Google Sheet":
            src_info = f"Source: [Sheet] {self.src_id.text()}/{self.src_list.currentText()}"
        else:
            src_info = f"Source: [TSV] {os.path.basename(self.src_file.text())}"

        log_line = f"{timestamp} | {src_info}\n{message}\n"
        try:
            existing_content = ""
            if os.path.exists(log_file):
                with open(log_file, 'r', encoding='utf-8') as f: existing_content = f.read()
            with open(log_file, 'w', encoding='utf-8') as f:
                f.write(log_line)
                f.write("-" * 80 + "\n")
                f.write(existing_content)
        except Exception as e:
            print(f"Warning: Could not write to log file {log_file}: {e}")

    def _load_log_file(self):
        basename = self._get_target_base_filename()
        if not basename:
            QMessageBox.warning(self, "No Target", "Select a target sheet to load its log.")
            return
        log_file = os.path.join(LOG_PATH, f"{basename}.log")
        if not os.path.isfile(log_file):
            self.report.setPlainText("No log file found for this target.")
            return
        try:
            with open(log_file, 'r', encoding='utf-8') as f: self.report.setPlainText(f.read())
            self.report.insertPlainText(f"Log for {basename}:\n" + "="*40 + "\n")
        except Exception as e:
            QMessageBox.critical(self, "Log Error", f"Could not read log file: {e}")