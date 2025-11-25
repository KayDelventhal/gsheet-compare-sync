#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
from PySide6.QtWidgets import QApplication

# --- CRITICAL FIX: These lines must be BEFORE 'from src.logic import ...' ---
# This adds the parent directory (project root) to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.ui import CompareSyncUI

def main():
    app = QApplication(sys.argv)
    w = CompareSyncUI()
    w.resize(900, 800)
    w.show()
    sys.exit(app.exec())

if __name__ == "__main__":
    main()