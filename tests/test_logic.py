import os
import sys
import unittest
#import datetime as dt

# --- CRITICAL FIX: These lines must be BEFORE 'from src.logic import ...' ---
# This adds the parent directory (project root) to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.logic import normalize_cell, compare_two_sheets, CompareResult

class TestLogic(unittest.TestCase):
    def test_normalize_cell_floats(self):
        self.assertEqual(normalize_cell(" 123.45 "), 123.45)
        self.assertEqual(normalize_cell("1,234.50"), 1234.5)
        self.assertEqual(normalize_cell(10), 10.0)
        
    def test_normalize_cell_dates(self):
        # Assuming ISO format output
        self.assertEqual(normalize_cell("2023-12-25"), "2023-12-25")
        self.assertEqual(normalize_cell("25/12/2023"), "2023-12-25")
        
    def test_compare_two_sheets_basic(self):
        s_h = ["ID", "Value", "Ignored"]
        s_r = [
            ["1", "Apple", "X"],
            ["2", "Banana", "X"],
        ]
        t_h = ["ID", "Value"]
        t_r = [
            ["1", "Apple"],
            ["2", "Cherry"], # Difference here
        ]
        
        res = compare_two_sheets(s_h, s_r, t_h, t_r, "ID", ["Value"])
        
        self.assertTrue("1" not in res.differences)
        self.assertTrue("2" in res.differences)
        
        # Check diff content: header, s_val, t_val
        diff = res.differences["2"][0]
        self.assertEqual(diff[0], "Value")
        self.assertEqual(diff[1], "Banana")
        self.assertEqual(diff[2], "Cherry")

if __name__ == '__main__':
    unittest.main()