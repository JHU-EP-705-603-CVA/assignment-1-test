import sys
import os
import subprocess
import unittest
import site

# Step 1: Install requirements using the correct Python executable
# ✅ Step 2: Make sure user-installed packages are visible
sys.path.append(site.getusersitepackages())

# Step 3: Add the test directory to the import path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../"))
# sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../securebank"))
# sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../securebank/data_sources"))

# -----------------------------
# Step 3: Run a Specific Unit Test
# -----------------------------
def run_test(test_name):
    # Import here to allow setup to complete first
    from unit_test import TestRawDataHandler

    suite = unittest.TestSuite()
    suite.addTest(TestRawDataHandler(test_name))

    runner = unittest.TextTestRunner(verbosity=0)
    result = runner.run(suite)

    if result.wasSuccessful():
        print("PASS")
        exit(0)
    else:
        print("FAIL")
        exit(1)

# -----------------------------
# Main Execution
# -----------------------------
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python grade_runner.py <test_method_name>")
        exit(1)

    test_method = sys.argv[1]

    try:
        run_test(test_method)
    except Exception as e:
        print(f"FAIL (exception: {e})")
        exit(1)
