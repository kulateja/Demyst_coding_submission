import os
import csv
import json
from parse_fwf import parse_fixed_width_file  # Import your parser function

def test_basic_functionality():
    # Mock spec file for the test
    spec = {
        "ColumnNames": ["Name", "Age", "City"],
        "Offsets": [10, 5, 15],
        "FixedWidthFileName": "test_fixed_width.fwf",
        "DelimitedFileName": "test_output.csv",
        "FixedWidthEncoding": "windows-1252",
        "DelimitedEncoding": "utf-8",
        "IncludeHeader": "True"
    }

    # Write the spec to a JSON file
    spec_file = "test_spec.json"
    with open(spec_file, "w") as f:
        json.dump(spec, f)

    # Create a mock fixed-width file
    with open(spec["FixedWidthFileName"], "w", encoding=spec["FixedWidthEncoding"]) as f:
        f.write("Name      Age  City           \n")  # Header row
        f.write("Teja      30   New York       \n")
        f.write("Sai       25   Los Angeles    \n")
        f.write("Charlie   40   San Francisco  \n")

    # Parse the fixed-width file
    parse_fixed_width_file(spec_file)

    # Validate the CSV output
    with open(spec["DelimitedFileName"], "r", encoding=spec["DelimitedEncoding"]) as csvfile:
        reader = csv.reader(csvfile)
        rows = list(reader)

    # Expected output
    expected_output = [
        ["Name", "Age", "City"],  # Header row
        ["Teja", "30", "New York"],
        ["Sai", "25", "Los Angeles"],
        ["Charlie", "40", "San Francisco"]
    ]

    assert rows == expected_output, f"Basic Functionality - CSV output did not match expected output.\nExpected: {expected_output}\nGot: {rows}"
    print("Test Passed: Basic Functionality")
    # Cleanup
    os.remove(spec_file)
    os.remove(spec["FixedWidthFileName"])
    os.remove(spec["DelimitedFileName"])

# Run the test
test_basic_functionality()



def test_encoding_handling():
    # Mock spec with encoding variations
    spec = {
        "ColumnNames": ["Name", "Age", "City"],
        "Offsets": [10, 5, 15],
        "FixedWidthFileName": "test_fixed_width_encoding.fwf",
        "DelimitedFileName": "test_output_encoding.csv",
        "FixedWidthEncoding": "windows-1252",
        "DelimitedEncoding": "utf-8",
        "IncludeHeader": "true"
    }

    # Write the spec to a JSON file
    spec_file = "test_encoding_spec.json"
    with open(spec_file, "w") as f:
        json.dump(spec, f)

    # Create a mock fixed-width file with UTF-16 encoding
    with open(spec["FixedWidthFileName"], "w", encoding=spec["FixedWidthEncoding"]) as f:
        f.write("Name      Age  City           \n")  # Header row
        f.write("Teja      30   New York       \n")
        f.write("Sai       25   Los Angeles    \n")
        f.write("Charlie   40   San Francisco  \n")

    # Parse the fixed-width file
    parse_fixed_width_file(spec_file)

    # Validate the CSV output
    with open(spec["DelimitedFileName"], "r", encoding=spec["DelimitedEncoding"]) as csvfile:
        reader = csv.reader(csvfile)
        rows = list(reader)

    # Expected output
    expected_output = [
        ["Name", "Age", "City"],  # Header row
        ["Teja", "30", "New York"],
        ["Sai", "25", "Los Angeles"],
        ["Charlie", "40", "San Francisco"]
    ]

    assert rows == expected_output, f"Encoding Handling -- CSV output did not match expected output.\nExpected: {expected_output}\nGot: {rows}"

    print("Test Passed: Encoding Handling")

    # Cleanup
    os.remove(spec_file)
    os.remove(spec["FixedWidthFileName"])
    os.remove(spec["DelimitedFileName"])

# Run the test
test_encoding_handling()




def test_file_length_consistency_negative():
    # Provided spec details
    spec = {
        "ColumnNames": [
            "f1", "f2", "f3", "f4", "f5",
            "f6", "f7", "f8", "f9", "f10"
        ],
        "Offsets": [
            5, 12, 3, 2, 13,
            7, 10, 13, 20, 13
        ],
        "FixedWidthEncoding": "windows-1252",
        "IncludeHeader": "True",
        "DelimitedEncoding": "utf-8"
    }

    total_length = sum(spec["Offsets"])

    # Create an invalid fixed-width file
    invalid_file = "test_fixed_width_negative.fwf"
    with open(invalid_file, "w", encoding=spec["FixedWidthEncoding"]) as f:
        # Write invalid rows
        f.write("C123      Invalid   303    EF Charles          7890123        \n")  # Too short
        f.write("D4567     Another   404    GH David            1234567890   \n")  # Too long

    # Validate file length for invalid rows
    with open(invalid_file, "r", encoding=spec["FixedWidthEncoding"]) as f:
        invalid_lines = f.readlines()

    invalid_passed = True
    for line in invalid_lines:
        if len(line.strip()) != total_length:
            invalid_passed = False
            print(f"Negative Test Passed: Found invalid row length: {len(line.strip())}")
            break

    assert not invalid_passed, "Negative Test Failed: All rows were incorrectly valid."

    # Cleanup
    os.remove(invalid_file)

# Run the test
test_file_length_consistency_negative()



def test_file_length_consistency_positive():
    spec = {
        "ColumnNames": [
            "f1", "f2", "f3", "f4", "f5",
            "f6", "f7", "f8", "f9", "f10"
        ],
        "Offsets": [5, 12, 3, 2, 13, 7, 10, 13, 20, 13],
        "FixedWidthEncoding": "windows-1252",
        "IncludeHeader": "True"
    }

    total_length = sum(spec["Offsets"])
    fixed_width_file = "test_fixed_width_positive.fwf"

    # Correctly format rows
    row1 = [
        "A1234", "Data", "101", "AB", "Alice",
        "1234567", "John Doe", "Los Angeles", "Active", ""
    ]
    row2 = [
        "B2345", "MoreData", "202", "CD", "Bob",
        "7654321", "Jane Smith", "San Francisco", "Inactive", ""
    ]

    with open(fixed_width_file, "w", encoding=spec["FixedWidthEncoding"]) as f:
        # Write header
        if spec["IncludeHeader"].lower() == "true":
            f.write("".join([col.ljust(offset) for col, offset in zip(spec["ColumnNames"], spec["Offsets"])]) + "\n")
        # Write rows
        f.write("".join([field.ljust(offset) for field, offset in zip(row1, spec["Offsets"])]) + "\n")
        f.write("".join([field.ljust(offset) for field, offset in zip(row2, spec["Offsets"])]) + "\n")

    with open(fixed_width_file, "r", encoding=spec["FixedWidthEncoding"]) as f:
        lines = f.readlines()

    data_rows = lines[1:] if spec["IncludeHeader"].lower() == "true" else lines

    for line in data_rows:
        stripped_line = line.strip('\n').strip('\r')
        stripped_length = len(stripped_line)
        print(f"Row Data: '{stripped_line}' (Length: {stripped_length})")  # Debugging Output
        assert stripped_length == total_length, \
            f"Row length mismatch: Expected {total_length}, Got {stripped_length}"

    print("Test Passed: File Length Consistency (Positive Case)")

    os.remove(fixed_width_file)

# Run the test
test_file_length_consistency_positive()

