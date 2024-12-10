import csv
import json

def parse_fixed_width_file(spec_file):
    """
    Parses a fixed-width file based on the provided spec.json and converts it into a delimited file (CSV).

    Parameters:
        spec_file (str): Path to the JSON file containing the specification.
    """
    # Load spec.json
    with open(spec_file, "r") as f:
        spec = json.load(f)

    # Extract settings from the spec
    column_names = spec["ColumnNames"]
    offsets = list(map(int, spec["Offsets"]))
    input_file = spec["FixedWidthFileName"]
    output_file = spec["DelimitedFileName"]
    input_encoding = spec["FixedWidthEncoding"]
    output_encoding = spec["DelimitedEncoding"]

    # Open the input and output files
    with open(input_file, "r", encoding=input_encoding) as fwf, open(output_file, "w", newline="", encoding=output_encoding) as csvfile:
        writer = csv.writer(csvfile)

        # Write the header only if IncludeHeader is True
        if spec.get("IncludeHeader", "False").lower() == "true":
            writer.writerow(column_names)

        # Read each line of the fixed-width file
        for line_number, line in enumerate(fwf):
            # Skip the first line if it's a duplicate header in the input file
            if line_number == 0 and spec.get("IncludeHeader", "False").lower() == "true":
                continue
            
            start = 0
            row = []
            for offset in offsets:
                # Extract the field using the offset
                field = line[start:start + offset].strip()
                row.append(field)
                start += offset
            # Write the row to the CSV
            writer.writerow(row)
