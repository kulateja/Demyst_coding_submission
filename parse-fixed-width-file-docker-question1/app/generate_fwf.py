import json
import random
import string

def generate_fixed_width_file_from_spec(spec_file):
    # Load spec.json
    with open(spec_file, "r") as f:
        spec = json.load(f)

    column_names = spec["ColumnNames"]
    offsets = list(map(int, spec["Offsets"]))
    output_file = spec["FixedWidthFileName"]
    encoding = spec["FixedWidthEncoding"]

    # Generate file
    with open(output_file, "w", encoding=encoding) as f:
        if spec.get("IncludeHeader", "False").lower() == "true":
            header = "".join([col.ljust(offset) for col, offset in zip(column_names, offsets)])
            f.write(header + "\n")

        for _ in range(100):  # Adjust row count as needed
            row = "".join(
                [
                    "".join(random.choices(string.ascii_letters + string.digits, k=offset)).ljust(offset)
                    for offset in offsets
                ]
            )
            f.write(row + "\n")

# Usage
generate_fixed_width_file_from_spec("spec.json")
