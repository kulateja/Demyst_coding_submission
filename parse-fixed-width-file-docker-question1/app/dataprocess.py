from generate_fwf import generate_fixed_width_file_from_spec
from parse_fwf import parse_fixed_width_file

# Generate fixed-width file
generate_fixed_width_file_from_spec("spec.json")

# Parse fixed-width file to CSV
parse_fixed_width_file("spec.json")
