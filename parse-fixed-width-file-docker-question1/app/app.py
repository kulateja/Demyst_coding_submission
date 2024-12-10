from flask import Flask
import dataprocess
from generate_fwf import generate_fixed_width_file_from_spec
from parse_fwf import parse_fixed_width_file

app = Flask(__name__)

@app.route('/')
def hello_world():
    return 'Hello, Docker!'

# Generate fixed-width file
generate_fixed_width_file_from_spec("spec.json")

# Parse fixed-width file to CSV
parse_fixed_width_file("spec.json")

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)