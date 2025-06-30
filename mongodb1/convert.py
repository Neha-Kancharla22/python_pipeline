import json

# Define input and output file paths
input_path = r'C:\Users\Neha\Downloads\project.txt'
output_path = r'C:\Users\Neha\Documents\mongoo\project.json'

# Read from the .txt file and load JSON content
with open(input_path, 'r') as txt_file:
    data = json.load(txt_file)  # since it's valid JSON already

# Write to a .json file with indentation
with open(output_path, 'w') as json_file:
    json.dump(data, json_file, indent=4)

print("Conversion complete. JSON saved to:", output_path)
 