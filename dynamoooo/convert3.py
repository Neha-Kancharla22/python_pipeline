import json
 
# Simply read and parse the full JSON array directly
with open(r"C:\Users\Neha\Downloads\project.txt", 'r') as text_file:
    data = json.load(text_file)
 
with open('proj.json', 'w') as json_file:
    json.dump(data, json_file, indent=4)
 
print(" All records converted successfully!")
 
print("conversion complete")