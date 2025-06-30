import json
#Read the file
with open(r"C:\Users\Neha\Downloads\Doc_unstructured_1.txt", "r") as file:
    data = json.load(file)

for project in data:
    print(f"Project ID: {project['project_id']}")
    print(f"Project Name: {project['project_name']}")
    print(f"Client Name: {project['client']['name']}")
    print("Technologies Used:", ", ".join(project['technologies']))
    print("Team Members:")
    for member in project['team']['members']:
        print(f" - {member['name']} ({member['role']})")
    print("-" * 40)
with open("structured_output.json", "w") as outfile:
    json.dump(data, outfile, indent=4)
