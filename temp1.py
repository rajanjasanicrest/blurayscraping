import json

master_data = []
for year in range(1996, 2025):
    print(year)
    try:
        with open(f'data/DVD-{year}.json', 'r', encoding='utf-8') as file:
            data = json.load(file)
            master_data.extend(data)
    except FileNotFoundError:
        print(f"File for year {year} not found. Skipping.")

with open('data/DVD-us.json', 'w', encoding='utf-8') as outfile:
    json.dump(master_data, outfile, indent=4)