import requests
from bs4 import BeautifulSoup
import json

# params = {"limit" : "100"}
# URL = "https://catfact.ninja/breeds"
# get = requests.get(URL, params)

str_json = ""
with open("6\\breeds.json") as file:
    lines = file.readlines()
    for line in lines:
        str_json += line
    # file.write(json.dumps(get.json()))

data = json.loads(str_json)
data = data['data']

soup = BeautifulSoup("""<table>
    <tr>
        <th>breed</th>
        <th>country</th>
        <th>origin</th>
        <th>coat</th>
        <th>pattern</th>               
    </tr>
</table>""", features="html.parser")

table = soup.contents[0]

for tick in data:
    tr = soup.new_tag("tr")
    for key, val in tick.items():
        td = soup.new_tag("td")
        td.string = str(val)
        tr.append(td)
    table.append(tr)

with open ("task_6.html", "w",encoding="utf-8") as result:
    result.write(soup.prettify())
    result.write("\n")