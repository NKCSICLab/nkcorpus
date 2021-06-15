import json
file = open("../2021-02-03/filtered_clean/31/crawl-data/CC-MAIN-2021-10/segments/1614178347293.1/wet/CC-MAIN-20210224165708-20210224195708-00001.warc.wet.json","r")
data = json.load(file)
for i in data.items():
    print(i)
    input()