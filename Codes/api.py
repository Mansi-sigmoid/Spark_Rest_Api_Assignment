import json
import time
import requests
import csv
from stock_list import get_stock

url = "https://stock-market-data.p.rapidapi.com/stock/historical-prices"
count = 0
for stock in get_stock():
    if count < 25:
        querystring = {"ticker_symbol":stock,"years":"1","format":"json"}
        headers = {
            "X-RapidAPI-Key": "6274f3e13cmsha095150f3be4140p1493a8jsn9dd41ca2f031",
            "X-RapidAPI-Host": "stock-market-data.p.rapidapi.com"
        }
        response = requests.request("GET", url, headers=headers, params=querystring)

        headers = ["Open", "High", "Low", "Close", "Adj_close", "Volume", "Date", "Stock_name"]
        json_response = json.loads(response.text)
        data_file = open(f'Data/{stock}.csv', 'w')
        csv_writer = csv.writer(data_file)
        rows = [headers]
        for data in json_response['historical prices']:
            temp = []
            temp = list(data.values())
            temp.append(stock)
            rows.append(temp)
            time.sleep(8)
        csv_writer.writerows(rows)

        data_file.close()
        count = count + 1
