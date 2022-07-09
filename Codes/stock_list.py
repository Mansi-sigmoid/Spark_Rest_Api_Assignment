import json
import requests

url = "https://stock-market-data.p.rapidapi.com/market/index/s-and-p-six-hundred"

headers = {
	"X-RapidAPI-Key": "6274f3e13cmsha095150f3be4140p1493a8jsn9dd41ca2f031",
	"X-RapidAPI-Host": "stock-market-data.p.rapidapi.com"
}

response = requests.request("GET", url, headers=headers)

json_response = json.loads(response.text)

print(response.text)
print(json_response)
def get_stock():
	total_stocks=json_response['stocks']
	stocks=total_stocks[0:100]
	return stocks
	# print(stocks)
	# print(type(json_response))


print(response.text)