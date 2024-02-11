import requests
import json


data = {"queries" : ["COVID-19 coronaviruses", "Emergency Care", "fever Infectious" ],
                     "random_command" : "insert"}

ip = '34.125.201.35'
port = '9999'

URL = "http://"+ip+":"+port+"/execute_query"
response = requests.post(URL,json=data)
print(response.content)