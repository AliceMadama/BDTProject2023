import json

import requests


# class DirectionsAPI:
#     def __init__(self):
#         self.base_url = 'http://localhost:8080/ors/v2/directions/driving-car'
#         self.api_key = 'YOUR_API_KEY'
#         self.headers = {
#             'Accept': 'application/json, application/geo+json, application/gpx+xml, img/png; charset=utf-8',
#             'Content-Type': 'application/json; charset=utf-8'
#         }
#
#     def get_directions(self, route_request):
#         body = {
#             "coordinates": [route_request["src"], route_request["dst"]],
#             "alternative_routes": {"target_count": 3}  # target count control how many routes it returns
#         }
#
#         call = requests.post(f'{self.base_url}', json=body, headers=self.headers)
#         return call.text
#         # return json_formatted_str

class DirectionsAPI:
    def __init__(self):
        self.base_url = 'http://localhost:8080/ors/v2/directions/driving-car//geojson'
        self.api_key = 'YOUR_API_KEY'
        self.headers = {
    'Accept': 'application/json, application/geo+json, application/gpx+xml, img/png; charset=utf-8',
    'Authorization': '5b3ce3597851110001cf6248f61f97b9c8934a1a80a0c1abefca6064',
    'Content-Type': 'application/json; charset=utf-8'
}

    def get_directions(self, route_request):
        body = {
            "coordinates": [route_request["src"], route_request["dst"]],
            "alternative_routes": {"target_count": 3}  # target count control how many routes it returns
        }

        call = requests.post(f'{self.base_url}', json=body, headers=self.headers)
        return call.json()
        # return json_formatted_str




if __name__ == "__main__":
    route_request = {
        'src': [-73.84335071056566, 40.82601606504347],
        'dst': [-74.0060, 40.7128],
    }
    print(route_request)

    directions_api = DirectionsAPI()
    result = directions_api.get_directions(route_request)
    print(result)
    print("123")

