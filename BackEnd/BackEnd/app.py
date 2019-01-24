from flask import Flask, request
from flask_restful import Resource, Api
import psycopg2
import math
from numpy import mean
import config
from threading import Thread, Semaphore
from werkzeug.serving import WSGIRequestHandler
import queue
import time

app = Flask(__name__)

try:
    conn = psycopg2.connect(config.CONNECTION_STRING)
    cursor = conn.cursor()
except:
    print("Connection failed")

app.config['DEBUG'] = True

api = Api(app)
sem = Semaphore()


def create_indicator_request(first_waypoint, second_waypoint):
    first_waypoint_coord = [round(float(x), 7) for x in first_waypoint.split(",")]
    second_waypoint_coord = [round(float(x), 7) for x in second_waypoint.split(",")]
    center_waypoint = [round((second_waypoint_coord[0]+first_waypoint_coord[0])/2, 7), round((second_waypoint_coord[1]+first_waypoint_coord[1])/2, 7)]
    rayon = round(math.sqrt((center_waypoint[0]-first_waypoint_coord[0])**2)+((center_waypoint[1]-first_waypoint_coord[1])**2), 7)
    rqt = ('SELECT avg(indicateur) '
           'FROM '
           'usager_accidente_par_vehicule as usg '
           'WHERE '
           + str(rayon) +' > |/((usg.longitude-(' + str(center_waypoint[1]) +'))^2+(+usg.latitude-(' + str(center_waypoint[0]) +'))^2)')
    return rqt


def processWaypointQueue(waypoint, waypoints, q, index):
    waypoint_interval = 10 #place temporairement ici
    rqt = create_indicator_request(waypoint, waypoints[index + waypoint_interval])
    danger_level = 0
    sem.acquire()
    cursor.execute(rqt)
    for record in cursor:
        if record[0]:
            danger_level = record[0]
    sem.release()
    q.put(danger_level)


class ServiceIndicator(Resource):
    def get(self):
        return {"get": "not implemented"}

    def post(self):
        try:
            res_queue = queue.Queue()

            json = request.json['response']
            if json is None:
                return {"response": "JSon not found"}, 404

            waypoint_interval = 100

            for indexRoute, route in enumerate(request.json['response']['route']):
                waypoints = route['shape']
                moy_indicator = []
                threads = []

                for index, waypoint in enumerate(waypoints):
                    if index > len(waypoints) - waypoint_interval:
                        break

                    if index % waypoint_interval == 0:
                        t = Thread(target=processWaypointQueue, args=(waypoint, waypoints, res_queue, index))
                        t.start()
                        threads.append(t)

                for t in threads:
                    t.join()

                while not res_queue.empty():
                    result = res_queue.get()
                    moy_indicator.append(result)

                json['route'][indexRoute]['dangerLevel'] = mean(moy_indicator)

            return {"response": json}
        except Exception as e:
            print(e)
            return {"response": "An internal error occurred"}, 404

    def delete(self):
        return {"delete": "not implemented"}

    def put(self):
        return {"put": "not implemented"}


class ServiceIndicatorLight(Resource):
    def get(self):
        try:
            return {"get": "not implemented"}
        except Exception as e:
            print(e)
            return {"response": {}}, 404

    def post(self):
        try:
            json = request.json

            if json is None:
                return {"post": []}, 404

            response = []
            for route in json['routes']:
                waypoints = route['waypoints']
                moy_indicator = []
                for indexWaypoint, waypoint in enumerate(waypoints):
                    if indexWaypoint >= (len(waypoints)-1):
                        break
                    rqt = create_indicator_request(waypoint, waypoints[indexWaypoint + 1])
                    cursor.execute(rqt)
                    for record in cursor:
                        if record[0]:
                            moy_indicator.append(record[0])
                if (not moy_indicator):
                    moy_indicator = [1]

                response.append({
                    'id': route['id'],
                    'dangerLevel': mean(moy_indicator)
                })

            if response is None:
                return {"post": []}, 404

            return {"response": response}

        except Exception as e:
            print(e)
            return {"response": {}}, 404

    def delete(self):
        return {"delete": "example"}

    def put(self):
        return {"put": "example"}


api.add_resource(ServiceIndicator, '/Indicator')

api.add_resource(ServiceIndicatorLight, '/IndicatorLight')

if __name__ == '__main__':
    WSGIRequestHandler.protocol_version = "HTTP/1.1"
    app.run()
