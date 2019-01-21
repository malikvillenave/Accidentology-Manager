from flask import Flask, request
from flask_restful import Resource, Api
import psycopg2
import math
from numpy import mean
import config
import time
from threading import Thread, Semaphore
import queue

app = Flask(__name__)

try:
    conn = psycopg2.connect(config.CONNECTION_STRING)
    cursor = conn.cursor()
except:
    print("Connection failed")

app.config['DEBUG'] = True

api = Api(app)
sem = Semaphore()

# rqt a modifier/utiliser plustard
# rqt = "SELECT indicateur " \
#         "FROM " \
#         "usager_accidente_par_vehicule as usg AND" \
#         "usg  LEFT OUTER JOIN ON type_vehicule ( usg.id_type_vehicule. = type_vehicule.id_typeVehicule) AND" \
#         "usg LEFT OUTER JOIN ON type_route ( usg.id_type_route. = type_route.id_typeRoute) AND" \
#         "usg LEFT OUTER JOIN ON meteo ( usg.id_meteo. = meteo.id_meteo)" \
#         "WHERE" \
#         "type_route.type_route = " + request.args.get('type_route')+ \
#         "AND usg.longitude < " + (waypoint['lon'] - interval)+ \
#         "AND usg.longitude > " + (waypoint['lon'] + interval)+ \
#         "AND usg.latitude < " + (waypoint['lat'] - interval)+ \
#         "AND usg.latitude > " + (waypoint['lat'] + interval)


def create_indicator_request(first_waypoint,second_waypoint):
    first_waypoint_coord = [round(float(x),7) for x in first_waypoint.split(",")]
    second_waypoint_coord = [round(float(x),7) for x in second_waypoint.split(",")]
    center_waypoint = [round((second_waypoint_coord[0]+first_waypoint_coord[0])/2,7), round((second_waypoint_coord[1]+first_waypoint_coord[1])/2,7)]
    rayon = round(math.sqrt((center_waypoint[0]-first_waypoint_coord[0])**2)+((center_waypoint[1]-first_waypoint_coord[1])**2),7)
    rqt = ("SELECT avg(indicateur) " 
        "FROM " 
        "usager_accidente_par_vehicule as usg " 
        "WHERE "
        +str(rayon) +" > |/((usg.longitude-("+str(center_waypoint[1])+"))^2+(+usg.latitude-("+str(center_waypoint[0])+"))^2)")
    return rqt


def processWaypointQueue(waypoint,waypoints,q,index):
    waypoint_interval = 100 #place temporairement ici
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
            start = time.time()

            res_queue = queue.Queue()

            json = request.json['response']
            if json is None:
                return {"post": []}, 405

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

            end = time.time()

            return {"response": json}
        except Exception as e:
            print(e)
            return {"response": {}}, 404

    def delete(self):
        return {"delete": "not implemented"}

    def put(self):
        return {"put": "not implemented"}


api.add_resource(ServiceIndicator, '/Indicator')
if __name__ == '__main__':
    app.run()
