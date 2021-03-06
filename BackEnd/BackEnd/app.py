from flask import Flask, request
from flask_restful import Resource, Api
import psycopg2
import math
import config
from threading import Thread, Semaphore
from werkzeug.serving import WSGIRequestHandler
import queue
import time

app = Flask(__name__)

app.config['DEBUG'] = True

api = Api(app)
sem = Semaphore()


def create_indicator_paramGrouped_request(first_waypoint, second_waypoint, hrmn, param):
    first_waypoint_coord = [round(float(x), 7) for x in first_waypoint.split(",")]
    second_waypoint_coord = [round(float(x), 7) for x in second_waypoint.split(",")]
    center_waypoint = [round((second_waypoint_coord[0] + first_waypoint_coord[0]) / 2, 7),
                       round((second_waypoint_coord[1] + first_waypoint_coord[1]) / 2, 7)]
    rayon = round(math.sqrt((center_waypoint[0] - first_waypoint_coord[0]) ** 2) + (
                (center_waypoint[1] - first_waypoint_coord[1]) ** 2), 7)
    if param == "lt1":
        rqt = ('SELECT sum(indicateur), count(*) '
               'FROM '
               'usager_accidente_par_vehicule as usg '
               'WHERE '
               + str(rayon) + ' > |/((usg.longitude-(' + str(center_waypoint[1]) + '))^2+(+usg.latitude-(' + str(
                    center_waypoint[0]) + '))^2) AND'
                                          ' id_heure IN ('
                                          ' SELECT id_heure'
                                          ' FROM public."Heure"'
                                          ' WHERE abs((heure*60+minute) - (' + str(
                    hrmn) + ')) < 90 OR abs((heure*60+minute) - (' + str(hrmn) + ')) > 1350)')
    elif param == "gte1":
        rqt = ('SELECT sum(indicateur), count(*) '
               'FROM '
               'usager_accidente_par_vehicule as usg '
               'WHERE '
               + str(rayon) + ' > |/((usg.longitude-(' + str(center_waypoint[1]) + '))^2+(+usg.latitude-(' + str(
                    center_waypoint[0]) + '))^2) AND'
                                          ' id_heure IN ('
                                          ' SELECT id_heure'
                                          ' FROM public."Heure"'
                                          ' WHERE abs((heure*60+minute) - ('+str(hrmn)+')) >= 90 OR abs((heure*60+minute) - ('+str(hrmn)+')) <= 1350)')
    else:
        rqt = ('SELECT sum(indicateur), count(*) '
               'FROM '
               'usager_accidente_par_vehicule as usg '
               'WHERE '
               + str(rayon) + ' > |/((usg.longitude-(' + str(center_waypoint[1]) + '))^2+(+usg.latitude-(' + str(
                    center_waypoint[0]) + '))^2)')
    return rqt


param_weights = {
    "lt1":1,
    "gte1":0.5
}


def processWaypointHourGroupedQueue(waypoint, waypoints, waypoint_interval, q, index, hrmn, param, w):
    rqt = create_indicator_paramGrouped_request(waypoint, waypoints[index + waypoint_interval], hrmn, param)
    ind_sum = 0
    weight_sum = 0
    acc_count = 0

    try:
        conn = psycopg2.connect(config.CONNECTION_STRING)
        cursor = conn.cursor()
    except:
        print("Connection to database failed")

    cursor.execute(rqt)
    for record in cursor:
        if record[0]:
            ind_sum = record[0] * w
            weight_sum = record[1] * w
            acc_count = record[1]

    q.put((ind_sum, weight_sum, acc_count))

#erviceIndicatorHourGroupedPara
class ServiceIndicator(Resource):
    def get(self):
        return {"get": "example"}

    def post(self):
        try:
            json = request.json

            if json is None:
                return {"post": "JSon file not found"}, 404

            res_queue = queue.Queue()

            hr = json['heure']
            mn = json['minute']
            hrmn = hr * 60 + mn
            waypoint_interval = 10
            start = time.time()
            response = []
            
            for route in json['routes']:

                waypoints = route['waypoints']
                threads = []
                ind_sum = 0
                weight_sum = 0
                acc_count = 0

                for indexWaypoint, waypoint in enumerate(waypoints):
                    if indexWaypoint >= len(waypoints) - waypoint_interval:
                        break

                    if indexWaypoint % waypoint_interval == 0:
                        for param, w in param_weights.items():
                            t = Thread(target=processWaypointHourGroupedQueue, args=(waypoint, waypoints, waypoint_interval, res_queue, indexWaypoint, hrmn, param, w))
                            t.start()
                            threads.append(t)

                for t in threads:
                    t.join()

                while not res_queue.empty():
                    result = res_queue.get()
                    ind_sum = ind_sum + result[0]
                    weight_sum = weight_sum + result[1]
                    acc_count = acc_count + result[2]

                if acc_count == 0:
                    response.append({
                        'id': route['id'],
                        'dangerLevel': 1
                    })
                else:
                    moy_ind = (ind_sum*1.0) / (weight_sum*1.0)
                    # Formule (1/(1+(2/n)))*IND
                    # n = nombre d'accidents
                    response.append({
                        'id': route['id'],
                        'dangerLevel': (1/(1+(4/acc_count)))*moy_ind
                    })
            end = time.time()
            print(end-start)
            if json is None:
                return {"post": "The response JSon could not be created"}, 404

            return {"response": response}
        except Exception as e:
            print(e)
            return {"response": "An error occurred"}, 404

    def delete(self):
        return {"delete": "example"}

    def put(self):
        return {"put": "example"}


api.add_resource(ServiceIndicator, '/Indicator')

if __name__ == '__main__':
    WSGIRequestHandler.protocol_version = "HTTP/1.1"
    app.run()
