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

try:
    conn = psycopg2.connect(config.CONNECTION_STRING)
    cursor = conn.cursor()
except:
    print("Connection failed")

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
                    hrmn) + ')) < 60 OR abs((heure*60+minute) - (' + str(hrmn) + ')) > 1380)')
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
                                          ' WHERE abs((heure*60+minute) - ('+str(hrmn)+')) >= 60 OR abs((heure*60+minute) - ('+str(hrmn)+')) <= 1380)')
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


def processWaypointHourGroupedQueue(waypoint, waypoints, q, index, hrmn, param, w):
    waypoint_interval = 10 #place temporairement ici
    rqt = create_indicator_paramGrouped_request(waypoint, waypoints[index + waypoint_interval], hrmn, param)
    ind_sum = 0
    weight_sum = 0
    acc_count = 0
    sem.acquire()
    cursor.execute(rqt)
    for record in cursor:
        if record[0]:
            ind_sum = record[0] * w
            weight_sum = record[1] * w
            acc_count = record[1]
    sem.release()
    q.put((ind_sum, weight_sum, acc_count))

#erviceIndicatorHourGroupedPara
class ServiceIndicator(Resource):
    def get(self):
        return {"get": "example"}

    def post(self):
        try:
            res_queue = queue.Queue()

            json = request.json
            hr = json['heure']
            mn = json['min']
            hrmn = hr * 60 + mn
            waypoint_interval = 10
            start = time.time()
            for route in json['routes']:

                waypoints = route['waypoints']
                threads=[]
                ind_sum = 0
                weight_sum = 0
                acc_count = 0

                for index, waypoint in enumerate(waypoints):
                    if index > len(waypoints) - waypoint_interval:
                        break

                    if index % waypoint_interval == 0:
                        for param, w in param_weights.items():
                            t = Thread(target=processWaypointHourGroupedQueue, args=(waypoint, waypoints, res_queue, index, hrmn, param, w))
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
                    route['dangerLevel'] = 1
                else:
                    moy_ind = (ind_sum*1.0) / (weight_sum*1.0)
                    route['dangerLevel'] = (1/(1+(4/acc_count)))*moy_ind #utilisation de la formule (1/(1+(2/n)))*IND   avec    n le nombre d'accidents
            end = time.time()
            print(end-start)
            if json is None:
                return {"post": []}, 405

            return {"response": json}
        except Exception as e:
            print(e)
            return {"response": {}}, 404

    def delete(self):
        return {"delete": "example"}

    def put(self):
        return {"put": "example"}


api.add_resource(ServiceIndicator, '/Indicator')


if __name__ == '__main__':
    WSGIRequestHandler.protocol_version = "HTTP/1.1"
    app.run()
