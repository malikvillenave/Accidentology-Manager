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

            json = request.json
            waypoint_interval = 10

            for id in json:

                waypoints = request.json[id]['waypoints']
                moy_indicator = []

                for index, waypoint in enumerate(waypoints):
                    if index > len(waypoints) - waypoint_interval:
                        break

                    if index % waypoint_interval == 0:
                        rqt = create_indicator_request(waypoint, waypoints[index + waypoint_interval])
                        cursor.execute(rqt)
                        for record in cursor:
                            if record[0]:
                                moy_indicator.append(record[0])
                if (not moy_indicator):
                    moy_indicator = [1]
                request.json[id]['dangerLevel'] = mean(moy_indicator)

            if json is None:
                return {"post": []}, 405

            return {"response": json}
        except Exception as e:
            print(e)
            return {"response": {}}, 404

    def post(self):
        try:

            json = request.json
            waypoint_interval = 10

            for route in json:

                waypoints = route['waypoints']
                moy_indicator = []

                for index, waypoint in enumerate(waypoints):
                    if index > len(waypoints) - waypoint_interval:
                        break

                    if index % waypoint_interval == 0:
                        rqt = create_indicator_request(waypoint, waypoints[index + waypoint_interval])
                        cursor.execute(rqt)
                        for record in cursor:
                            if record[0]:
                                moy_indicator.append(record[0])
                if (not moy_indicator):
                    moy_indicator = [1]
                route['dangerLevel'] = mean(moy_indicator)

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


def create_indicator_param_request(first_waypoint, second_waypoint, hrmn, param):
    first_waypoint_coord = [round(float(x), 7) for x in first_waypoint.split(",")]
    second_waypoint_coord = [round(float(x), 7) for x in second_waypoint.split(",")]
    center_waypoint = [round((second_waypoint_coord[0] + first_waypoint_coord[0]) / 2, 7),
                       round((second_waypoint_coord[1] + first_waypoint_coord[1]) / 2, 7)]
    rayon = round(math.sqrt((center_waypoint[0] - first_waypoint_coord[0]) ** 2) + (
                (center_waypoint[1] - first_waypoint_coord[1]) ** 2), 7)
    if param == "lt1":
        rqt = ('SELECT indicateur '
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
        rqt = ('SELECT indicateur '
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
        rqt = ('SELECT indicateur '
               'FROM '
               'usager_accidente_par_vehicule as usg '
               'WHERE '
               + str(rayon) + ' > |/((usg.longitude-(' + str(center_waypoint[1]) + '))^2+(+usg.latitude-(' + str(
                    center_waypoint[0]) + '))^2)')
    return rqt

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
                                          ' WHERE abs((heure*100+minute) - (' + str(
                    hrmn) + ')) < 100 OR abs((heure*100+minute) - (' + str(hrmn) + ')) > 2300)')
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
                                          ' WHERE abs((heure*100+minute) - ('+str(hrmn)+')) >= 100 OR abs((heure*100+minute) - ('+str(hrmn)+')) <= 2300)')
    else:
        rqt = ('SELECT sum(indicateur), count(*) '
               'FROM '
               'usager_accidente_par_vehicule as usg '
               'WHERE '
               + str(rayon) + ' > |/((usg.longitude-(' + str(center_waypoint[1]) + '))^2+(+usg.latitude-(' + str(
                    center_waypoint[0]) + '))^2)')
    return rqt

param_weights = {"lt1":1,
                 "gte1":0.5}


class ServiceIndicatorHour(Resource):
    def get(self):
        return {"get": "example"}

    def post(self):
        try:

            json = request.json
            hr = json['heure']
            mn = json['min']
            hrmn = hr * 60 + mn
            waypoint_interval = 10
            start = time.time()
            for route in json['routes']:

                waypoints = route['waypoints']
                ind_sum = 0
                weight_sum = 0
                acc_count = 0

                for index, waypoint in enumerate(waypoints):
                    if index > len(waypoints) - waypoint_interval:
                        break

                    if index % waypoint_interval == 0:
                        for param, w in param_weights.items():
                            rqt = create_indicator_param_request(waypoint, waypoints[index + waypoint_interval], hrmn, param)
                            cursor.execute(rqt)
                            for record in cursor:
                                if record[0]:
                                    ind_sum = ind_sum + (record[0] * w)
                                    weight_sum = weight_sum + w
                                    acc_count = acc_count + 1
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

class ServiceIndicatorHourGrouped(Resource):
    def get(self):
        return {"get": "example"}

    def post(self):
        try:

            json = request.json
            hr = json['heure']
            mn = json['min']
            hrmn = hr * 60 + mn
            waypoint_interval = 10
            start = time.time()
            for route in json['routes']:

                waypoints = route['waypoints']
                ind_sum = 0
                weight_sum = 0
                acc_count = 0

                for index, waypoint in enumerate(waypoints):
                    if index > len(waypoints) - waypoint_interval:
                        break

                    if index % waypoint_interval == 0:
                        for param, w in param_weights.items():
                            rqt = create_indicator_paramGrouped_request(waypoint, waypoints[index + waypoint_interval], hrmn, param)
                            cursor.execute(rqt)
                            for record in cursor:
                                if record[0]:
                                    ind_sum = ind_sum + record[0]*w
                                    weight_sum = weight_sum + record[1]*w
                                    acc_count = acc_count + record[1]
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


class ServiceIndicatorHourGroupedPara(Resource):
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

api.add_resource(ServiceIndicatorLight, '/IndicatorLight')

api.add_resource(ServiceIndicatorHour, '/IndicatorHour')

api.add_resource(ServiceIndicatorHourGrouped, '/IndicatorHourGrouped')

api.add_resource(ServiceIndicatorHourGroupedPara, '/IndicatorHourGroupedPara')


if __name__ == '__main__':
    WSGIRequestHandler.protocol_version = "HTTP/1.1"
    app.run()
