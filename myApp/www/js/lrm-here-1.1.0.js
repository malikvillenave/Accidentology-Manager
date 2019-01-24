(function e(t,n,r){function s(o,u){if(!n[o]){if(!t[o]){var a=typeof require=="function"&&require;if(!u&&a)return a(o,!0);if(i)return i(o,!0);var f=new Error("Cannot find module '"+o+"'");throw f.code="MODULE_NOT_FOUND",f}var l=n[o]={exports:{}};t[o][0].call(l.exports,function(e){var n=t[o][1][e];return s(n?n:e)},l,l.exports,e,t,n,r)}return n[o].exports}var i=typeof require=="function"&&require;for(var o=0;o<r.length;o++)s(r[o]);return s})({1:[function(require,module,exports){
function corslite(url, callback, cors) {
    //console.log("lrm-here-1.1.0 - Corslite");
    var sent = false;

    if (typeof window.XMLHttpRequest === 'undefined') {
        return callback(Error('Browser not supported'));
    }

    if (typeof cors === 'undefined') {
        var m = url.match(/^\s*https?:\/\/[^\/]*/);
        cors = m && (m[0] !== location.protocol + '//' + location.domain +
                (location.port ? ':' + location.port : ''));
    }

    var x = new window.XMLHttpRequest();

    function isSuccessful(status) {
        return status >= 200 && status < 300 || status === 304;
    }

    if (cors && !('withCredentials' in x)) {
        // IE8-9
        x = new window.XDomainRequest();

        // Ensure callback is never called synchronously, i.e., before
        // x.send() returns (this has been observed in the wild).
        // See https://github.com/mapbox/mapbox.js/issues/472
        var original = callback;
        callback = function() {
            if (sent) {
                original.apply(this, arguments);
            } else {
                var that = this, args = arguments;
                setTimeout(function() {
                    original.apply(that, args);
                }, 0);
            }
        }
    }

    function loaded() {
        
        if (
            // XDomainRequest
            x.status === undefined ||
            // modern browsers
            isSuccessful(x.status))
            {
                //console.log("Loaded IF")
                callback.call(x, null, x);
            }
        else callback.call(x, x, null);
    }

    // Both `onreadystatechange` and `onload` can fire. `onreadystatechange`
    // has [been supported for longer](http://stackoverflow.com/a/9181508/229001).
    if ('onload' in x) {
        x.onload = loaded;
    } else {
        x.onreadystatechange = function readystate() {
            if (x.readyState === 4) {
                loaded();
            }
        };
    }

    // Call the callback with the XMLHttpRequest object as an error and prevent
    // it from ever being called again by reassigning it to `noop`
    x.onerror = function error(evt) {
        // XDomainRequest provides no evt parameter
        callback.call(this, evt || true, null);
        callback = function() { };
    };

    // IE9 must have onprogress be set to a unique function.
    x.onprogress = function() { };

    x.ontimeout = function(evt) {
        callback.call(this, evt, null);
        callback = function() { };
    };

    x.onabort = function(evt) {
        callback.call(this, evt, null);
        callback = function() { };
    };

    // GET is the only supported HTTP Verb by XDomainRequest and is the
    // only one supported here.
    x.open('GET', url, true);

    // Send the request. Sending data is not supported.
    x.send(null);
    sent = true;

    return x;
}

if (typeof module !== 'undefined') module.exports = corslite;

},{}],2:[function(require,module,exports){
// haversine
// By Nick Justice (niix)
// https://github.com/niix/haversine

var haversine = (function() {

  // convert to radians
  var toRad = function(num) {
    return num * Math.PI / 180
  }

  return function haversine(start, end, options) {
    var km    = 6371
    var mile  = 3960
    options   = options || {}

    var R = options.unit === 'mile' ?
      mile :
      km

    var dLat = toRad(end.latitude - start.latitude)
    var dLon = toRad(end.longitude - start.longitude)
    var lat1 = toRad(start.latitude)
    var lat2 = toRad(end.latitude)

    var a = Math.sin(dLat/2) * Math.sin(dLat/2) +
            Math.sin(dLon/2) * Math.sin(dLon/2) * Math.cos(lat1) * Math.cos(lat2)
    var c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a))

    if (options.threshold) {
      return options.threshold > (R * c)
    } else {
      return R * c
    }
  }

})()

module.exports = haversine
},{}],3:[function(require,module,exports){
(function (global){
(function() {
    
    'use strict';

    var L = (typeof window !== "undefined" ? window['L'] : typeof global !== "undefined" ? global['L'] : null);
    var corslite = require('corslite');
    var haversine = require('haversine');

    L.Routing = L.Routing || {};

    L.Routing.Here = L.Class.extend({
        options: {
            serviceUrl: 'https://route.cit.api.here.com/routing/7.2/calculateroute.json',
            timeout: 30 * 1000,
            alternatives: 0,
            mode: 'fastest;car;traffic:enabled',
            urlParameters: {}
        },

        initialize: function(appId, appCode, options) {
            this._appId = appId;
            this._appCode = appCode;
            L.Util.setOptions(this, options);
        },

        route: function(waypoints, callback, context, options) {

            var timedOut = false,
                wps = [],
                url,
                timer,
                wp,
                i;

            options = options || {};
            url = this.buildRouteUrl(waypoints, options);
            console.log(url);
            timer = setTimeout(30000000,function() {
                                timedOut = true;
                                callback.call(context || callback, {
                                    status: -1,
                                    message: 'Here request timed out.'
                                });
                            }, this.options.timeout);

            // Create a copy of the waypoints, since they
            // might otherwise be asynchronously modified while
            // the request is being processed.
            for (i = 0; i < waypoints.length; i++) {
                wp = waypoints[i];
                wps.push({
                    latLng: wp.latLng,
                    name: wp.name,
                    options: wp.options
                });
            }

            corslite(url, L.bind(function(err, resp) {
                var ladata = null;

                //clearTimeout(timer);
                if (!timedOut) {
                    if (!err) {

                
                        //Récupéreration de la reponse de Here
                        var dataJson = resp.responseText;
                        
                        //Parser pour récupérer toutes les routes possibles
                        var parseRoute = JSON.parse(dataJson);
                        //var dataJsonV2 = parseRoute.response.route;
                        //console.log(dataJsonV2);
                        
                        //Ajout d'un attribut id pour chaque route détectée
                        parseRoute.response.route.map(function(route,index) {
                            route['id'] = index;
                        });
                        //console.log('affichage de parseRoute');
                        //console.log(parseRoute);

                    //Récupération des coordonnées GPS du point de départ
                    //Servira pour la requete pour OpenWeather
                        var latGPS = parseRoute.response.route[0].waypoint[0].originalPosition.latitude;
                        var lonGPS = parseRoute.response.route[0].waypoint[0].originalPosition.longitude;
                        var weatherJSONid;
                        var weatherJSONmain;
                        var urlWeather = "https://api.openweathermap.org/data/2.5/weather?lat=" + latGPS +"&lon=" + lonGPS + "&appid=60ffb92f8328c08440fcb05e58563fb4";
                        
                        var xhr = new XMLHttpRequest();

                        var url =urlWeather;
                        
                        xhr.open("GET", url, false);
                        xhr.onreadystatechange = function () { 
                            if (xhr.status == 200) {
                                //console.log("reponse meteo");
                                var rep = JSON.parse(xhr.response);
                                //console.log(rep);
                                //weatherJSONid =rep.weather[0].id;
                                weatherJSONmain =rep.weather[0].main;
                            }
                        };
        
                        xhr.send(null);
                    
                        
                        var currentDate = new Date();
                        
                        //Alleger le JSON en ne prenant que les donnees utiles pour l'appli
                        var dataJsonLight = {

                                heure: currentDate.getHours(),
                                minute:currentDate.getMinutes(),
                                weather:weatherJSONmain,
                                routes: parseRoute.response.route.map(function(route)
                                {
                                
                                    return {

                                        id:route.id,
                                        waypoints: route.shape.filter(function(shape,index) {
                                            return index % 10 == 0;
                                        })
                                    }
                            
                                })
                                
                        };
                

                        //console.log('affichage du json a envoyer');
                        console.log(dataJsonLight);
                        
                        
                        var xhr = new XMLHttpRequest();

                        var url = "http://10.0.2.2:5000/IndicatorLight";
                        
                        xhr.open("POST", url, true);
                        
                        xhr.timeout = 30*1000*1000;
                        xhr.setRequestHeader("Content-type", "application/json");
                        xhr.setRequestHeader('Cache-Control', 'no-cache');
                        xhr.onreadystatechange = function () { 
                            if (xhr.readyState == 4 && xhr.status == 200) {
                                document.getElementsByClassName("routesToDisplay")[0].setAttribute('style', 'display:block;');
                                
                                var responseBackend = JSON.parse(xhr.response);

                                //traitement reponse backend
                               // console.log('id : '+responseBackend.response.routes[0].id);
                                parseRoute.response.route.map(function(route,index) {
                                    if(route['id'] == responseBackend.response.routes[index].id ){

                                        route['dangerLevel'] = responseBackend.response.routes[index].dangerLevel;
                                    }
                                });
                                //console.log("Affichage resultat traitement");
                                //console.log(parseRoute);
                                
                               this._routeDone(parseRoute, wps, callback, context);
                            }
                        }.bind(this);
                        //Envoi du Json au backEnd
                        
                        xhr.send(JSON.stringify(dataJsonLight));
                        //console.log("Envoi xhr fait");


                    } else {
                        callback.call(context || callback, {
                            status: -1,
                            message: 'HTTP request failed: ' + err
                        });
                    }
                }
            }, this));

            return this;
        },

        _routeDone: function(response, inputWaypoints, callback, context) {
        
            var alts = [],
                waypoints,
                waypoint,
                coordinates,
                i, j, k,
                instructions,
                distance,
                time,
                leg,
                maneuver,
                startingSearchIndex,
                instruction,
                path;

            context = context || callback;
            if (!response.response.route) {
                callback.call(context, {
                    // TODO: include all errors
                    status: response.type,
                    message: response.details
                });
                return;
            }

            for (i = 0; i < response.response.route.length; i++) {
                path = response.response.route[i];
                coordinates = this._decodeGeometry(path.shape);
                startingSearchIndex = 0;

                instructions = [];
                time = 0;
                distance = 0;
                for(j = 0; j < path.leg.length; j++) {
                    leg = path.leg[j];
                    for(k = 0; k < leg.maneuver.length; k++) {
                        maneuver = leg.maneuver[k];
                        distance += maneuver.length;
                        time += maneuver.travelTime;
                        instruction = this._convertInstruction(maneuver, coordinates, startingSearchIndex);
                        instructions.push(instruction);
                        startingSearchIndex = instruction.index;
                    }
                }

                waypoints = [];
                for(j = 0; j < path.waypoint.length; j++) {
                    waypoint = path.waypoint[j];
                    waypoints.push(new L.LatLng(
                        waypoint.mappedPosition.latitude, 
                        waypoint.mappedPosition.longitude));
                }

                //console.log("Danger Level");
                //console.log(response.response.route[i].dangerLevel);
                alts.push({
                    name: '',
                    coordinates: coordinates,
                    dangerLevel : response.response.route[i].dangerLevel,
                    instructions: instructions,
                    summary: {
                        totalDistance: distance,
                        totalTime: time,
                    },
                    inputWaypoints: inputWaypoints,
                    waypoints: waypoints
                });
            }

            callback.call(context, null, alts);
        },

        _decodeGeometry: function(geometry) {
            //console.log("decodeGeometry");
            var latlngs = new Array(geometry.length),
                coord,
                i;
            for (i = 0; i < geometry.length; i++) {
                coord = geometry[i].split(",");
                latlngs[i] = ([parseFloat(coord[0]), parseFloat(coord[1])]);
            }

            return latlngs;
        },

        buildRouteUrl: function(waypoints, options) {
            //console.log("buildRouteUrl");
            var locs = [],
                i,
                alternatives,
                baseUrl;
            
            for (i = 0; i < waypoints.length; i++) {
                locs.push('waypoint' + i + '=geo!' + waypoints[i].latLng.lat + ',' + waypoints[i].latLng.lng);
            }
            if(waypoints.length > 2) {
                alternatives = 0;
            } else {
                //With more than 1 waypoint, requests for alternatives are invalid
                alternatives = this.options.alternatives;
            }
            baseUrl = this.options.serviceUrl + '?' + locs.join('&');

            return baseUrl + L.Util.getParamString(L.extend({
                    instructionFormat: 'text',
                    app_code: this._appCode,
                    app_id: this._appId,
                    representation: "navigation",
                    mode: this.options.mode,
                    alternatives: alternatives
                }, this.options.urlParameters), baseUrl);
        },

        _convertInstruction: function(instruction, coordinates, startingSearchIndex) {
            //console.log("convertInstruction");
            var i,
            distance,
            closestDistance = 0,
            closestIndex = -1,
            coordinate = instruction.position;
            if(startingSearchIndex < 0) {
                startingSearchIndex = 0;
            }
            for(i = startingSearchIndex; i < coordinates.length; i++) {
                distance = haversine(coordinate, {latitude:coordinates[0], longitude:coordinates[1]});
                if(distance < closestDistance || closestIndex == -1) {
                    closestDistance = distance;
                    closestIndex = i;
                }
            }
            return {
                text: instruction.instruction,//text,
                distance: instruction.length,
                time: instruction.travelTime,
                index: closestIndex
                /*
                type: instruction.action,
                road: instruction.roadName,
                */
            };
        },

    });
    
    L.Routing.here = function(appId, appCode, options) {
        return new L.Routing.Here(appId, appCode, options);
    };

    module.exports = L.Routing.Here;
})();

}).call(this,typeof global !== "undefined" ? global : typeof self !== "undefined" ? self : typeof window !== "undefined" ? window : {})
},{"corslite":1,"haversine":2}]},{},[3]);