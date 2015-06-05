// Copyright (c) 2015 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

"use strict";

var assert = require('assert');
var inherits = require('util').inherits;
var States = require('./states');
var EventEmitter = require('./lib/event_emitter');
var StateMachine = require('./state_machine');

// The each circuit uses the circuits collection as the "nextHandler" for
// "shouldRequest" to consult.  Peers use this hook to weight peers both by
// healthy and other factors, but the circuit only needs to know about health
// before forwarding.

function AlwaysShouldRequestHandler() { }

AlwaysShouldRequestHandler.prototype.shouldRequest = function shouldRequest() {
    return true;
};

var alwaysShouldRequestHandler = new AlwaysShouldRequestHandler();

//  circuit = circuits
//      .circuitsByServiceName[serviceName]
//      .circuitsByCallerName[callerName]
//      .circuitsByEndpointName[endpointName]

function EndpointCircuits(root) {
    var self = this;
    self.root = root;
    self.circuitsByEndpointName = {};
}

EndpointCircuits.prototype.getCircuit = function getCircuit(callerName, serviceName, endpointName) {
    var self = this;
    var circuit = self.circuitsByEndpointName['$' + endpointName];
    if (!circuit) {
        circuit = new Circuit();
        circuit.callerName = callerName;
        circuit.serviceName = serviceName;
        circuit.endpointName = endpointName;
        circuit.shouldRequestOptions = self.root.shouldRequestOptions;
        circuit.stateOptions = self.root.stateOptions;
        circuit.setState(States.HealthyState);
        self.circuitsByEndpointName['$' + endpointName] = circuit;
    }
    return circuit;
};

function ServiceCircuits(root) {
    var self = this;
    self.root = root;
    self.circuitsByCallerName = {};
}

ServiceCircuits.prototype.getCircuit = function getCircuit(callerName, serviceName, endpointName) {
    var self = this;
    var circuits = self.circuitsByCallerName['$' + callerName];
    if (!circuits) {
        circuits = new EndpointCircuits(self.root);
        self.circuitsByCallerName['$' + callerName] = circuits;
    }
    return circuits.getCircuit(callerName, serviceName, endpointName);
};

function Circuits(options) {
    var self = this;
    self.circuitsByServiceName = {};
    self.stateOptions = {
        stateMachine: self,
        nextHandler: alwaysShouldRequestHandler,
        timers: options.timers,
        random: options.random,
        period: options.period,
        maxErrorRate: options.maxErrorRate,
        minRequests: options.minRequests,
        probation: options.probation,
    };
    self.shouldRequestOptions = {};
}

Circuits.prototype.getCircuit = function getCircuit(callerName, serviceName, endpointName) {
    var self = this;
    var circuits = self.circuitsByServiceName['$' + serviceName];
    if (!circuits) {
        circuits = new EndpointCircuits(self);
        self.circuitsByServiceName['$' + callerName] = circuits;
    }
    return circuits.getCircuit(callerName, serviceName, endpointName);
};

Circuits.prototype.handleRequest = function handleRequest(req, buildRes, nextHandler) {
    var self = this;
    // Default the caller name.
    // All callers that fail to specifiy a cn share a circuit for each sn:en
    // and fail together.
    var callerName = req.headers.cn || 'yunocn';
    var serviceName = req.serviceName;
    if (!serviceName) {
        return buildRes().sendError('BadRequest', 'All requests must have a service name');
    }
    return req.withArg1(function withArg1(endpointName) {
        var circuit = self.getCircuit(callerName, serviceName, endpointName);
        return circuit.handleRequest(req, buildRes, nextHandler);
    });
};

// Called upon membership change to collect services that the corresponding
// exit node is no longer responsible for.
Circuits.prototype.updateServices = function updateServices(managesService) {
    var self = this;
    var serviceNames = Object.keys(self.circuitsByServiceName);
    for (var index = 0; index < serviceNames.length; index++) {
        var serviceName = serviceNames[index];
        if (!managesService(serviceName)) {
            delete self.circuitsByServiceName[serviceName];
        }
    }
};

function Circuit() {
    var self = this;
    self.circuitName = null;
    self.callerName = null;
    self.serviceName = null;
    self.endpointName = null;
    self.shouldRequestOptions = null;
    self.stateOptions = null;
    StateMachine.call(self);
    EventEmitter.call(self);
    self.stateChangedEvent = self.defineEvent('stateChanged');
}

inherits(Circuit, EventEmitter);

Circuit.prototype.setState = StateMachine.prototype.setState;

Circuit.prototype.handleRequest = function handleRequest(req, buildRes, nextHandler) {
    var self = this;
    if (self.state.shouldRequest(req, self.shouldRequestOptions)) {
        return self.monitorRequest(req, buildRes, nextHandler);
    } else {
        return buildRes().sendError('UnexpectedError', 'Service is not healthy');
    }
};

Circuit.prototype.monitorRequest = function monitorRequest(req, buildRes, nextHandler) {
    var self = this;
    self.state.onRequest(req);

    function monitorBuildRes(options) {
        var res = buildRes(options);
        res.errorEvent.on(onResponseError);
        res.finishEvent.on(onResponseFinish);
        return res;
    }

    function onResponseError(err) {
        self.state.onRequestError(err);
    }

    function onResponseFinish() {
        self.state.onRequestResponse(req);
    }

    return nextHandler.handleRequest(req, monitorBuildRes);
};

module.exports = Circuits;
