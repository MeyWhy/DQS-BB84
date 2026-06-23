# DQS-BB84: Scalable and Secure QKD: Designing a Microservices-Based Framework for the BB84 Protocol Validated through Optical Simulation

Distributed BB84 Quantum Key Distribution simulation based on a microservices architecture.

## Overview

DQS-BB84 is a scalable implementation of the BB84 Quantum Key Distribution protocol where protocol roles are decomposed into independent services. The system supports concurrent key-generation sessions, asynchronous task execution, and distributed simulation of quantum communication.

<img width="960" height="438" alt="dqs_dashboard" src="https://github.com/user-attachments/assets/09232968-5c60-4da7-b1fa-f9c0eef0a912" />


The project combines:

* Microservices architecture
* Quantum network simulation using QuNetSim
* Asynchronous task processing
* REST-based service communication
* Session management and monitoring
* Optical Fiber Channel Model for Simulation

## Features

* BB84 key generation workflow
* Multiple concurrent QKD sessions
* Simulated quantum and classical channels
* Distributed execution through independent services
* Key Management Entity (KME) integration
* Session tracking and monitoring
* Performance and scalability evaluation
* Realistic optical fiber attenuation simulation 

## Architecture

The platform is composed of several services:

* **Alice Service** - Initiates key generation sessions
* **Bob Service** - Receives and measures qubits
* **KME Service** - Manages generated keys and sessions
* **Task Queue & Workers** - Execute simulations asynchronously
* **Monitoring Components** - Collect runtime metrics

## Technology Stack

* Python
* FastAPI
* Celery
* Redis
* QuNetSim
* Ansys Lumerical

## Research Context

This project was developed as part of a Master's thesis exploring scalable and secure distributed implementations of the BB84 protocol through modern software architecture principles. In addition, a 3 layer photonic simulation that uses Ansys Lumerical for Beer-Lambert attenuation, Ornstein-Uhlenbeck polarization drift, and SNSP Detector simulation.

This project is intended for educational and research purposes.
