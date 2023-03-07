from demo_efficacy_app import DemoEfficacyApp, ModelParser

import os
import json
import time

from gridappsd import GridAPPSD, topics as t
from gridappsd.topics import simulation_input_topic, simulation_output_topic #use this topic to get streaming data
from gridappsd.topics import application_input_topic, application_output_topic #use this topic for app input/output
from gridappsd.topics import service_input_topic, service_output_topic #use this topic for service input/output (e.g. alarms, SE)
from gridappsd.simulation import Simulation # Import Simulation Library



from cimlab.loaders import Parameter, ConnectionParameters
from cimlab.loaders.blazegraph.blazegraph import BlazegraphConnection
from cimlab.models import DistributedModel

import importlib
global cim

# Specify CIM version 
cim_profile = 'rc4_2021' #CIM 17V23
cim = importlib.import_module('cimlab.data_profile.' + cim_profile)

    
def _main():
    
    # Connect to GridAPPSD:
    os.environ['GRIDAPPSD_APPLICATION_ID'] = 'maple-hello-world'
    os.environ['GRIDAPPSD_APPLICATION_STATUS'] = 'STARTED'
    os.environ['GRIDAPPSD_USER'] = 'system'
    os.environ['GRIDAPPSD_PASSWORD'] = 'manager'
    gapps = GridAPPSD()
    assert gapps.connected
    gapps_log = gapps.get_logger()

    
    # Connect to Blazegraph for CIM-LAB:
    params = ConnectionParameters([Parameter(key="url", value="http://localhost:8889/bigdata/namespace/kb/sparql")])
    bg = BlazegraphConnection(params, 'rc4_2021')
    
    
    # Get CIM-LAB Distributed Model:
    model_mrid = "_9E985101-27AD-4FE4-B36A-EBECC98CDFAB" # maple10bus
    feeder = cim.Feeder(name = "maple 10 bus", mRID = model_mrid)
    
    topic = "goss.gridappsd.request.data.topology"
    message = {
       "requestType": "GET_SWITCH_AREAS",
       "modelID":  model_mrid,
       "resultFormat": "JSON"
    }
    topology_response = gapps.get_response(topic, message, timeout=30)
    
    network = DistributedModel(connection=bg, feeder=feeder, topology=topology_response['feeders'])
    
    # Run CIM-LAB routines to get power system model info
    ModelParser(network)
    
    # Start a new simulation
    run_config = json.load(open("maple_demo_sim_config.json")) # Pull simulation start message from saved file
    simulation_obj = Simulation(gapps, run_config) # Create Simulation object
    simulation_obj.start_simulation() # Start Simulation
    simulation_id = simulation_obj.simulation_id # Obtain Simulation ID
    print("Successfully started simulation with simulation_id: ", simulation_id)
    
    # Run Demo Efficacy App
    app = DemoEfficacyApp(gapps, network, simulation_id)
    sim_topic = simulation_output_topic(simulation_id)
    gapps.subscribe(sim_topic, app)
    
    while True:
        time.sleep(1)
        
        
if __name__ == "__main__":
    _main()
    