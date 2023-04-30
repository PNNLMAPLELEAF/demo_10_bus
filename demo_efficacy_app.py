"""
Created on March 6, 2023
@author: Alexander Anderson
This is a demo app that publishes efficacy metrics and writes them to csv file
"""

import math
import csv
import time

from gridappsd.topics import application_input_topic, application_output_topic #use this topic for app input/output


import importlib
global cim

# Specify CIM version 
cim_profile = 'rc4_2021' #CIM 17V23
cim = importlib.import_module('cimlab.data_profile.' + cim_profile)
sparql = importlib.import_module('cimlab.loaders.sparql.' + cim_profile)


# Example model parser
def ModelParser(network):
    # only interested in a few classes of objects
    cim_classes = [cim.EnergyConsumer, cim.PowerElectronicsConnection, cim.Analog, cim.LoadBreakSwitch]
    
    # parse through each area:
    for switch_area in network.switch_areas:
        for cim_class in cim_classes:
            if cim_class in switch_area.typed_catalog:
                switch_area.get_all_attributes(cim_class)
         
        
        
# Example method for parsing voltage measurements
def ParseVoltMeas(message, dist_area, cim_class):
    
    volt_meas = {}
    if cim_class in dist_area.typed_catalog: #check if class exists
        for obj in list(dist_area.typed_catalog[cim_class].values()): #iterate through all object instances
            volt_meas[obj.mRID] = {}
            volt_meas[obj.mRID]["name"] = obj.name #initialize meas instance
            for meas in obj.Measurements: #iterate through all measurements for object
                if meas.measurementType == "PNV": #check if phase-neutral-voltage
                    volt_meas[obj.mRID][meas.phases] = message["message"]["measurements"][meas.mRID]["magnitude"] #get value from message
    else:
        power_meas = {}
    return volt_meas
     
    
    
# Example method for parsing P & Q measurements
def ParsePowerMeas(message, dist_area, cim_class):
    
    power_meas = {}
    if cim_class in dist_area.typed_catalog: #check if class exists
        for obj in list(dist_area.typed_catalog[cim_class].values()): #iterate through all object instances
            power_meas[obj.mRID] = {}
            power_meas[obj.mRID]["name"] = obj.name #initialize meas instance
            power_meas[obj.mRID]["p"] = 0.0
            power_meas[obj.mRID]["q"] = 0.0
            for meas in obj.Measurements: #iterate through all measurements for object
                if meas.measurementType == "VA": #check if phase apparent power (volt-amp)
                    phs_s = message["message"]["measurements"][meas.mRID]["magnitude"] #get value from message
                    phs_a = message["message"]["measurements"][meas.mRID]["angle"]
                    power_meas[obj.mRID]["p"] = power_meas[obj.mRID]["p"] + phs_s*math.cos(math.radians(phs_a))
                    power_meas[obj.mRID]["q"] = power_meas[obj.mRID]["q"] + phs_s*math.sin(math.radians(phs_a))
    else:
        power_meas = {}                 
    return power_meas
        
        
    
                
class DemoEfficacyApp():
    # Application init
    def __init__(self, gapps, network, simulation_id):
        self.simulation_id = simulation_id
        self.network = network
        self.gapps = gapps
        self.efficacy = {}
        self.der = {}
        self.load = {}
        
        self.app_output_topic = application_output_topic('demo-efficacy-app', simulation_id)
        
        # Create output file with feeder breaker names        
        self.csv_headers = ["timestamp"]
        for switch_area in self.network.switch_areas:
            parent_switch = list(switch_area.boundary_switches.values())[0]
            self.csv_headers.append(parent_switch.name)
        with open('efficacy'+str(simulation_id)+'.csv', 'w', encoding='utf-8') as self.csv_file:
            self.csv_writer = csv.DictWriter(self.csv_file, fieldnames = self.csv_headers)
            self.csv_writer.writeheader()
            
        with open('load'+str(simulation_id)+'.csv', 'w', encoding='utf-8') as self.csv_file:
            self.csv_writer = csv.DictWriter(self.csv_file, fieldnames = self.csv_headers)
            self.csv_writer.writeheader()
            
        with open('der'+str(simulation_id)+'.csv', 'w', encoding='utf-8') as self.csv_file:
            self.csv_writer = csv.DictWriter(self.csv_file, fieldnames = self.csv_headers)
            self.csv_writer.writeheader()
        
    
    # Application core - this must be named "on_message(self, headers, message)"
    # This runs every time a new measurement set is received
    def on_message(self, headers, message):
        
        # Check if message received is simulation output message
        if "output" in headers["destination"]:
            timestamp = message["message"]["timestamp"]
            date_time = time.strftime("%D %H:%M", time.localtime(int(timestamp)))
            
            self.efficacy[timestamp] = {}
            self.der[timestamp] = {}
            self.load[timestamp] = {}
            
            self.efficacy[timestamp]["timestamp"] = date_time
            self.der[timestamp]["timestamp"] = date_time
            self.load[timestamp]["timestamp"] = date_time
            
            
            # Get feeder head p,q from breakers
#             breaker_power = ParsePowerMeas(message, self.network, cim.LoadBreakSwitch) # VA meas missing from CIMHub insert script

            # Get load and DER power from feeders
            for switch_area in self.network.switch_areas:
                
                der_power = ParsePowerMeas(message, switch_area, cim.PowerElectronicsConnection)
                load_power = ParsePowerMeas(message, switch_area, cim.EnergyConsumer)
                
                
                total_der_p = 0.0
                for mrid in list(der_power.keys()):
                    total_der_p = total_der_p + der_power[mrid]["p"]

                total_load_p = 0.0  
                for mrid in list(load_power.keys()):
                    total_load_p = total_load_p + load_power[mrid]["p"]
                
                parent_switch = list(switch_area.boundary_switches.values())[0]
                

                if load_power:
                    self.efficacy[timestamp][parent_switch.name] =  (total_load_p + total_der_p)/total_load_p
                    
                    self.der[timestamp][parent_switch.name] = total_der_p
                    self.load[timestamp][parent_switch.name] = total_load_p
                    
                    
                    
                
            with open('efficacy'+str(self.simulation_id)+'.csv', 'a', encoding='utf-8') as self.csv_file:
                self.csv_writer = csv.DictWriter(self.csv_file, fieldnames = self.csv_headers)
                self.csv_writer.writerow(self.efficacy[timestamp])
            with open('load'+str(self.simulation_id)+'.csv', 'a', encoding='utf-8') as self.csv_file:
                self.csv_writer = csv.DictWriter(self.csv_file, fieldnames = self.csv_headers)
                self.csv_writer.writerow(self.load[timestamp])
            with open('der'+str(self.simulation_id)+'.csv', 'a', encoding='utf-8') as self.csv_file:
                self.csv_writer = csv.DictWriter(self.csv_file, fieldnames = self.csv_headers)
                self.csv_writer.writerow(self.der[timestamp])
                
            self.publish_efficacy(timestamp)
            
                
    def publish_efficacy(self, timestamp):
        message = {
            'timestamp': timestamp,
            'efficacy': self.efficacy[timestamp],
            'der': self.der[timestamp]
        }
        self.gapps.send(self.app_output_topic, message)
        
