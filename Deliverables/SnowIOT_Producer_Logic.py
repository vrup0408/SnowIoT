import json
import random
import string
import threading 
from azure.eventhub import EventHubProducerClient, EventData
from datetime import datetime


event_hubs_connection_string = "Endpoint=sb://evhns-snowiot.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=6IiQgE9d3Sg/ERIYdpWsm/f5Pjny3tOEl+AEhOiXoJg="
eventhub_name = "evh-snowiot"

def generate_vin():
    letters_and_digits = string.ascii_uppercase + string.digits
    return ''.join(random.choice(letters_and_digits) for _ in range(17))

def generate_data_and_send():
    
    producer_client = EventHubProducerClient.from_connection_string(event_hubs_connection_string, eventhub_name=eventhub_name)

    try:
        while True:

                vin = generate_vin()
                age_ind = str(round(random.uniform(1,30)))
                odo_read = round(random.uniform(0, 50000))
                id = str(random.randint(1, 100000))
                

                def generate_trip_start_event():
                    trip_start_event = {
                        "tripvehicledata": [
                                {
                                        "id": id,
                                        "vin": vin,
                                        "PUBLISH_TIME": datetime.utcnow().isoformat(),
                                        "eventCd": "TripStart",
                                        "data": [
                                            {
                                                "name": "AGEIND",#random(1-30)
                                                "val": age_ind,
                                                "uom": "NA",
                                                "cts": datetime.utcnow().isoformat()
                                                },
                                            {
                                                    "name": "CURRENT_SPEED",
                                                    "val": "NULL",
                                                    "uom": "NA",
                                                    "cts": datetime.utcnow().isoformat()
                                                },

                                                {
                                                    "name": "CURRENT_SPEED_VALIDITY",
                                                    "val": "NULL",
                                                    "uom": "NA",
                                                    "cts": datetime.utcnow().isoformat()
                                                },

                                                {
                                                    "name": "DIR",
                                                    "val": "141",
                                                    "uom": "NA",
                                                    "cts": datetime.utcnow().isoformat()
                                                },

                                                {
                                                    "name": "DRIVER_SEATBELT_INDICATOR",
                                                    "val": random.choice(["LATCHED", "UNLATCHED"]),
                                                    "uom": "NA",
                                                    "cts": datetime.utcnow().isoformat()
                                                },

                                                {
                                                    "name": "EHPE",
                                                    "val": "2",
                                                    "uom": "NA",
                                                    "cts": datetime.utcnow().isoformat()
                                                },

                                                {
                                                    "name": "ELV",
                                                    "val": "1105",
                                                    "uom": "NA",
                                                    "cts": datetime.utcnow().isoformat()
                                                },

                                                {
                                                    "name": "ENGINE_RUN_TIME_TOTAL",
                                                    "val": "1609270",
                                                    "uom": "sec",
                                                    "cts": datetime.utcnow().isoformat()
                                                },

                                                {
                                                    "name": "ENG_IDLE_RUN_TIME_TOTAL",
                                                    "val": "661090",
                                                    "uom": "sec",
                                                    "cts": datetime.utcnow().isoformat()
                                                },

                                                {
                                                    "name": "ENG_IDL_RUNT_TOTSUP",
                                                    "val": "TRUE",
                                                    "uom": "NA",
                                                    "cts": datetime.utcnow().isoformat()
                                                },

                                                {
                                                    "name": "ENG_RUNT_PTO_ACT_TOTSUP",
                                                    "val": "FALSE",
                                                    "uom": "NA",
                                                    "cts": datetime.utcnow().isoformat()
                                                },

                                                {
                                                    "name": "ENG_RUN_TME_PTO_ACT_TOT",
                                                    "val": "0",
                                                    "uom": "sec",
                                                    "cts": datetime.utcnow().isoformat()
                                                },

                                                {
                                                    "name": "ENG_RUN_TOTSUP",
                                                    "val": "TRUE",
                                                    "uom": "NA",
                                                    "cts": datetime.utcnow().isoformat()
                                                },

                                                {
                                                    "name": "FUEL_USED",
                                                    "val": "2463.81",
                                                    "uom": "L",
                                                    "cts": datetime.utcnow().isoformat()
                                                },

                                                {
                                                    "name": "IGN_CYL",
                                                    "val": "ON",
                                                    "uom": "NA",
                                                    "cts": datetime.utcnow().isoformat()
                                                },

                                                {
                                                    "name": "LAT",
                                                    "val": "35.1532211",
                                                    "uom": "degree",
                                                    "cts": datetime.utcnow().isoformat()
                                                },

                                                {
                                                    "name": "LNG",
                                                    "val": "101.9026337",
                                                    "uom": "degree",
                                                    "cts": datetime.utcnow().isoformat()
                                                },

                                                {
                                                    "name": "LOCATION_TIME_OFFSET",
                                                    "val": "5",
                                                    "uom": "hr",
                                                    "cts": datetime.utcnow().isoformat()
                                                },

                                                {
                                                    "name": "ODO_READ",
                                                    "val": odo_read,
                                                    "uom": "km",
                                                    "cts": datetime.utcnow().isoformat()
                                                },

                                                {
                                                    "name": "SPDINKM",
                                                    "val": "0",
                                                    "uom": "kmph",
                                                    "cts": datetime.utcnow().isoformat()
                                                },

                                                {
                                                    "name": "SPEED_RATE_OF_CHANGE",
                                                    "val": "NULL",
                                                    "uom": "NA",
                                                    "cts": datetime.utcnow().isoformat()
                                                },

                                                {
                                                    "name": "SPEED_RATE_OF_CHANGE_POSITIVE",
                                                    "val": "NULL",
                                                    "uom": "NA",
                                                    "cts": datetime.utcnow().isoformat()
                                                },

                                                {
                                                    "name": "SYSTEM_POWER_MODE",
                                                    "val": "CRANK",
                                                    "uom": "NA",
                                                    "cts": datetime.utcnow().isoformat()
                                                },
                                            ]
                                            }
                                        ]
                                        }    
                    return json.dumps(trip_start_event)

                def generate_hard_brake_event():
                    s_km = str(round(random.uniform(20, 140)))
                    current_speed = random.uniform(20, 140)
                    current_speed_validity = "VALID" if current_speed < 80 else "INVALID"
                    s_change = str(round(random.uniform(7, 10)))
                    hard_brake_event = {
                        "tripvehicledata": [
                                {
                                    "id": id,
                                    "vin": vin,
                                    "PUBLISH_TIME": datetime.utcnow().isoformat(),
                                    "eventCd": "HardBrake",
                                    "data": [
                                        {
                                            "name": "AGEIND",
                                            "val": age_ind,
                                            "uom": "NA",
                                            "cts": datetime.utcnow().isoformat()
                                            },
                                        {
                                                "name": "CURRENT_SPEED",
                                                "val": str(round(current_speed)),
                                                "uom": "kmph",
                                                "cts": datetime.utcnow().isoformat()
                                            },

                                            {
                                                "name": "CURRENT_SPEED_VALIDITY",
                                                "val": current_speed_validity,
                                                "uom": "NA",
                                                "cts": datetime.utcnow().isoformat()
                                            },

                                            {
                                                "name": "DIR",
                                                "val": "12",
                                                "uom": "NA",
                                                "cts": datetime.utcnow().isoformat()
                                            },

                                            {
                                                "name": "DRIVER_SEATBELT_INDICATOR",
                                                "val": "NULL",
                                                "uom": "NA",
                                                "cts": datetime.utcnow().isoformat()
                                            },

                                            {
                                                "name": "EHPE",
                                                "val": "1",
                                                "uom": "NA",
                                                "cts": datetime.utcnow().isoformat()
                                            },

                                            {
                                                "name": "ELV",
                                                "val": "1103",
                                                "uom": "NA",
                                                "cts": datetime.utcnow().isoformat()
                                            },

                                            {
                                                "name": "ENGINE_RUN_TIME_TOTAL",
                                                "val": "NULL",
                                                "uom": "NA",
                                                "cts": datetime.utcnow().isoformat()
                                            },

                                            {
                                                "name": "ENG_IDLE_RUN_TIME_TOTAL",
                                                "val": "NULL",
                                                "uom": "NA",
                                                "cts": datetime.utcnow().isoformat()
                                            },

                                            {
                                                "name": "ENG_IDL_RUNT_TOTSUP",
                                                "val": "NULL",
                                                "uom": "NA",
                                                "cts": datetime.utcnow().isoformat()
                                            },

                                            {
                                                "name": "ENG_RUNT_PTO_ACT_TOTSUP",
                                                "val": "NULL",
                                                "uom": "NA",
                                                "cts": datetime.utcnow().isoformat()
                                            },

                                            {
                                                "name": "ENG_RUN_TME_PTO_ACT_TOT",
                                                "val": "NULL",
                                                "uom": "NA",
                                                "cts": datetime.utcnow().isoformat()
                                            },

                                            {
                                                "name": "ENG_RUN_TOTSUP",
                                                "val": "NULL",
                                                "uom": "NA",
                                                "cts": datetime.utcnow().isoformat()
                                            },

                                            {
                                                "name": "FUEL_USED",
                                                "val": "NULL",
                                                "uom": "NA",
                                                "cts": datetime.utcnow().isoformat()
                                            },

                                            {
                                                "name": "IGN_CYL",
                                                "val": "NULL",
                                                "uom": "NA",
                                                "cts": datetime.utcnow().isoformat()
                                            },

                                            {
                                                "name": "LAT",
                                                "val": "35.1532211",
                                                "uom": "degree",
                                                "cts": datetime.utcnow().isoformat()
                                            },

                                            {
                                                "name": "LNG",
                                                "val": "101.9026337",
                                                "uom": "degree",
                                                "cts": datetime.utcnow().isoformat()
                                            },

                                            {
                                                "name": "LOCATION_TIME_OFFSET",
                                                "val": "-5",
                                                "uom": "hr",
                                                "cts": datetime.utcnow().isoformat()
                                            },

                                            {
                                                "name": "ODO_READ",
                                                "val": "NULL",
                                                "uom": "NA",
                                                "cts": datetime.utcnow().isoformat()
                                            },

                                            {
                                                "name": "SPDINKM",
                                                "val": s_km,
                                                "uom": "kmph",
                                                "cts": datetime.utcnow().isoformat()
                                            },

                                            {
                                                "name": "SPEED_RATE_OF_CHANGE",
                                                "val": s_change,
                                                "uom": "m/s^2",
                                                "cts": datetime.utcnow().isoformat()
                                            },

                                            {
                                                "name": "SPEED_RATE_OF_CHANGE_POSITIVE",
                                                "val": "FALSE",
                                                "uom": "NA",
                                                "cts": datetime.utcnow().isoformat()
                                            },

                                            {
                                                "name": "SYSTEM_POWER_MODE",
                                                "val": "NULL",
                                                "uom": "NA",
                                                "cts": datetime.utcnow().isoformat()
                                            },
                                        ]     
                                        }     
                                    ]
                                    }
                    return json.dumps(hard_brake_event)

                def HardAcceleration():
                    sp_change = str(round(random.uniform(7, 10)))
                    sp_km = str(round(random.uniform(20, 140)))
                    current_speed1 = random.uniform(20, 140)
                    current_speed_validity1 = "VALID" if current_speed1 < 80 else "INVALID"
                    HardAcceleration = {
                        "tripvehicledata": [
                                { 
                                            "id": id,
                                            "vin": vin,
                                            "PUBLISH_TIME": datetime.utcnow().isoformat(),
                                            "eventCd": "HardAcceleration",
                                            "data": [
                                                {
                                                    "name": "AGEIND",
                                                    "val": age_ind,
                                                    "uom": "NA",
                                                    "cts": datetime.utcnow().isoformat()
                                                    },
                                                {
                                                    "name": "CURRENT_SPEED",
                                                    "val": str(round(current_speed1)),
                                                    "uom": "kmph",
                                                    "cts": datetime.utcnow().isoformat()
                                                    },

                                                    {
                                                    "name": "CURRENT_SPEED_VALIDITY",
                                                    "val": current_speed_validity1,
                                                    "uom": "NA",
                                                    "cts": datetime.utcnow().isoformat()
                                                    },

                                                    {
                                                    "name": "DIR",
                                                    "val": "0",
                                                    "uom": "NA",
                                                    "cts": datetime.utcnow().isoformat()
                                                    },

                                                    {
                                                    "name": "DRIVER_SEATBELT_INDICATOR",
                                                    "val": "NULL",
                                                    "uom": "NA",
                                                    "cts": datetime.utcnow().isoformat()
                                                    },

                                                    {
                                                    "name": "EHPE",
                                                    "val": "0",
                                                    "uom": "NA",
                                                    "cts": datetime.utcnow().isoformat()
                                                    },

                                                    {
                                                    "name": "ELV",
                                                    "val": "1102",
                                                    "uom": "NA",
                                                    "cts": datetime.utcnow().isoformat()
                                                    },

                                                    {
                                                    "name": "ENGINE_RUN_TIME_TOTAL",
                                                    "val": "NULL",
                                                    "uom": "NA",
                                                    "cts": datetime.utcnow().isoformat()
                                                    },

                                                    {
                                                    "name": "ENG_IDLE_RUN_TIME_TOTAL",
                                                    "val": "NULL",
                                                    "uom": "NA",
                                                    "cts": datetime.utcnow().isoformat()
                                                    },

                                                    {
                                                    "name": "ENG_IDL_RUNT_TOTSUP",
                                                    "val": "NULL",
                                                    "uom": "NA",
                                                    "cts": datetime.utcnow().isoformat()
                                                    },

                                                    {
                                                    "name": "ENG_RUNT_PTO_ACT_TOTSUP",
                                                    "val": "NULL",
                                                    "uom": "NA",
                                                    "cts": datetime.utcnow().isoformat()
                                                    },

                                                    {
                                                    "name": "ENG_RUN_TME_PTO_ACT_TOT",
                                                    "val": "NULL",
                                                    "uom": "NA",
                                                    "cts": datetime.utcnow().isoformat()
                                                    },

                                                    {
                                                    "name": "ENG_RUN_TOTSUP",
                                                    "val": "NULL",
                                                    "uom": "NA",
                                                    "cts": datetime.utcnow().isoformat()
                                                    },

                                                    {
                                                    "name": "FUEL_USED",
                                                    "val": "NULL",
                                                    "uom": "NA",
                                                    "cts": datetime.utcnow().isoformat()
                                                    },

                                                    {
                                                    "name": "IGN_CYL",
                                                    "val": "NULL",
                                                    "uom": "NA",
                                                    "cts": datetime.utcnow().isoformat()
                                                    },

                                                    {
                                                    "name": "LAT",
                                                    "val": "35.1532211",
                                                    "uom": "degree",
                                                    "cts": datetime.utcnow().isoformat()
                                                    },

                                                    {
                                                    "name": "LNG",
                                                    "val": "101.9026337",
                                                    "uom": "degree",
                                                    "cts": datetime.utcnow().isoformat()
                                                    },

                                                    {
                                                    "name": "LOCATION_TIME_OFFSET",
                                                    "val": "-5",
                                                    "uom": "hr",
                                                    "cts": datetime.utcnow().isoformat()
                                                    },

                                                    {
                                                    "name": "ODO_READ",
                                                    "val": "NULL",
                                                    "uom": "NA",
                                                    "cts": datetime.utcnow().isoformat()
                                                    },

                                                    {
                                                    "name": "SPDINKM",
                                                    "val": sp_km,
                                                    "uom": "kmph",
                                                    "cts": datetime.utcnow().isoformat()
                                                    },

                                                    {
                                                    "name": "SPEED_RATE_OF_CHANGE",
                                                    "val": sp_change,
                                                    "uom": "m/s^2",
                                                    "cts": datetime.utcnow().isoformat()
                                                    },

                                                    {
                                                    "name": "SPEED_RATE_OF_CHANGE_POSITIVE",
                                                    "val": "TRUE",
                                                    "uom": "NA",
                                                    "cts": datetime.utcnow().isoformat()
                                                    },

                                                    {
                                                    "name": "SYSTEM_POWER_MODE",
                                                    "val": "NULL",
                                                    "uom": "NA",
                                                    "cts": datetime.utcnow().isoformat()
                                                    },
                                            ]
                                            }
                                    ]
                                }
                    return json.dumps(HardAcceleration)

                def DriverSeatbeltChange():
                    dr_st = random.choice(["LATCHED", "UNLATCHED",""])
                    DriverSeatbeltChange = {
                        "tripvehicledata": [
                                { 
                                                "id": id,  
                                                "vin": vin,
                                                "PUBLISH_TIME": datetime.utcnow().isoformat(),
                                                "eventCd": "DriverSeatbeltChange",
                                                "data": [
                                                    {
                                                        "name": "AGEIND",
                                                        "val": age_ind,
                                                        "uom": "NA",
                                                        "cts": datetime.utcnow().isoformat()
                                                        },
                                                    {
                                                        "name": "CURRENT_SPEED",
                                                        "val": "NULL",
                                                        "uom": "NA",
                                                        "cts": datetime.utcnow().isoformat()
                                                        },

                                                        {
                                                        "name": "CURRENT_SPEED_VALIDITY",
                                                        "val": "NULL",
                                                        "uom": "NA",
                                                        "cts": datetime.utcnow().isoformat()
                                                        },

                                                        {
                                                        "name": "DIR",
                                                        "val": "58",
                                                        "uom": "NA",
                                                        "cts": datetime.utcnow().isoformat()
                                                        },

                                                        {
                                                        "name": "DRIVER_SEATBELT_INDICATOR",
                                                        "val": dr_st,
                                                        "uom": "NA",
                                                        "cts": datetime.utcnow().isoformat()
                                                        },

                                                        {
                                                        "name": "EHPE",
                                                        "val": "3",
                                                        "uom": "NA",
                                                        "cts": datetime.utcnow().isoformat()
                                                        },

                                                        {
                                                        "name": "ELV",
                                                        "val": "1111",
                                                        "uom": "NA",
                                                        "cts": datetime.utcnow().isoformat()
                                                        },

                                                        {
                                                        "name": "ENGINE_RUN_TIME_TOTAL",
                                                        "val": "NULL",
                                                        "uom": "NA",
                                                        "cts": datetime.utcnow().isoformat()
                                                        },

                                                        {
                                                        "name": "ENG_IDLE_RUN_TIME_TOTAL",
                                                        "val": "NULL",
                                                        "uom": "NA",
                                                        "cts": datetime.utcnow().isoformat()
                                                        },

                                                        {
                                                        "name": "ENG_IDL_RUNT_TOTSUP",
                                                        "val": "NULL",
                                                        "uom": "NA",
                                                        "cts": datetime.utcnow().isoformat()
                                                        },

                                                        {
                                                        "name": "ENG_RUNT_PTO_ACT_TOTSUP",
                                                        "val": "NULL",
                                                        "uom": "NA",
                                                        "cts": datetime.utcnow().isoformat()
                                                        },

                                                        {
                                                        "name": "ENG_RUN_TME_PTO_ACT_TOT",
                                                        "val": "NULL",
                                                        "uom": "NA",
                                                        "cts": datetime.utcnow().isoformat()
                                                        },

                                                        {
                                                        "name": "ENG_RUN_TOTSUP",
                                                        "val": "NULL",
                                                        "uom": "NA",
                                                        "cts": datetime.utcnow().isoformat()
                                                        },

                                                        {
                                                        "name": "FUEL_USED",
                                                        "val": "NULL",
                                                        "uom": "NA",
                                                        "cts": datetime.utcnow().isoformat()
                                                        },

                                                        {
                                                        "name": "IGN_CYL",
                                                        "val": "NULL",
                                                        "uom": "NA",
                                                        "cts": datetime.utcnow().isoformat()
                                                        },

                                                        {
                                                        "name": "LAT",
                                                        "val": "35.1532211",
                                                        "uom": "degree",
                                                        "cts": datetime.utcnow().isoformat()
                                                        },

                                                        {
                                                        "name": "LNG",
                                                        "val": "101.9026337",
                                                        "uom": "degree",
                                                        "cts": datetime.utcnow().isoformat()
                                                        },

                                                        {
                                                        "name": "LOCATION_TIME_OFFSET",
                                                        "val": "-5",
                                                        "uom": "hr",
                                                        "cts": datetime.utcnow().isoformat()
                                                        },

                                                        {
                                                        "name": "ODO_READ",
                                                        "val": "NULL",
                                                        "uom": "NA",
                                                        "cts": datetime.utcnow().isoformat()
                                                        },

                                                        {
                                                        "name": "SPDINKM",
                                                        "val": "0",
                                                        "uom": "kmph",
                                                        "cts": datetime.utcnow().isoformat()
                                                        },

                                                        {
                                                        "name": "SPEED_RATE_OF_CHANGE",
                                                        "val": "NULL",
                                                        "uom": "NA",
                                                        "cts": datetime.utcnow().isoformat()
                                                        },

                                                        {
                                                        "name": "SPEED_RATE_OF_CHANGE_POSITIVE",
                                                        "val": "NULL",
                                                        "uom": "NA",
                                                        "cts": datetime.utcnow().isoformat()
                                                        },

                                                        {
                                                        "name": "SYSTEM_POWER_MODE",
                                                        "val": "NULL",
                                                        "uom": "NA",
                                                        "cts": datetime.utcnow().isoformat()
                                                        },
                                                ]
                                                }
                                        ]
                                    }
                    return json.dumps(DriverSeatbeltChange)

                def TripEnd():
                    # odo_read = str(round(random.uniform(0, 50000)))#add odoread of tripstart+random(300,2000)
                    odo_read1 = round(odo_read + random.uniform(300,2000))
                    TripEnd = {
                        "tripvehicledata": [
                                {
                                        "id": id,
                                        "vin": vin,
                                        "PUBLISH_TIME": datetime.utcnow().isoformat(),
                                        "eventCd": "TripEnd",
                                        "data": [
                                            {
                                                "name": "AGEIND",
                                                "val": age_ind,
                                                "uom": "NA",
                                                "cts": datetime.utcnow().isoformat()
                                                },
                                            {
                                                "name": "CURRENT_SPEED",
                                                "val": "NULL",
                                                "uom": "NA",
                                                "cts": datetime.utcnow().isoformat()
                                                },

                                                {
                                                "name": "CURRENT_SPEED_VALIDITY",
                                                "val": "NULL",
                                                "uom": "NA",
                                                "cts": datetime.utcnow().isoformat()
                                                },

                                                {
                                                "name": "DIR",
                                                "val": "128",
                                                "uom": "NA",
                                                "cts": datetime.utcnow().isoformat()
                                                },

                                                {
                                                "name": "DRIVER_SEATBELT_INDICATOR",
                                                "val": "UNLATCHED",
                                                "uom": "NA",
                                                "cts": datetime.utcnow().isoformat()
                                                },

                                                {
                                                "name": "EHPE",
                                                "val": "2",
                                                "uom": "NA",
                                                "cts": datetime.utcnow().isoformat()
                                                },

                                                {
                                                "name": "ELV",
                                                "val": "1108",
                                                "uom": "NA",
                                                "cts": datetime.utcnow().isoformat()
                                                },

                                                {
                                                "name": "ENGINE_RUN_TIME_TOTAL",
                                                "val": "1611197",
                                                "uom": "sec",
                                                "cts": datetime.utcnow().isoformat()
                                                },

                                                {
                                                "name": "ENG_IDLE_RUN_TIME_TOTAL",
                                                "val": "661513",
                                                "uom": "sec",
                                                "cts": datetime.utcnow().isoformat()
                                                },

                                                {
                                                "name": "ENG_IDL_RUNT_TOTSUP",
                                                "val": "TRUE",
                                                "uom": "NA",
                                                "cts": datetime.utcnow().isoformat()
                                                },

                                                {
                                                "name": "ENG_RUNT_PTO_ACT_TOTSUP",
                                                "val": "FALSE",
                                                "uom": "NA",
                                                "cts": datetime.utcnow().isoformat()
                                                },

                                                {
                                                "name": "ENG_RUN_TME_PTO_ACT_TOT",
                                                "val": "0",
                                                "uom": "sec",
                                                "cts": datetime.utcnow().isoformat()
                                                },

                                                {
                                                "name": "ENG_RUN_TOTSUP",
                                                "val": "TRUE",
                                                "uom": "NA",
                                                "cts": datetime.utcnow().isoformat()
                                                },

                                                {
                                                "name": "FUEL_USED",
                                                "val": "2467.21875",
                                                "uom": "L",
                                                "cts": datetime.utcnow().isoformat()
                                                },

                                                {
                                                "name": "IGN_CYL",
                                                "val": "OFF",
                                                "uom": "NA",
                                                "cts": datetime.utcnow().isoformat()
                                                },

                                                {
                                                "name": "LAT",
                                                "val": "35.1532211",
                                                "uom": "degree",
                                                "cts": datetime.utcnow().isoformat()
                                                },

                                                {
                                                "name": "LNG",
                                                "val": "101.9026337",
                                                "uom": "degree",
                                                "cts": datetime.utcnow().isoformat()
                                                },

                                                {
                                                "name": "LOCATION_TIME_OFFSET",
                                                "val": "-5",
                                                "uom": "hr",
                                                "cts": datetime.utcnow().isoformat()
                                                },

                                                {
                                                "name": "ODO_READ",
                                                "val": odo_read1,
                                                "uom": "km",
                                                "cts": datetime.utcnow().isoformat()
                                                },

                                                {
                                                "name": "SPDINKM",
                                                "val": "0",
                                                "uom": "kmph",
                                                "cts": datetime.utcnow().isoformat()
                                                },

                                                {
                                                "name": "SPEED_RATE_OF_CHANGE",
                                                "val": "NULL",
                                                "uom": "NA",
                                                "cts": datetime.utcnow().isoformat()
                                                },

                                                {
                                                "name": "SPEED_RATE_OF_CHANGE_POSITIVE",
                                                "val": "NULL",
                                                "uom": "NA",
                                                "cts": datetime.utcnow().isoformat()
                                                },

                                                {
                                                "name": "SYSTEM_POWER_MODE",
                                                "val": "OFF",
                                                "uom": "NA",
                                                "cts": datetime.utcnow().isoformat()
                                                },
                                                ]
                                        }
                                ]
                            }
                    return json.dumps(TripEnd)
                    
                event_batch = producer_client.create_batch()

                event_data = EventData(generate_trip_start_event())
                event_batch.add(event_data)

                #print("Trip start Event sent to Event Hub:", generate_trip_start_event())

                num_of_events1 = random.randint(1, 4)
                for i in range(num_of_events1):
                    num_of_events = random.randint(1, 3)
                    if(num_of_events==1):
                        event_data = EventData(generate_hard_brake_event())
                        event_batch.add(event_data)
                        #print("Message written to file:", generate_hard_brake_event())
                    elif(num_of_events==2):
                        event_data = EventData(HardAcceleration())
                        event_batch.add(event_data)
                        #print("Message written to file:", HardAcceleration())
                    else:
                        event_data = EventData(DriverSeatbeltChange())
                        event_batch.add(event_data)
                        #print("Message written to file:", DriverSeatbeltChange())

                event_data = EventData(TripEnd())
                event_batch.add(event_data)
            
                with producer_client:
                        producer_client.send_batch(event_batch)

                #print("Trip End Event sent to Event Hub:", TripEnd())

    except KeyboardInterrupt:
            print("Data generation stopped.")
    except Exception as e:
            print("Error:", e)
    finally:
            pass
    
num_threads = 500
threads = []

for _ in range(num_threads):
    thread = threading.Thread(target=generate_data_and_send)
    threads.append(thread)
    thread.start()

# Wait for all threads to finish
for thread in threads:
    thread.join()