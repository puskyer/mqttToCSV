#!/usr/bin/python3
# -*- coding: utf-8 -*-

# import the module
import python_weather
import asyncio

import json
import random
import time
import smtplib
import ssl
from typing import TextIO
from paho.mqtt import client as mqtt_client
import csv
import datetime
import pandas as pd
import ftplib
from copy import deepcopy

# config.json data file
data_file: TextIO
with open('config.json', "r") as data_file:
    DataJson = json.load(data_file)
data_file.close()

broker = DataJson["mqtt"]["broker"]
port = DataJson["mqtt"]["port"]
username = DataJson["mqtt"]["mqtt_user"]
password = DataJson["mqtt"]["mqtt_password"]
client_id = f'mqtt-{random.randint(0, 1000)}'
mqttJson = {
    "CCS811": {
        "UnitC": "C",
        "UnitF": "F",
        "PressureUnit": "hPa",
        "ESP32TemperatureC": 0.0,
        "ESP32TemperatureF": 0.0,
        "SI7021TemperatureC": 0.0,
        "SI7021TemperatureF": 0.0,
        "SI7021Humitity": 0.0,
        "SI7021Dewpoint": 0.0,
        "BMP280TemperatureC": 0.0,
        "BMP280TemperatureF": 0.0,
        "BMP280Pressure": 0.0,
        "eCO2": 0,
        "TVOC": 0,
        "Device Date": "devdate",
        "Device Time": "devtime",
        "Date": "thedate",
        "Time": "thetime"
        },
    "weather": {
            "timezone_offset": "-5",
            "Temperature": 0.0,
            "degree_type": "C",
            "TemperatureF": 0.0,
            "UnitF": "F",
            "feels_like": 0.0,
            "feels_like_conv": 0.0,
            "humidity": 0,
            "wind_display": "",
            "sky_text": "",
            "date": {               # datetime.datetime(2021, 11, 17, 19, 15)
                "year": 0000,
                "month": 00,
                "day": 00,
                "hour": 00,
                "minutes": 00
            },
            "day": " ",
            "observation_point": ""
        }
}

# mosquitto_sub -v  -h mqtt.lan -u mqttUser -P MqttPass  -t fireplacefan/tele/SENSOR
# PowR2/tele/SENSOR = {"Time":"2021-11-17T18:08:34","ENERGY":{"TotalStartTime":"2021-11-17T16:12:22","Total":0.734,
# "Yesterday":0.000,"Today":0.734,"Period": 0,"Power": 0,"ApparentPower": 0,"ReactivePower": 0,"Factor":0.00,
# "Voltage":119,"Current":0.000}}

# fireplacefan/tele/SENSOR = {"Time":"2021-11-17T17:13:36","DS18B20":{"Id":"05167219F3FF","Temperature":32.3},"TempUnit":"C"}

topic_fp_SENSOR = "CCS811/tele/SENSOR"
topic_sub = [(topic_fp_SENSOR, 1)]

topic_pub_POWER = b'CCS811/cmnd/POWER'
topic_pub_STATUS = "CCS811/cmnd/STATUS"
topic_pub = "CCS811/stat/python"
last_time_check = 0

#Function to send files to a FTP Server
def send_to_ftp (csv):
    host = '127.0.0.1'          #Define host
    username = 'ftpuser'        #Username for server
    passwd = 'owl'              #Password for server

    ftp_server = ftplib.FTP(host, username, passwd)     #Define FTP Server
    ftp_server.encoding = "utf-8"                       #Define type of data encoding of network

    testFile = open(csv, 'rb')                          #Open file

    ftp_server.storbinary("STOR %s" %csv, testFile)     #Store file data in FTP Server

    testFile.close()            #Close file
    ftp_server.quit()           #End user session


def cross_join(left, right):
    new_rows = [] if right else left
    for left_row in left:
        for right_row in right:
            temp_row = deepcopy(left_row)
            for key, value in right_row.items():
                temp_row[key] = value
            new_rows.append(deepcopy(temp_row))
    return new_rows


def flatten_list(data):
    for elem in data:
        if isinstance(elem, list):
            yield from flatten_list(elem)
        else:
            yield elem


def json_to_dataframe(data_in):
    def flatten_json(data, prev_heading=''):
        if isinstance(data, dict):
            rows = [{}]
            for key, value in data.items():
                rows = cross_join(rows, flatten_json(value, prev_heading + '.' + key))
        elif isinstance(data, list):
              rows = []
              for item in data:
                [rows.append(elem) for elem in flatten_list(flatten_json(item, prev_heading))]
        else:
                  rows = [{prev_heading[1:]: data}]
        return rows

    return pd.DataFrame(flatten_json(data_in))

async def getweather():
    # declare the client. format defaults to the metric system (celcius, km/h, etc.)
    weatherclient = python_weather.Client(format=python_weather.METRIC)

    # fetch a weather forecast from a city
    weather = await weatherclient.find("Winchester Ontario")

    mqttJson["weather"]["timezone_offset"] = weather.timezone_offset

    if weather.degree_type == "C":
        mqttJson["weather"]["Temperature"] = weather.current.temperature
        mqttJson["weather"]["degree_type"] = weather.degree_type
        # convert to fahrenheit
        mqttJson["weather"]["TemperatureF"] = round(((mqttJson["weather"]["Temperature"] * 1.8) + 32), 2)
        mqttJson["weather"]["UnitF"] = "F"
        mqttJson["weather"]["feels_like"] = str(weather.current.feels_like) + " " + weather.degree_type + " / " + str(round(((weather.current.feels_like * 1.8) + 32), 2)) + " F"
    else:
        mqttJson["weather"]["TemperatureF"] = weather.current.temperature
        mqttJson["weather"]["UnitF"] = weather.degree_type
        # covert to celsius
        mqttJson["weather"]["Temperature"] = round(((mqttJson["weather"]["Temperature"] - 32) / 1.8), 2)
        mqttJson["weather"]["degree_type"] = "C"
        mqttJson["weather"]["feels_like"] = str(round(((weather.current.feels_like - 32) / 1.8), 2)) + " F / " + str(weather.current.feels_like) + " " + weather.degree_type

    mqttJson["weather"]["humidity"] = weather.current.humidity
    mqttJson["weather"]["wind_display"] = weather.current.wind_display
    mqttJson["weather"]["sky_text"] = weather.current.sky_text
    mqttJson["weather"]["date"]["year"] = weather.forecasts[0].date.year
    mqttJson["weather"]["date"]["month"] = weather.forecasts[0].date.month
    mqttJson["weather"]["date"]["day"] = weather.forecasts[0].date.day
    mqttJson["weather"]["date"]["hour"] = weather.forecasts[0].date.hour
    mqttJson["weather"]["date"]["minutes"] = weather.forecasts[0].date.minute
    mqttJson["weather"]["day"] = weather.current.day
    mqttJson["weather"]["observation_point"]  = weather.current.observation_point

    # close the wrapper once done
    await weatherclient.close()

def sendemail(subject, text):
    to = DataJson["email"]["To"]
    # Gmail Sign In
    smtp_sender = DataJson["email"]["From"]
    smtp_passwd = DataJson["email"]["Password"]
    smtp_server = DataJson["email"]["SMTPServer"]
    smtp_port = DataJson["email"]["SMTPPort"]
    body = '\r\n'.join(['To: %s' % to,
                        'From: %s' % smtp_sender,
                        'Subject: %s' % subject,
                        '', text])

    context = ssl.create_default_context()
    with smtplib.SMTP(smtp_server, smtp_port) as server:
        #server.set_debuglevel(1)
        server.ehlo()
        server.starttls(context=context)
        server.ehlo()
        server.login(smtp_sender, smtp_passwd)
        try:
            server.sendmail(smtp_sender, [to], body)
            print('email sent')
        except:
            print('error sending mail')
        server.quit()

def connect_mqtt():
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    # Set Connecting Client ID
    mqttclient = mqtt_client.Client(client_id)
    mqttclient.username_pw_set(username, password)
    mqttclient.on_connect = on_connect
    mqttclient.connect(broker, port)
    return mqttclient

def subscribe(client: mqtt_client):
    reply = client.subscribe(topic_sub)
    print(reply)

# def publish(client: mqtt_client):
#    client.publish(topic, msg)
#    client.connect(broker, port)
#    return client

def publish(client: mqtt_client):
    time.sleep(1)
    result = client.publish(topic, msg)
    # result: [0, 1]
    status = result[0]
    if status == 0:
       print(f"Send `{msg}` to topic `{topic}`")
    else:
       print(f"Failed to send message to topic {topic}")

# 22:30:18.781 MQT: CCS811/tele/SENSOR = {
# "Time":"2022-07-29T22:30:18",
# "SI7021":{"Temperature":24.2,"Humidity":59.4,"DewPoint":15.8},
# "BMP280":{"Temperature":23.1,"Pressure":709.1},
# "CCS811":{"eCO2":5170,"TVOC":7486},
# "ESP32":{"Temperature":41.1},
# "PressureUnit":"hPa",
# "TempUnit":"C"}

def on_message(client, userdata, msg):
    global last_time_check
    tempmqttdata = json.loads(msg.payload.decode())
    csvmsg = msg.payload.decode('utf-8')                        #Decode MQTT Message
    
    print(f"Received `{csvmsg}` from `{msg.topic}` topic")

    if time.localtime().tm_hour != last_time_check:
        try:
            loop = asyncio.get_event_loop()
            loop.run_until_complete(getweather())
            last_time_check = time.localtime().tm_hour
        except:
            print("Get Weather failed!")
            last_time_check = time.localtime().tm_hour

    if msg.topic == topic_fp_SENSOR:
        mytime = time.localtime()
        mqttJson["CCS811"]["Date"] = str(mytime[0]) + '/' + str(mytime[1]) + '/' + str(mytime[2])
        mqttJson["CCS811"]["Time"] = str(mytime[3]) + ':' + str(mytime[4]) + ':' + str(mytime[5])
        tempdate = tempmqttdata['Time']
        tempSdate = tempdate.split("T")
        mqttJson["CCS811"]["Device Date"] = tempSdate[0]
        mqttJson["CCS811"]["Device Time"] = tempSdate[1]
        if tempmqttdata['TempUnit'] == "C":
            mqttJson["CCS811"]["SI7021TemperatureC"] = tempmqttdata['SI7021']['Temperature']
            mqttJson["CCS811"]["BMP280TemperatureC"] = tempmqttdata['BMP280']['Temperature'] 
            mqttJson["CCS811"]["ESP32TemperatureC"] = tempmqttdata['ESP32']['Temperature']            
            mqttJson["CCS811"]["Unit"] = tempmqttdata['TempUnit']
            # convert to fahrenheit
            mqttJson["CCS811"]["SI7021TemperatureF"] = round(((tempmqttdata['SI7021']['Temperature'] * 1.8) + 32), 2)
            mqttJson["CCS811"]["BMP280TemperatureF"] = round(((tempmqttdata['BMP280']['Temperature'] * 1.8) + 32), 2)
            mqttJson["CCS811"]["ESP32TemperatureF"] = round(((tempmqttdata['ESP32']['Temperature'] * 1.8) + 32), 2)
            mqttJson["CCS811"]["UnitF"] = "F"
        else:
            mqttJson["CCS811"]["SI7021TemperatureF"] = tempmqttdata['SI7021']['Temperature']
            mqttJson["CCS811"]["BMP280TemperatureF"] = tempmqttdata['BMP280']['Temperature'] 
            mqttJson["CCS811"]["ESP32TemperatureF"] = tempmqttdata['ESP32']['Temperature']
            mqttJson["CCS811"]["UnitF"] = tempmqttdata['TempUnit']
            # covert to celsius
            mqttJson["CCS811"]["SI7021TemperatureC"] = round(((tempmqttdata['SI7021']['Temperature'] - 32) / 1.8), 2)
            mqttJson["CCS811"]["BMP280TemperatureC"] = round(((tempmqttdata['BMP280']['Temperature'] - 32) / 1.8), 2)
            mqttJson["CCS811"]["ESP32TemperatureC"] = round(((tempmqttdata['ESP32']['Temperature'] - 32) / 1.8), 2)
            mqttJson["CCS811"]["Unit"] = "C"
        mqttJson["CCS811"]["SI7021Humidity"] = tempmqttdata['SI7021']['Humidity']
        mqttJson["CCS811"]["SI7021DewPoint"] = tempmqttdata['SI7021']['DewPoint']
        mqttJson["CCS811"]["BMP280Pressure"] = tempmqttdata['BMP280']['Pressure']
        mqttJson["CCS811"]["eCO2"] = tempmqttdata['CCS811']['eCO2']
        mqttJson["CCS811"]["TVOC"] = tempmqttdata['CCS811']['TVOC']
        mqttJson["CCS811"]["PressureUnit"] = tempmqttdata['PressureUnit']
        
         
        # fahrenheit = (celsius * 1.8) + 32
        # celsius = (fahrenheit - 32) / 1.8

    JsonMqtt = json.dumps(mqttJson)
    # config.json data file
    data_file: TextIO
    with open('CCS811.json', "a") as data_file:
        data_file.write(JsonMqtt)
        data_file.write(",")
        data_file.close()
        
    msgDict = eval(csvmsg)                              #Convert MQTT Message to dictionary                               

    print(f"Dicionary is  `{msgDict}`")
    print ("Type: ", type(msgDict), "\n")               #Debug to see new message type
    
    #Try to write the dictonary to a csv file
    try:
        print ("start")
        df = json_to_dataframe(msgDict)                            #Define the dataframes
        print(df)
        df.to_csv(dateTime, index = False, header =  True)              #Write dataframes to csv
        # send = send_to_ftp(dateTime)                                    #Send the csv file to a FTPS
        print ("Recorded in CSV file: ", dateTime)                      #Confirming message is sent
    #Excepting to handle the wrong data type
    except ValueError as ve:
        print("Wrong type")    

    # if int( mqttJson["TH16"]["TemperatureC"]) != last_temp or (mqttJson["PowR2"]["PowR2 State"] == "ON" and PowR2EmailOnce == 1):
    #    last_temp = int( mqttJson["TH16"]["TemperatureC"])
    #    text = '\r\n'.join(['Weather in %s ' % mqttJson["weather"]["observation_point"],
    #                        'Date / Time %s ' % time.asctime(),
    #                        'Humidity is %s' % mqttJson["weather"]["humidity"],
    #                        'Possibility of %s' % mqttJson["weather"]["sky_text"],
    #                        'Wind is %s' % mqttJson["weather"]["wind_display"],
    #                        'Temperature is %s %s / %s %s' % (mqttJson["weather"]["Temperature"], mqttJson["weather"]["degree_type"], mqttJson["weather"]["TemperatureF"], mqttJson["weather"]["UnitF"]),
    #                        'Feels Like %s ' % mqttJson["weather"]["feels_like"],
    #                        'Hot Tub Temperature is %s  %s / %s %s' % (mqttJson["TH16"]["TemperatureC"], mqttJson["TH16"]["Unit"], mqttJson["TH16"]["TemperatureF"], mqttJson["TH16"]["UnitF"]),
    #                        'TH16 Date is %s' % mqttJson["TH16"]["Device Date"],
    #                        'TH16 Time is %s' % mqttJson["TH16"]["Device Time"]
    #                        ])
    # sendemail("not sure", text)


currentDateTime = datetime.datetime.now()                           #Get current date and time
dateTime = str(currentDateTime.strftime("%Y%m%dT%H%M%S"))+'.csv'    #Create filename for csv
client = connect_mqtt()
subscribe(client)
print("Connected to MQTT broker " + str(broker) + " subscribed to " + str(topic_sub) + " topic ")
client.on_message = on_message
client.loop_forever()


