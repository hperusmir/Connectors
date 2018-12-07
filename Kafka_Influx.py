import ujson as json
import dpath
from influxdb import InfluxDBClient
from confluent_kafka import Consumer, KafkaError
import re
import time

class Vividict(dict):
    def __missing__(self, key):
        value = self[key] = type(self)()
        return value
###################################################################################
# Define the functional Parameters, for Data Collection, Translation and Delivery #
###################################################################################

topic = "SinglePart_PerfTest"
group = "Python_Consumer_Group"
servers = 'iotaasnip2:9092'
client = "Confluent_Consumer1"
KeyCollections=['deveui','snr','live','fcnt','port','rssi','freq','dr_used','gtw_id','cr_user','timestamp','time_on_air_ms','device_redundancy','id']

settings = {
    'bootstrap.servers': servers,
    'group.id': group,
    'client.id': client,
    'enable.auto.commit': True,
    'session.timeout.ms': 6000,
    'default.topic.config': {'auto.offset.reset': 'smallest'}
}


KCons = Consumer(settings)

print ("Starting the Process")
process_start=time.time()
total_time=0
KCons.subscribe([topic])
try:
    while True:
        msg = KCons.poll()
        if msg is None:
            continue
        elif not msg.error():
            try:
                msg_proc_start = time.time()
                Device=""
                Message_ID=""
                Device_Dict=Vividict()
                GW_Dict=Vividict()
                json_m = json.loads(msg.value())
                string_m = str(msg.value())
                Database=str(json_m["userid"])
                no_gateways = string_m.count('gtw_id')
                Customer = str(json_m["userid"])
                Group = str(json_m["groupid"])
                InfluxClient = InfluxDBClient('localhost', 8086, 'root', 'root', Database)
                InfluxClient.create_database(Database)
                #print str(json_m)
                for Key in KeyCollections:
                    for element in dpath.util.search(json_m, '**/' + Key, yielded=True):
                        if "gtw_info" not in element[0]:
                            Keys=element[0].split("/")
                            KeyLabel=Keys[len(Keys)-1].encode("utf-8")
                            dev_fields_dict=Vividict()
                            if KeyLabel == "deveui":
                                Device = str(element[1])
                            for Parent in Keys[0:len(Keys)-1]:
                                Parent = Parent.encode("utf-8")
                                Value = json_m[Parent][Key]
                                #print ("Parent", Parent , "Value", Value)
                                dev_fields_dict[str(Key)] = Value
                                if Key == "id":
                                    Message_ID = Value
                                if Key == "dr_used":
                                    tnumbers = re.findall(r"[-+]?\d*\.\d+|\d+", Value)
                                    tstrings = re.findall(r"[aA-zZ]+", Value)
                                    for index, member in enumerate(tstrings):
                                        tempkey = member
                                        tempvalue = tnumbers[index]
                                        dev_fields_dict[str(tempkey)] = tempvalue
                            Device_Dict[Device].update(dev_fields_dict)
                            Device_Dict[Device]["nb_gateways"] = no_gateways
                            Device_Dict[Device]["msg_id"] = Message_ID
                        if "gtw_info" in element[0]:
                            gw_fields_dict = Vividict()
                            #print "Entering a GW area"
                            Keys=element[0].split("/")
                            KeyLabel=Keys[len(Keys)-1].encode("utf-8")
                            GW_ID_PATH=Keys[0:-1]
                            #print "GW PATH IS IS:" , GW_ID_PATH
                            #print "Keys", str(Keys) , "Label", KeyLabel
                            Parent_Path='/'.join(Keys)
                            #GW_Dict["tags"]["Group"] = str(json_m["groupid"])
                            #GW_Dict["tags"]["Customer"] = str(json_m["userid"])
                            GTW_ID = dpath.util.get(json_m, str('/'.join(Keys[0:-1]))+"/gtw_id" )
                            #print "GTW ID IS" , GTW_ID
                            #print "PARENT" , str(Parent_Path)
                            if KeyLabel != "gtw_id":
                                gw_fields_dict[str(KeyLabel)] = dpath.util.get(json_m, str(Parent_Path))
                                #print "ADDED" , str(gw_fields_dict)
                                GW_Dict[str(GTW_ID)].update(gw_fields_dict)
                #print str(Device_Dict)
                GW_Influx=Vividict()
                Device_Influx=Vividict()

                for Gateway in GW_Dict:
                    #print Gateway
                    GW_Influx["measurement"]=Group+"_GatewayReadings"
                    GW_Influx["tags"]["Customer"]=Customer
                    GW_Influx["tags"]["gtw_id"]= Gateway
                    for field in GW_Dict[Gateway]:
                        GW_Influx["fields"][field]=GW_Dict[Gateway][field]
                        GW_Influx["fields"]["msg_id"] = Message_ID
                    InfluxClient.write_points([GW_Influx])

                for Device in Device_Dict:
                    #print Device
                    Device_Influx["measurement"]=Group+"_DeviceReadings"
                    Device_Influx["tags"]["Customer"]=Customer
                    Device_Influx["tags"]["Device"]= Device
                    for field in Device_Dict[Device]:
                        Device_Influx["fields"][field]=Device_Dict[Device][field]
                    #print str(Device_Influx)
                    #print("total time taken to process message: ", time.time() - msg_proc_start)
                    write_start=time.time()
                    InfluxClient.write_points([Device_Influx])
                    #print("total time to write into Influx: ", time.time() - write_start)
                    total_time+=time.time()-msg_proc_start
            except Exception,e:
                print "Not a valid Message. " + str(e) + str(msg.value())
                pass
        elif msg.error().code() == KafkaError._PARTITION_EOF:
            print('End of partition reached {0}/{1}'
                 .format(msg.topic(), msg.partition()))
        else:
            print('Error occured: {0}'.format(msg.error().str()))
except KeyboardInterrupt:
        pass

finally:
    KCons.close()
print ("Processing everything took:  " , total_time )
