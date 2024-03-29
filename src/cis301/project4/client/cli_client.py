#TODO Implement the client application
import json

import requests

from cis301.project4.phonecall import PhoneCall
from cis301.project4.util import Util


class PhoneBillClient():

    def __init__(self, hostname="localhost", port="8000", username, password):
        self.__host = hostname
        self.__port = port
        self.__uname = username
        self.__password = password


    def get_username(self):
        return self.__uname

    def get_password(self):
        return self.__password

    def set_username(self, uname):
         self.__uname = uname

    def set_password(self, passwd):
         self.__password = passwd

    def register_user(self):
        pass


    def add_phonecall(self, phonecall):
        # convert data to JSON
        phonecallJSON = Util.phonecallToJSON(phonecall, True)
        #generate a request
        #every request needs authentication
        url = 'http://' + self.__host + ':' + self.__port + '/auth'
        data = {"email":f"{self.__uname}", "password":f"{self.__password}", "client":True}

        headers={'content-type':'application/json',}
        auth_res = requests.post( url, data=json.dumps(data), headers= headers )

        # check response
        print( auth_res )
        print( auth_res.text )
        # send request (POST)
        url = 'http://'+self.__host+':'+self.__port+'/user/add'
        res = requests.post(url, data=json.dumps(phonecallJSON), cookies=auth_res.cookies,headers=headers )

        #check response
        print(res)
        print( res.text )
        if json.loads(res.text)['res']== "200":
            print(f'Phonecall ADDED: {phonecall.__str__()}')
        else:
            print(f'ERROR: could not add new record \n\t{json.loads(res.text)["res"]}')

    def del_phonecall(self, phonecall_id):
        # generate a request
        # every request needs authentication
        url = 'http://' + self.__host + ':' + self.__port + '/auth'
        data = {"email": f"{self.__uname}", "password": f"{self.__password}", "client": True}

        headers = {'content-type': 'application/json', }
        auth_res = requests.post(url, data=json.dumps(data), headers=headers)

        # check response
        print(auth_res)
        print(auth_res.text)
        # send request (POST)
        url = 'http://' + self.__host + ':' + self.__port + '/user/delete'
        res = requests.post(url, data=json.dumps({"phonecall_id":str(phonecall_id)}), cookies=auth_res.cookies, headers=headers)

        if json.loads(res.text)['res'] == "200":
            print(f'Phonecall {phonecall_id} DELETED')
        else:
            print(f'ERROR: could not delete record {phonecall_id} \n\t{json.loads(res.text)["res"]}')
        # check response
        print(res)
        print(res.text)
    def update_phonecall(self, phonecall_id):
        #convert to JSON
        phonecallJSON = Util.phonecallToJSON(phonecall, True)
        #generate request
        # every request needs authentication
        url = 'http://' + self.__host + ':' + self.__port + '/auth'
        data = {"email": f"{self.__uname}", "password": f"{self.__password}", "client": True}

        headers = {'content-type': 'application/json', }
        auth_res = requests.post(url, data=json.dumps(data), headers=headers)
        # check response
        print(auth_res)
        print(auth_res.text)
        #HOW TO UPDATE HERE !!!!!!!!!
        # send request (POST)
        url = 'http://' + self.__host + ':' + self.__port + '/user/update'
        res = requests.post(url, data=json.dumps({"phonecall_id": str(phonecall_id)}), cookies=auth_res.cookies,
                            headers=headers)
        #to here !!!!!!
        if json.loads(res.text)['res']== "200":
            print(f'Phonecall UPDATED: {phonecall.__str__()}')
        else:
            print(f'ERROR: could not update record {phonecall_id} \n\t{json.loads(res.text)["res"]}')




if __name__== '__main__':
    username = "morgan@cau.edu"
    password = "123456"
    phonecall = PhoneCall('404-880-4567', '404-880-9632', '11/11/2020 15:10', '11/11/2020 15:25')
    pbc = PhoneBillClient()
    pbc.set_username(username)
    pbc.set_password(password)
    pbc.add_phonecall(phonecall)
    pbc.del_phonecall(phonecall_id)
    pbc.update_phonecall(phonecall_id)


