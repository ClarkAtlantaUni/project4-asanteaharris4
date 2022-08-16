import sqlite3
import uuid
from hashlib import md5

from cis301.phonebill.phonebill_doa import AbstractPhoneBill_DOA
from cis301.phonebill.phonecall_doa import AbstractPhoneCall_DOA

# TODO Finish Implementing the abstract methods
from cis301.project4 import phonecall
from cis301 import phonebill


class PhoneBill_DOA(AbstractPhoneBill_DOA, AbstractPhoneCall_DOA):
    def __init__(self, dbfile):
        self.dbfile = dbfile

    def insert_phonebill(self, phonebill):
        conn = sqlite3.connect(self.dbfile)
        c = conn.cursor()           #establishes connection
        data = (phonebill.get_phonecalls(), phonebill.get_callee(), phonebill.get_starttime_string(),
                phonebill.get_endtime_string())

        c.execute('INSERT INTO phonebills (caller,callee, startDate, endDate) VALUES (?,?,?)', data)
        conn.commit()
        pid = c.lastrowid
        data = (phonebill.get_uid, pid)
        c.execute('INSERT INTO phonebills(uid,pid) VALUES(?,?);', data)
        conn.commit()
        conn.close()
        return pid




    def update_phonebill(self, phonebill):
        conn = sqlite3.connect(self.dbfile) #establishes connection
        c = conn.cursor()
        data = (phonebill.get_customer(), phonebill.get_phonecalls())  # query rows in phonebill table
        #from sqltutorial.net understanding
        sql = """UPDATE phonebill
                SET customername= , phonecall=
                WHERE uid=
                """
        c.execute(sql, phonebill) #where do i pass it the data
        conn.commit()
        conn.close()
    def delete_phonebill(self, phonebill):
        conn = sqlite3.connect(self.dbfile) #establishes connection
        c = conn.cursor()
        data = (phonebill.get_customer(), phonebill.add_phonecall() ,phonebill.get_phonecalls())  # all rows in phonebill table
        sql= 'DELETE FROM phonebills WHERE uuid= '
        c.execute(sql,data) #tried passing data instead of phonebill here
        conn.commit()
        conn.close()
    def select_phonebill(self, phonebill_id):
        conn = sqlite3.connect(self.dbfile) #establishes connection
        c = conn.cursor()
        data= (phonebill_id.get_customer(), phonebill_id.add_phonecall(), phonebill_id.get_phonecalls())    #all rows in phonebill table
        c.execute('SELECT * FROM phonebill', data)
        conn.commit()
        conn.close()

    def search_phonebills_bydate(self, startdate, enddate):
        conn = sqlite3.connect(self.dbfile)  #establishes connection
        c = conn.cursor()
        #from hybrid understanding of w3schools & jerniyah
        c.execute= ('SELECT * FROM phonebill WHERE startdate AND enddate BETWEEN startdate AND enddate')
        result = c.fetchall() #fetch all rows from the table
        conn.commit()
        conn.close()
        return result

    def insert_phonecall(self, phonecall):
        conn = sqlite3.connect( self.dbfile )
        c = conn.cursor()
        data = (phonecall.get_caller(), phonecall.get_callee(), phonecall.get_starttime_string(), phonecall.get_endtime_string())
        c.execute( 'INSERT INTO phonecalls ( caller,callee, startdate, enddate) VALUES (?,?,?,?);', data )
        conn.commit()
        pid = c.lastrowid
        data = (phonecall.get_uid(), pid)
        c.execute( 'INSERT INTO phonebills ( uid, pid ) VALUES (?,?);', data )
        conn.commit()
        conn.close()
        return pid

    def update_phonecall(self, phonecall):
        conn = sqlite3.connect(self.dbfile)  # establishes connection
        c = conn.cursor()
        data = (phonecall.get_caller(), phonecall.get_callee(), phonecall.get_starttime_string(),
                phonecall.get_endtime_string()) #all rows from phonecall table, match insert phonecall
        c.execute(' UPDATE phonecall SET caller= , callee= , startdate= , enddate= ,'
                  'WHERE uid= ', data)
        conn.commit()
        pid = c.lastrowid
        data = (phonecall.get_uid(), pid)
        c.execute('UPDATE phonebills (uid,pid) VALUES (?,?);', data)
        conn.commit()
        conn.close()
        return pid


    def delete_phonecall(self, phonecall):
        conn = sqlite3.connect(self.dbfile)  # establishes connection
        c = conn.cursor()
        data = (phonecall_id, uid)
        c.execute('DELETE FROM phonebills WHERE pid=? and uid=  ', data)
        conn.commit()
        conn.close()


    def select_phonecall(self, phonecall_id):
        conn = sqlite3.connect(self.dbfile)  # establishes connection
        c = conn.cursor()
        data = (phonecall.get_caller(), phonecall.get_callee(), phonecall.get_starttime_string(),
                phonecall.get_endtime_string())  # all rows from phonecall table, match insert phonecall
        # from w3schools select statement
        c.execute('SELECT * FROM phonecall', data)
        conn.commit()
        conn.close()

    def search_phonecalls_bydate(self, startdate, enddate):
        conn = sqlite3.connect(self.dbfile)  # establishes connection
        c = conn.cursor()
        data = (phonecall.get_caller(), phonecall.get_callee(), phonecall.get_starttime_string(),
                phonecall.get_endtime_string())  # all rows from phonecall table, match insert phonecall
        c.execute('SELECT * FROM phonecall WHERE starttime AND endtime BETWEEN startdate AND enddate', data)
        result = c.fetchall()
        conn.commit()
        conn.close()
        return result

    def search_phonecalls_bycustomername(self, customer_id):
        conn = sqlite3.connect(self.dbfile)  # establishes connection
        c = conn.cursor()
        data = (phonecall.get_caller(), phonecall.get_callee(), phonecall.get_starttime_string(),
                phonecall.get_endtime_string())  # all rows from phonecall table, match insert phonecall
        c.execute('SELECT * FROM phonecall WHERE name ==customer_id', data) #how i think it should go
        result = c.fetchall()
        conn.commit()
        conn.close()
        return result

    def insert_user(self, user):
        if not self.is_user_exists(user):
            conn = sqlite3.connect(self.dbfile)
            c = conn.cursor()
            data = (user["id"], user["name"], user["email"],user["password"])
            c.execute('INSERT INTO users (id, name,email, password) VALUES (?,?,?,?);', data)
            conn.commit()
            conn.close()
            return  True
        else:
            return False

    def authenticate_user(self,user):
        conn = sqlite3.connect(self.dbfile)
        c = conn.cursor()
        #passwd = str(md5(user['password'].encode()).digest())
        c.execute('select * from users where users.email=? and users.password=?', (user["email"] , user['password']))
        if c.fetchone():
            conn.close()
            return True
        else:
            conn.close()
            return False

    def get_user_by_email(self, user):
        conn = sqlite3.connect(self.dbfile)
        c = conn.cursor()
        c.execute('select * from users where users.email=?', (user["email"],))
        return c.fetchone()

    def is_user_exists(self, user):
        conn = sqlite3.connect(self.dbfile)
        c = conn.cursor()
        c.execute('select * from users where users.email=?', (user["email"],))
        rows =c.fetchone()
        if rows:
            return True
        else:
            return False
#TODO  Implement Data Object Access(DOA) functionalities for Phonebills and PhoneCalls


if __name__ == '__main__':
    doa = PhoneBill_DOA('../data/phonebill.db')
    user = dict()
    user['id'] = str(uuid.uuid4())
    user['email'] = 'abc@abc.com'
    user['name']="Test User"
    user['password'] = "123456"
    user['password'] = str(md5(user['password'].encode()).digest())
    doa.insert_user(user)
    print(doa.is_user_exists(user))