import sqlite3





connection = sqlite3.connect("certificat.db")

#try:
#    connection.cursor().execute('CREATE TABLE nodes (name TEXT, certificat TEXT, PRIMARY KEY(name, certificat))')
##self.connection.cursor().execute('INSERT INTO nodes VALUES(?,?)',("machine1","89:D3:12:5E:97:34:B6:00:CB:F2:68:7F:7A:5E:0A:C5"))
#except Exception as err:
#    print('issue while creating database: %s' % err)
#    
#try:
#    connection.cursor().execute('INSERT INTO nodes VALUES(?,?)',("machine1","89:D3:12:5E:97:34:B6:00:CB:F2:68:7F:7A:5E:0A:C5"))
#except Exception as err:
#    print('issue while creating database: %s' % err)
#    
#
#connection.commit()

res = connection.cursor().execute('SELECT * FROM nodes')
for row in res:
    print row