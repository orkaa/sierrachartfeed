#!/usr/bin/python
'''
Created on 14.04.2011

@author: slush
@licence: Public domain
@version 0.5

Modified to work with MPEX on 24.2.2013
by @orkaa

'''

from optparse import OptionParser
import datetime
import time
import os
import sys
import urllib2
import socket
import requests
import pickle

try:
    import simplejson as json
except ImportError:
    import json

from scid import ScidFile, ScidRecord

MPEX_URL = 'https://sauth.bitcoin-assets.com/'

class ScidHandler(object):
    def __init__(self, symbol, datadir, volume_precision):
        self.symbol = symbol
        self.filename = os.path.join(datadir, "%s.scid" % symbol)
        self.volume_precision = volume_precision
        self.load()
        
    def load(self):
        print 'Loading data file', self.filename
        if os.path.exists(self.filename):
            self.scid = ScidFile()
            self.scid.load(self.filename)
        else:
            self.scid = ScidFile.create(self.filename)
        self.scid.seek(self.scid.length)
         
    def ticker_update(self, data):
        latest_id = int(data['id'])
        price = float(data['price']) / 100000000
        volume = int(float(data['volume'])*10**self.volume_precision)
        date = datetime.datetime.fromtimestamp(float(data['timestamp']))
        
        print self.symbol, date, price, float(volume)/10**self.volume_precision
        
        # Datetime, Open, High, Low, Close, NumTrades, TotalVolume, BidVolume, AskVolume):
        try:
            rec = ScidRecord(date, price, price, price, price, 1, volume, 0, 0)
            self.scid.write(rec.to_struct())
            self.scid.fp.flush()
            pickle.dump( latest_id, open(mpex_id_filename, "wb" ))
        except Exception as e:
            print str(e)
  
def linesplit(sock):
    buffer = ''
    while True:
        try:
            r = sock.recv(1024)
            if r == '':
                raise Exception("Socket failed")
            
            buffer = ''.join([buffer, r])
        except Exception as e:
            if str(e) != 'timed out': # Yes, there's not a better way...
                raise

        while "\n" in buffer:
            (line, buffer) = buffer.split("\n", 1)
            yield line

class ScidLoader(dict):
    def __init__(self, datadir, volume_precision):
        super(ScidLoader, self).__init__() # Don't use any template dict
        
        self.datadir = datadir
        self.volume_precision = volume_precision
        
    def __getitem__(self, symbol):
        try:
            return dict.__getitem__(self, symbol)
        except KeyError:
            handler = ScidHandler(symbol, self.datadir, self.volume_precision)
            self[symbol] = handler
            return handler

def auth(latest_id):
    auth = {'user': options.MPEX_USER, 'pass': options.MPEX_PASS, 'start': latest_id}
    r = requests.post(MPEX_URL, verify=False, data=auth).json()
    if r.has_key('error'):
        if r['error'] in ("Invalid login.", "Missing user or pass."):
            sys.exit(r['error'])
        raise Exception(r['error'])
    else:
        token = r['token']
        mpex_socket = (r['socket'], int(r['port']))
        return token, mpex_socket

         
if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-d", "--datadir", dest="datadir", default='c:/SierraChart/data/',
                  help="Data directory of SierraChart software")
    parser.add_option("-y", "--disable-history", action='store_true', default=False,
                  help="Disable downloads from bitcoincharts.com")
    parser.add_option("--volume-precision", default=2, dest="precision", type="int",
                  help="Change decimal precision for market volume.")
    parser.add_option("-s", "--symbols", dest='symbols', default='*',
                  help="Charts to watch, comma separated. Use * for streaming all markets.")
    parser.add_option("-u", "--user", dest='MPEX_USER',
                  help="Username")
    parser.add_option("-p", "--password", dest='MPEX_PASS',
                  help="Password")

    (options, args) = parser.parse_args()

    if not options.MPEX_USER:
        options.MPEX_USER = raw_input("User: ")
    if not options.MPEX_PASS:
        options.MPEX_PASS = raw_input("Password: ")
    
    if options.precision < 0 or options.precision > 8:
        print "Precision must be between 0 and 8"
        sys.exit()

    # Symbols to watch    
    symbols = options.symbols.split(',')
    scids = ScidLoader(options.datadir, options.precision)

    mpex_id_filename = os.path.join(options.datadir, "MPEX_ID")
    if not os.path.exists(mpex_id_filename):
        pickle.dump( 0, open(mpex_id_filename, "wb" ))
            
    for s in symbols:
        if s != '*':
            scids[s]
        
    while True:
        try:
            if options.disable_history:
                latest_id = -1
            else:
                latest_id = pickle.load(open(mpex_id_filename, "rb" ))

            print "Opening streaming socket..."
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            token, mpex_socket = auth(latest_id)

            sock.settimeout(1)
            sock.connect(mpex_socket)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            sock.send("%s\n" % token)
            
            for line in linesplit(sock):
                rec = json.loads(line)

                symbol = rec['symbol']
                if symbol not in symbols and '*' not in symbols:
                    # Filtering out symbols which user don't want to store
                    # If '*' is in symbols, don't filter anything
                    continue
                scids[symbol].ticker_update(rec)

        except KeyboardInterrupt:
            print "Ctrl+C detected..."
            break
        except Exception as e:
            print "%s, retrying..." % str(e)
            time.sleep(5)
            continue
        finally:
            print "Stopping streaming socket..."
            try:
                sock.send("BYE")
            except socket.error:
                pass
            sock.close()
            
    for scid in scids.values():
        scid.scid.close()
