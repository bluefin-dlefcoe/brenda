# -*- coding: utf-8 -*-
"""
Created on Thu Nov 11 07:37:45 2021

@author: Keith Wynroe
"""

import paramiko
import PySimpleGUI as sg
import pandas as pd
import numpy as np
import xlwings as xw
import threading
import logging
import time
import os
import random
import time
from collections import defaultdict
from datetime import datetime

username, password = "Charles Turner", "101-One=100"
client = paramiko.SSHClient()
client.load_system_host_keys()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())


bbg_order_keys = {
"R":"quote_request",
"AI":"quote_status_report",
"S":"quote",
"CW":"quote_ack",
"Z":"quote_cancel",
"AJ":"quote_respone",
"8":"execution_report",
"BN":"execution_ack",
"AG":"quote_request_reject"   
    }

bbg_cmd = """
        cd /opt/Bluefin/RfqAdapter/client.logs/; 
        tail -n 10 FIX.4.4-BLUEETFRFQ-BLPRFQ.messages.current.log | tr '\01' '|'
          """
          
twb_cmd = """
        cd /opt/Bluefin/RfqAdapter/client.logs/; 
        tail -n 10 FIX.4.4-BLUE_ETF_PROD_20967_DLRDPL-TRADEWEB.messages.current.log | tr '\01' '|'
          """
          
bbg_keys = [ "35", "131", "55", "38", "54", "15", "694"]       
twb_keys =  [ "35", "131", "455", "38", "54", "15", "694"]  
update_map = {"5":"Done Away",
              "6":"Pass",
              "7":"End Trade",
              "8":"Expired"}


def query(command):
    try:
        stdin, stdout, stderr = client.exec_command(command)
        return stdout.readlines()
    except KeyboardInterrupt:
            pass
        
def pub_log_parser(line):
    return line.split("\t")[1].split(",")
    
def process(msg, keys):
    msg = msg.split("|")
    msg = [x.split("=") for x in msg if "=" in x]
    msg_dct = {}
    for key in keys:
        msg_dct[key] = [x[1] for x in msg if x[0] == key]
    
    return msg_dct

def match_id(id_, arr):
    return id_ == arr[0]


def tail(f, lines=10, _buffer=4098):
    """Tail a file and get X lines from the end"""
    # place holder for the lines found
    lines_found = []

    # block counter will be multiplied by buffer
    # to get the block size from the end
    block_counter = -1

    # loop until we find X lines
    while len(lines_found) < lines:
        try:
            f.seek(block_counter * _buffer, os.SEEK_END)
        except IOError:  # either file is too small, or too many lines requested
            f.seek(0)
            lines_found = f.readlines()
            break
        lines_found = f.readlines()
        block_counter -= 1

    return lines_found[-lines:]



def process(msg, keys):
    msg = msg.split("|")
    msg = [x.split("=") for x in msg if "=" in x]
    msg_dct = {}
    for key in keys:
        msg_dct[key] = [x[1] for x in msg if x[0] == key]   
    return msg_dct
                        
    
class brenda_gui():
    
    def __init__(self, data, mapping, map_dct, log_file):
        self.data = data
        self.last_msg = {"bbg":"", "twb":""}
        self.last_line = []
        self.open_rfqs = []
        self.mapping = mapping
        self.map_dct = map_dct
        self.fair_values = defaultdict(list)
        self.got_update = []
        self.twb_directions = {"1":"BID", "2":"ASK", "7":"MARKET"}
        self.bbg_directions = {"0":"BID", "1":"ASK", "2":"MARKET"}
        sg.theme("DarkAmber")
        rfq_feed = [
            [sg.Table(
                values=self.data,
                headings=["ticker", "quantity", "direction", "currency", "status", "  owner  ", "source"],
                col_widths = 100,
                num_rows=10,
 
                key='-TABLE1-',
                enable_events=True,
                enable_click_events=True,
                justification='center',

                )]]

        summary_data = [[sg.Table(values = [["", "", "", "", "", "", ""]],
                                headings = [" TICKER ", " OUR BID ", " MKT BID ", "FAIR_VALUE", " MKT ASK ", " OUR ASK ", "POSITION"],
                                key='-TABLE2-',
                                col_widths = 100,
                                num_rows=10,
           
                                enable_events=True,
                                enable_click_events=True,
                                justification='center')],
                        [
                            sg.Text("", size=(60, 5),  relief=sg.RELIEF_SUNKEN, background_color='white', text_color='black',
                                    enable_events=True, key="-COMMENT BOX-")]
        
        ]
        
        self.layout = [
        [
        sg.Column(rfq_feed),
        sg.VSeperator(),
        sg.Column(summary_data)]
        ]
        
        self.window = sg.Window('BRENDA', self.layout, finalize=True)
        self.table1 = self.window['-TABLE1-']
        self.table2 = self.window['-TABLE2-']

    
    def match_id(self, id_, arr):
        return id_ == arr[0]
    
    def parse_pub(self, line):
        ls = line.split('\t')[1].split(",")
        if len(ls) < 15:
            return tuple([""]*6)
        return (ls[6], ls[11], ls[9], ls[8], ls[10], ls[12], ls[13], ls[14])
        
        
    def push_rfq(self, msg_dct, source):
        self.open_rfqs.append(msg_dct["131"])
        if len(self.open_rfqs) > 50:
            self.open_rfqs.pop(0)
        row = list([x[0] for x in msg_dct.values() if x])
        row.append("open")
        row = row[1:]
        row.append(own_dct[row[1]])
        if source == "bbg":
            row[3] = self.bbg_directions[row[3]]
        else:
            row[3] = self.twb_directions[row[3]]
        row.append(source)
        print(row)
        
        self.data.insert(0, row[1:])
        if len(self.data) > 10:
            self.data.pop(-1)
                            
    def listen(self, source):
        if source == "bbg":
            new_msg = query(bbg_cmd)
           
        else:
            new_msg = query(twb_cmd)
        
        if new_msg[0] == self.last_msg[source]:
            pass
        else:
            self.last_msg[source] = new_msg[0]
            if source == "bbg":
                msg_dcts = [process(x, bbg_keys) for x in new_msg]
                
            else:
                msg_dcts = [process(x, twb_keys) for x in new_msg]
            
            for msg_dct in [x for x in msg_dcts if len(x) > 0]:
                #print(msg_dct)
                if "35" in msg_dct.keys():
                    if msg_dct["35"] == ["R"] and msg_dct["131"] not in self.open_rfqs:
                        self.push_rfq(msg_dct, source)
                        """print(self.data)"""
                    elif msg_dct["35"] == ["AJ"] and msg_dct["131"] in self.open_rfqs:
                        match_list = list(map(lambda x: match_id(str(msg_dct["131"][0]), x), self.data))

                        if True in match_list:
                            match_index = match_list.index(True)                      
                            self.data[match_index][-3] = update_map[msg_dct["694"][0]]
        self.window['-TABLE1-'].update(values = self.data)

                        
    def pull_data(self, ticker, map_dct, sheet):
         if ticker in map_dct.keys():
             lookup_str = "B{_}:H{_}".format( _ = map_dct[ticker])
             res =  [sheet.range(lookup_str).value]
             res[0] = [round(x, 3) for x in res[0] if type(x) == float]
             temp = res[0][:5]
             res.append([round(((x/temp[2])-1)*10000, 1) for x in temp])
             print("DEBUG IS ", res)
             return res
         else:
             return [[random.random()]*6]
    


        
    def run(self):
            with open(log_file) as infile:   
                infile.seek(0,2)
                while True:
                        self.event, self.values = self.window.read(timeout = 100)
                        self.listen("twb")
                        self.window['-TABLE1-'].update(values = self.data)
                        self.listen("bbg")
                        self.window['-TABLE1-'].update(values = self.data)
                        if self.event == sg.TIMEOUT_KEY:
                            self.window.refresh()
                        elif self.event == sg.WIN_CLOSED:
                            break
                        line = infile.readline()
                        if (line == self.last_line):
                            pass
                        elif len(line) > 0:
                            self.last_line = line
                            #("NEW LINE " + line)
                            data = self.parse_pub(line)
                            self.fair_values[data[0]] = data[1:]
                            self.got_update.append(data[0])
                        
                        if isinstance(self.event, tuple) and self.event[0:2] == ('-TABLE1-', '+CICKED+'):
                    
                    # click position row, column, count from 0, -1 for headings
                            
                            row, col = self.event[2]
                            selection = self.values['-TABLE1-']
                        # previous selected row number, not now.
                            previous_select_row = selection[0] if selection else 'None'
                            ticker = self.data[row][0]
                            print(ticker in self.got_update, ticker)
                            etf_data = self.fair_values[ticker]
                            
                            
                            if etf_data:
                                print("DEBUG = "+ str(etf_data))
                                #etf_data = self.pull_data(self.data[row][1], self.map_dct, self.sheet)
                                display_vals = [[np.round(float(x), 3) if len(x) > 0 else 0 for x in etf_data[:-1]]]        
                                bps_diff = [np.round(((x/display_vals[0][2])-1)*10000, 1) for x in display_vals[0]]
                                bps_diff[-1] = ""
                                bps_diff.insert(0, "")
                                print(bps_diff)
                                display_vals[0].insert(0, ticker)
                                display_vals.append(bps_diff)
                                print(display_vals)
                                self.window['-TABLE2-'].update(values = display_vals)
                            else:
                                self.window['-TABLE2-'].update(values = [[ticker, "No data :("]])
     


date = datetime.strftime(datetime.today().date(), "%Y%m%d")
data = [["", "", "", "", "", "", ""]]
log_file = "\\\\ht-pubsub1\\pubsublogs\\pubsub_log_"+date+".csv"
mapping = pd.read_csv("Y:\\Brenda\\mapping.csv", index_col = "Unnamed: 0")
path = "Y:\\Brenda\\"
os.chdir(path)
df = pd.read_csv(path + "live_prices_copy.csv")
map_dct = {}
own_dct = defaultdict(str)
for i in df.index.tolist():
        map_dct[df.loc[i, "Row"]] = i+2
        own_dct[df.loc[i, "Row"]] = df.loc[i, "Owner"]
client = paramiko.SSHClient()
client.load_system_host_keys()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
username, password = "Keith Wynroe", "London99"
client.connect("ld4-3", username = username, password = password)
brenda_gui(data, mapping, map_dct, log_file).run()
        

