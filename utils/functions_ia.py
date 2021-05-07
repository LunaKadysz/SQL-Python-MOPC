# -*- coding: utf-8 -*-
"""
Created on Thu Mar 25 11:18:47 2021

@author: Luna
"""
import os
import pypyodbc as podbc
import pandas as pd
import gspread
import gspread_dataframe as gd
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from os.path import basename
from pretty_html_table import build_table
from imbox import Imbox # pip install imbox
import traceback

ips = {"active":'192.168.130.13' , "warehouse":"192.168.120.29"}
users ={"active":'santio' , "warehouse":"santio"}
passwords ={"active":'1q2w3e4r5t*' , "warehouse":"1Q2w3e4r5t*"}
credentials = {"ia_gmail":"/home/marcos-rago/Documents/IA/utils/credentials_iamopc.json"}
allowing_access_email =  {"ia_gmail":"ia-mopc-python@mopc-303421.iam.gserviceaccount.com"}

def download_email_attachments(subject,sent_from,download_path):
    sender = get_default_sender()
    host = "imap.gmail.com"
    username = sender["address"]
    password = sender["password"]

    download_folder = download_path

    if not os.path.isdir(download_folder):
        os.makedirs(download_folder, exist_ok=True)

    mail = Imbox(host, username=username, password=password, ssl=True, ssl_context=None, starttls=False)
    messages = mail.messages(subject=subject, sent_from=sent_from) # filtro por este asunto

    for (uid, message) in messages:
        mail.mark_seen(uid) # opcional, marco el mensaje como leido
        for idx, attachment in enumerate(message.attachments):
            try:
                att_fn = attachment.get('filename')
                download_path = f"{download_folder}/{att_fn}"
                print(download_path)
                with open(download_path, "wb") as fp:
                    fp.write(attachment.get('content').read())
            except:
                print(traceback.print_exc())

    mail.logout()

def connect_database(base = "active"):
    try:
        server = ips[base]
        database = 'master'
        username = users[base]
        password = passwords[base]
        cnxn = podbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
        return cnxn
    except:
        print("Connection with {} failed".format(base))

def connect_spreadsheets(user = "ia_gmail"):
    try:
        gc = gspread.service_account(filename=credentials[user])
        return gc
    except:
        print("Connection with Google Sheets failed")

def get_df_from_spreadsheet(gc, key, sheet_name):
    try:
        sh = gc.open_by_key(key)
        worksheet = sh.worksheet(sheet_name)
        df = gd.get_as_dataframe(worksheet).dropna(how = "all")
        return df
    except:
        print("Error getting sheet named '{}' from Google Sheets".format(sheet_name))

def get_spreadsheet_colnames(gc,key,sheet_name):
        sh = gc.open_by_key(key)
        worksheet = sh.worksheet(sheet_name)
        cols = gd.get_as_dataframe(worksheet).dropna(how = "all").columns
        return cols


    
def overwrite_spreadsheet_with_df(gc, df, key, sheet_name):
    try:
        sh = gc.open_by_key(key)
        worksheet = sh.worksheet(sheet_name)
        worksheet.clear()
        gd.set_with_dataframe(worksheet, df)
    except:
        print("Writing dataframe to spreadsheet failed")


def send_mail(subject: str, body:str, receivers=None, sender=None, files = None):
    """
    Send an email.
    Mandatory Inputs:
    subject = mail subject
    body = mail body
    Optional Inputs:
    receivers is a list of mail adresses to receive the mail
    sender is a dict with keys "address" and "password"
    """
    if receivers is None:
        receivers = get_default_receivers()
    if sender is None:
        sender = get_default_sender()
    assert type(receivers) == list
    send(sender, receivers, subject, body,files)
    
def get_default_receivers():
    return ["datos.mopc@gmail.com"]

def send(sender, receivers, subject, body, files):
    msg = prepare_msg(sender["address"], subject, body)
    for f in files or []:
        with open(f, "rb") as fil:
            part = MIMEApplication(
                fil.read(),
                Name=basename(f)
            )
        part['Content-Disposition'] = 'attachment; filename="%s"' % basename(f)
        msg.attach(part)
    s = smtplib.SMTP_SSL('smtp.gmail.com')
    s.login(sender["address"],sender["password"])
    for receiver in receivers:
        s.sendmail(sender["address"], receiver, msg.as_string())
    s.quit()

def get_default_sender():
    sender = {}
    sender["address"] = "datos.mopc@gmail.com"
    sender["password"] = "datosmopc15"
    return sender

def prepare_msg(sender_address: str, subject: str, body: str):
    msg = MIMEMultipart('alternative')
    msg['From'] = sender_address
    msg['Subject'] = subject
    html = """\
            <html>
              <head></head>
              <body>"""+ body + """
              </body>
            </html>
            """
    part2 = MIMEText(html, 'html')
    msg.attach(part2)
    return msg