from telegram.ext import Updater
import requests
import schedule
import time
import datetime
import pandas as pd
import json
import gspread
from oauth2client.client import SignedJwtAssertionCredentials


def get_last_price(symbol, time_from, time_to):
    request_url = "https://www.poems.co.id/api/NewChrt/history?symbol={0}&resolution=D&from={1}&to={2}".format(symbol, time_from, time_to)
    response = requests.get(request_url)
    response_json = response.json()

    if response_json['s'] != 'no_data':
        return pd.DataFrame(response.json())


def get_google_alert_spreadsheet():
    json_key = json.load(open('cred.json'))  # json credentials you downloaded earlier
    scope = ['https://www.googleapis.com/auth/spreadsheets', "https://www.googleapis.com/auth/drive"]

    credentials = SignedJwtAssertionCredentials(json_key['client_email'], json_key['private_key'].encode(), scope)

    file = gspread.authorize(credentials)  # authenticate with Google
    sheet = file.open_by_key('1hwCmkin9YcM34tu1SN5tcf6H0pQFuG_imwz3yuhgyng').sheet1  # open sheet
    return sheet


def scan_alerts(alerts):
    new_alerts = []
    matched_alert = []
    current = time.time()
    start_time = current - 432000

    for alert in alerts:
        alert_symbol = alert[0]
        target_price = float(alert[1])

        series = get_last_price(alert_symbol, start_time, current)

        if series is not None:
            series['t'] = pd.to_datetime(series['t'], unit='s')
            tail = series.iloc[-1]
            pre_tail = series.iloc[-2]
            if (tail['l'] <= target_price <= tail['h']) or (pre_tail['c'] <= target_price <= tail['o']) or (pre_tail['c'] >= target_price >= tail['o']):
                print(alert_symbol)
                alert.append(str(tail['c']))
                matched_alert.append(alert)
                alert = None

        if alert is not None:
            new_alerts.append(alert)

    return new_alerts, matched_alert


def main():
    try:
        pre_existing_alerts = []

        # get alert configuration from google spreadsheet
        sheet = get_google_alert_spreadsheet()
        if sheet.get_all_values():
            pre_existing_alerts = sheet.get_all_values()
        print("{0} {1}".format(datetime.datetime.now(), pre_existing_alerts))

        # do scan
        new_alerts, matched_alert = scan_alerts(pre_existing_alerts)

        # if existing spreadsheet contains any items, then clear the sheet
        if sheet.get_all_values():
            sheet.clear()

        # insert new alert into google spreadsheet
        if new_alerts:
            for a in new_alerts:
                sheet.append_row(a)

        # send telegram messages
        if matched_alert:
            for matched in matched_alert:
                updater.bot.send_message(chat_id=645832944, text="{0} is currently at {1}. Target was at {2}"
                                         .format(matched[0],
                                                 matched[2],
                                                 matched[1]))
            updater.stop()
    except BaseException as ex:
        print(ex)


if __name__ == "__main__":
    print('Start')
    updater = Updater(token='855752600:AAGNLK9VCkMzPhSkWlBBkiyz4ct5lqtutui6', use_context=True)
    updater.bot.send_message(chat_id=645832944, text="[{0}] Daily Scan Begins ...".format(datetime.datetime.now()))
    main()  # initial run
    schedule.every(10).minutes.do(main)

while True:
    schedule.run_pending()
    time.sleep(1)

