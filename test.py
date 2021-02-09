'''
import schedule
import time


def job():
    name = time.time()
    file = open('testfile.txt' + str(name), 'w')
    file.write('hello')
    file.close()


schedule.every(1).minutes.do(job)

while True:
    schedule.run_pending()
    time.sleep(1)
'''