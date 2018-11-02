
import json, pika, io, random,curses, time, sys
from curses import wrapper

credentials = pika.PlainCredentials('123', '123')
try:
    connection = pika.BlockingConnection(pika.ConnectionParameters('host', 5672, '/', credentials))
    channel = connection.channel()
except pika.exceptions.ConnectionClosed:
    print('** Unable to publish connection failed **')

def straightMethod(stdscr):
    print('*** Press p to pause and c to continue ***')
    stdscr.nodelay(True)
    stdscr.clear()
    filepath = 'file.json'
    with open(filepath) as fp:
        for cnt, line in enumerate(fp):
            try:
                c = stdscr.getch()
                curses.flushinp()
                stdscr.clear()
                if c == ord('p'):
                    while 1:
                        stdscr.clear()
                        c = stdscr.getch()
                        if c == ord('c'):
                            break
                        else:
                            pass
                print('*** sending msg ***')
                channel.basic_publish(exchange='ex', routing_key='rk', body=line)
            except pika.exceptions.ConnectionClosed:
                print('** Unable to publish connection failed **')
            except:
                print('*** bad line ***')
            time.sleep(4)


wrapper(straightMethod)


