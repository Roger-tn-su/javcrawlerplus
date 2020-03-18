# encoding: utf-8
"""
@project = javcrawlerplus
@file = movie_info_crawler
@author = ThinkPad
@create_time = 2020-03-189:40
"""

from object_prototype import AvExpt
from multiproc_crawler import parse_genre
from multiprocessing import Process, Queue, Event, freeze_support
from settings import GENRE_PROC_NUM
import database


def log_movie_info(avnum_queue, expt_queue, proc_event, log_proc_name):
    print('log process is ready!')
    while True:
        movie_info = avnum_queue.get()
        try:
            database.insert_movie_info(movie_info[0], movie_info[1])
        except Exception as e:
            print(log_proc_name, 'facing exception when insert movie info:')
            print(e)
            raise AvExpt(0, 'movie_info', )
        if (avnum_queue.empty() is True) and (proc_event.is_set()):
            break


if __name__ == '__main__':
    parse_genre_proc = []
    log_expt_proc = []
    genre_proc_queue = Queue()
    avnum_proc_queue = Queue()
    expt_proc_queue = Queue()
    parse_genre_task_done = Event()
    parse_genre_task_done.clear()
    genres = database.get_genre_id()
    for genre_item in genres:
        genre_proc_queue.put(genre_item[0])
    freeze_support()
    for i in range(GENRE_PROC_NUM):
        proc_name = 'Genre process ' + str(i + 1)
        parse_genre_proc.append(Process(name=proc_name, target=parse_genre,
                                        args=(genre_proc_queue,
                                              avnum_proc_queue,
                                              expt_proc_queue, i,
                                              parse_genre_task_done)))
    log_movie_proc = Process(name='log_movie', target=log_movie_info,
                             args=(avnum_proc_queue,
                                   expt_proc_queue,
                                   parse_genre_task_done,
                                   'log_movie'))
    for i in range(GENRE_PROC_NUM):
        parse_genre_proc[i].start()
    log_movie_proc.start()

    for i in range(GENRE_PROC_NUM):
        parse_genre_proc[i].join()
    log_movie_proc.join()