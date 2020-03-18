# encoding: utf-8
"""
@project = javbus_crawler
@file = multiproc_crowler
@author = ThinkPad
@create_time = 2020-03-0911:25
"""

import parser_movie
from multiprocessing import Process, freeze_support, Queue, Event
from object_prototype import AvExpt
from settings import GENRE_PROC_NUM, MOVIE_PROC_NUM, EXPT_PROC_NUM
import pymysql
import time
import database


def parse_genre(genre_queue, avnum_queue, expt_queue, proc_num, proc_event):
    # get genreid from genre_queue, call crawl_avnum function to get avnum
    # put avnum into avnum_queue, put error genreid or list page into expt_queue
    print('Genre process {} is ready!'.format(proc_num))
    while genre_queue.empty() is not True:
        genreid = genre_queue.get()
        print('Genre process {} start parsing genre:{}'.format(proc_num, genreid))
        try:
            parser_movie.crawl_avnum(genreid, avnum_queue, expt_queue, proc_num)
        except AvExpt as e:
            e.change_type('genreid')
            expt_queue.put(e)
            continue
        print('Genre process {} parsing genre:{} complete'.format(proc_num, genreid))
        print('当前队列中番号数量：', avnum_queue.qsize())
        while avnum_queue.qsize() >= 100:
            time.sleep(2)
            print('等待', avnum_queue.qsize())
    proc_event.set()
    return


def parse_movie(avnum_queue, expt_queue, proc_num, genre_event, movie_event):
    # time.sleep(proc_num)
    # while avnum_queue.empty() is True:
    #     time.sleep(2)
    #     print(proc_num, '休眠')
    # while avnum_queue.empty() is not True:
    print('Avnum process {} is ready!'.format(proc_num))
    while True:
        while (avnum_queue.empty() is True) and (genre_event.is_set() is False):
            # print(avnum_queue.empty(), genre_event.is_set())
            time.sleep(2)
            # print(proc_num, '休眠')
        av_info = avnum_queue.get()
        print('Avnum process {} start parsing avnum:{}'.format(proc_num, av_info[0]))
        try:
            parser_movie.crawl_movie(av_info, expt_queue, proc_num)
        except AvExpt as e:
            print('Process {} facing exception when crawling link:'.format(proc_num))
            print(e)
            expt_queue.put(e)
            continue
        except OSError as ose:
            print('Process {} facing database exception when crawling {}:'.
                  format(proc_num, av_info[0]))
            expt_proc_queue.put(AvExpt(proc_num, 'OSError', av_info[0], ose))
        if avnum_queue.empty() and genre_event.is_set():
            print('Process {} complete parsing movie!'.format(proc_num))
            movie_event.set()
            break
    return


def log_expt(expt_queue, movie_event):
    print('Exception log process is ready!')
    while True:
        while expt_queue.empty() is not True:
            expt = expt_queue.get()
            print('写入异常：{}'.format(expt))
            try:
                # print('异常测试1:', expt, '异常测试2：', expt.expt_info)
                database.insert_expt(expt)
            except Exception as e:
                print('系统异常：{}'.format(e))
                expt_queue.put(expt)
                continue
        if (expt_queue.empty() is True) and (movie_event.is_set() is True):
            print('异常写入完成！')
            break


if __name__ == '__main__':
    # conn = pymysql.connect(host='localhost',
    #                        user='root',
    #                        password='19820612',
    #                        database='movie1026',
    #                        use_unicode=True,
    #                        charset='utf8')
    # c = conn.cursor()
    # sql_query = "select genreid from genre"
    #
    # with conn:
    #     c.execute(sql_query)
    # genres = list(c.fetchall())
    genres = database.get_genre_id()

    parse_genre_proc = []
    parse_movie_proc = []
    log_expt_proc = []
    genre_proc_queue = Queue()
    avnum_proc_queue = Queue()
    expt_proc_queue = Queue()
    parse_genre_task_done = Event()
    parse_movie_task_done = Event()
    parse_genre_task_done.clear()
    parse_movie_task_done.clear()
    print(parse_genre_task_done.is_set())
    for genre_item in genres:
        genre_proc_queue.put(genre_item[0])
    # for i in range(6):
    #     genre_proc_queue.get()
    freeze_support()
    for i in range(GENRE_PROC_NUM):
        proc_name = 'Genre process ' + str(i + 1)
        parse_genre_proc.append(Process(name=proc_name, target=parse_genre,
                                        args=(genre_proc_queue,
                                              avnum_proc_queue,
                                              expt_proc_queue, i,
                                              parse_genre_task_done)))
    for i in range(MOVIE_PROC_NUM):
        proc_name = 'Movie process ' + str(i + 1)
        parse_movie_proc.append(Process(name=proc_name, target=parse_movie,
                                        args=(avnum_proc_queue,
                                              expt_proc_queue, i,
                                              parse_genre_task_done,
                                              parse_movie_task_done
                                              )
                                        )
                                )
    for i in range(EXPT_PROC_NUM):
        proc_name = 'Exception process ' + str(i + 1)
        log_expt_proc.append(Process(name=proc_name, target=log_expt,
                                     args=(expt_proc_queue, parse_movie_task_done)))
    for i in range(GENRE_PROC_NUM):
        parse_genre_proc[i].start()
    for i in range(MOVIE_PROC_NUM):
        parse_movie_proc[i].start()
    for i in range(EXPT_PROC_NUM):
        log_expt_proc[i].start()

    while True:
        for i in range(GENRE_PROC_NUM):
            print(parse_genre_proc[i].name, parse_genre_proc[i].is_alive())
            if (parse_genre_proc[i].is_alive() is False) and (
                    parse_genre_task_done.is_set() is False):
                parse_genre_proc[i].terminate()
                parse_genre_proc[i].join()
                print(parse_genre_proc[i].name, 'joined!')
                time.sleep(2)
                parse_genre_proc[i] = Process(name='Genre process ' + str(i + 1),
                                              target=parse_genre,
                                              args=(avnum_proc_queue,
                                                    expt_proc_queue, i,
                                                    parse_genre_task_done,
                                                    parse_movie_task_done
                                                    )
                                              )
                parse_genre_proc[i].start()
                print(parse_genre_proc[i].name, 'reboot!')
        for i in range(MOVIE_PROC_NUM):
            print(parse_movie_proc[i].name, parse_movie_proc[i].is_alive())
            if (parse_movie_proc[i].is_alive() is False) and (
                    parse_movie_task_done.is_set() is False):
                parse_movie_proc[i].terminate()
                parse_movie_proc[i].join()
                print(parse_movie_proc[i].name, 'joined!')
                time.sleep(2)
                parse_movie_proc[i] = Process(name='Movie process ' + str(i + 1),
                                              target=parse_movie,
                                              args=(avnum_proc_queue,
                                                    expt_proc_queue, i,
                                                    parse_genre_task_done,
                                                    parse_movie_task_done
                                                    )
                                              )
                parse_movie_proc[i].start()
                print(parse_movie_proc[i].name, 'reboot!')
        if parse_movie_task_done.is_set():
            print('parse_movie_task_done', parse_movie_task_done.is_set())
            break
        time.sleep(20)

    for i in range(GENRE_PROC_NUM):
        parse_genre_proc[i].join()
    for i in range(MOVIE_PROC_NUM):
        parse_movie_proc[i].join()
    for i in range(EXPT_PROC_NUM):
        log_expt_proc[i].join()
