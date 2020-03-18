# encoding: utf-8
"""
@project = javcrawlerplus
@file = parser
@author = ThinkPad
@create_time = 2020-03-0922:39
"""

from settings import GENRE, ENTRY, REQURL, STAR
import math
import database
import bs4
import requests
from object_prototype import Movie, Star, Genre, Link, AvExpt
import os
import re
import time


# This is a file including functions of parsing javbus pages.


def crawl_avnum(genreid, avnum_queue, expt_queue, proc_num):
    # get avnum from javbus website according genreid, and put avnums into avnum_queue
    next_page = 1
    max_page = 1
    while next_page <= max_page:
        url = get_page_url(next_page, genreid)
        if next_page == 1:
            next_page_soup = get_list_page_soup(url, genreid, proc_num)
            movie_sum = int(next_page_soup.
                            find_all('a', {'class': 'mypointer', 'id': 'resultshowmag'})[0].
                            get_text().split()[-1]
                            )
            max_page = math.ceil(movie_sum / 30)
            print('genre {} 共{}页'.format(genreid, max_page))
        else:
            try:
                next_page_soup = get_list_page_soup(url, genreid, proc_num)
            except AvExpt as e:
                print(e)
                expt_queue.put(e)
                continue

        movie_links = get_movie_list(next_page_soup)
        for i in movie_links:

            # get av num from the soup
            av_num = get_av_num(i[0])

            # skip existed movie
            if database.check_existence(av_num):
                # print('* 已存在 %s 停止爬取 *' % av_num)
                continue
            else:
                avnum_queue.put(i)
                # print('* {} 放入队列 *'.format(av_num))
        next_page += 1


def crawl_movie(av_info, expt_queue, proc_num):
    # get movie data, genre data, star data, images data from javbus website according avnum
    av_num = get_av_num(av_info[0])
    if database.check_existence(av_num) is True:
        return
    print('Process {} get avnum: {}'.format(proc_num,av_num))
    # get movie soup
    soup = get_movie_soup(av_info, proc_num)

    # get movie class
    try:
        movie = get_movie_class(soup, av_num, av_info[1])
    except AvExpt as e:
        print('Process {} facing exception when database inserting:'.format(proc_num))
        print(e)
        expt_queue.put(e)
        return
    print('movie class:', movie)

    # get starID list of a movie
    star_id_iter = get_star_iter(soup)

    # get genre list of a movie
    genres = get_genre_iter(soup)

    # get links of a movie
    try:
        link_iter = get_download_iter(soup, av_info[0], av_num, proc_num)
    except AvExpt as e:
        print('Process {} facing exception when crawling link:'.format(proc_num))
        print(e)
        expt_queue.put(e)
        return

    # get sample images of a movie
    images = get_sample_img_iter(soup)

    # store movie info to database
    try:
        database.insert_movie(movie)
    except Exception as e:
        print('Process {} facing exception when insert movie:'.format(proc_num))
        print(e)
        expt_queue.put(AvExpt(proc_num,'database_insert', av_info[0], str(e),'movie_insert'))

    # store movie and star info to database
    for s in star_id_iter:
        if database.check_stars(s[0]):
            # print('Process{},avnum: {},starid: {}'.format(proc_num, av_num, s[0]))
            try:
                database.insert_m_s(av_num, s[0])
            except Exception as e:
                print('Process {} facing exception when insert m_s:'.format(proc_num))
                print(e)
                expt_queue.put(AvExpt(proc_num, 'database_insert', av_info[0], str(e), 'm_s_insert'))
        else:
            try:
                p = get_star(s, proc_num)
            except AvExpt as e:
                print('Process {} facing exception when crawling star:'.format(proc_num))
                print(e)
                expt_queue.put(e)
                continue
            try:
                database.insert_star(p)
            except Exception as e:
                print('Process {} facing exception when insert star:'.format(proc_num))
                print(e)
                expt_queue.put(AvExpt(proc_num, 'database_insert', av_info[0], str(e), 'star_insert'))
            try:
                database.insert_m_s(av_num, s[0])
            except Exception as e:
                print('Process {} facing exception when insert m_s:'.format(proc_num))
                print(e)
                expt_queue.put(AvExpt(proc_num, 'database_insert', av_info[0], str(e), 'm_s_insert'))

    for g in genres:
        if database.check_genres(g[0]):
            try:
                database.insert_m_g(av_num, g[0])
            except Exception as e:
                print('Process {} facing exception when insert m_g:'.format(proc_num))
                print(e)
                expt_queue.put(AvExpt(proc_num, 'database_insert', av_info[0], str(e), 'm_g_insert'))

        else:
            try:
                p = crawl_genre(g, proc_num)
            except AvExpt as e:
                print('Process {} facing exception when crawling genre:'.format(proc_num))
                print(e)
                expt_queue.put(e)
                continue
            try:
                database.insert_genre(p)
            except Exception as e:
                print('Process {} facing exception when insert genre:'.format(proc_num))
                print(e)
                expt_queue.put(AvExpt(proc_num, 'database_insert', av_info[0], str(e), 'genre_insert'))
            try:
                database.insert_m_g(av_num, g[0])
            except Exception as e:
                print('Process {} facing exception when insert m_g:'.format(proc_num))
                print(e)
                expt_queue.put(AvExpt(proc_num, 'database_insert', av_info[0], str(e), 'm_g_insert'))

    # store links info to database
    for li in link_iter:
        try:
            database.insert_magnet(li)
        except Exception as e:
            print('Process {} facing exception when insert magnet:'.format(proc_num))
            print(e)
            expt_queue.put(AvExpt(proc_num, 'database_insert', av_info[0], str(e), 'magnet_insert'))

    # store images url to database
    for im in images:
        try:
            database.insert_img(im, av_num)
        except Exception as e:
            print('Process {} facing exception when insert img:'.format(proc_num))
            print(e)
            expt_queue.put(AvExpt(proc_num, 'database_insert', av_info[0], str(e), 'img_insert'))

    print('Process {} 已扒取完毕：第 {} 页 番号：{}'.format(str(proc_num),
                                                str(os.path.basename(av_info[0])), av_num))

    return


def get_list_page_soup(list_url, genre_id, proc_num):
    """ parse list page soup"""
    user_agent = 'Mozilla / 5.0 (Windows NT 10.0; Win64; x64) AppleWebKit / 537.36(KHTML, ' \
                 'like Gecko) Chrome / 64.0.3282.140 Safari / 537.36 Edge / 18.17763 '
    cookie = {'existmag': 'mag',
              'expires': '365',
              'path': '/'}
    headers = {'User-agent': user_agent}
    # request to javbus
    try:
        res = requests.get(list_url, headers=headers, cookies=cookie, timeout=10)
        res.raise_for_status()
        # init beautiful soup
        soup = bs4.BeautifulSoup(res.text, 'lxml')
        print(list_url, '爬取成功')
        return soup
    except Exception as e:
        raise AvExpt(proc_num, 'list_page', list_url, str(e), genre_id)


def get_movie_soup(av_info, proc_num):
    user_agent = 'Mozilla / 5.0 (Windows NT 10.0; Win64; x64) AppleWebKit / 537.36(KHTML, ' \
                 'like Gecko) Chrome / 64.0.3282.140 Safari / 537.36 Edge / 18.17763 '
    headers = {'User-agent': user_agent}

    """ get the soup of given link"""
    try:
        view_page_res = requests.get(av_info[0], headers=headers, timeout=10)
        view_page_res.raise_for_status()
        print('Process {} 已获取网页 {}'.format(proc_num, av_info[0]))
        time.sleep(0.5)
    except Exception as e:
        print('Process {} 在已获取网页 {} 时发生异常'.format(proc_num, av_info[0]))
        raise AvExpt(proc_num, 'movie_page', av_info[0], str(e), av_info[1])
    # make soup of view page
    view_page_soup = bs4.BeautifulSoup(view_page_res.text, 'html.parser')

    return view_page_soup


def get_movie_class(soup, av_num, thumb_img_url):
    """ Get movie info from view page"""

    # don't get soup too often, the server will rufuse request
    # soup = get_link_soup(link)

    # get cover and title
    big_img_elems = soup.select('.bigImage img')

    # get title
    title = big_img_elems[0].get('title')

    # get cover image url
    cover_img = big_img_elems[0].get('src')

    # get release date
    date_elems = soup.find('span', text="發行日期:")
    if date_elems is not None:
        date = date_elems.next_sibling[1:]
    else:
        date = None

    # get movie length
    length_elems = soup.find('span', text='長度:')
    if length_elems is not None:
        length = int(str(length_elems.next_sibling).replace('分鐘', '', 1))
    else:
        length = None

    # get movie director
    director_elems = soup.find('span', text='導演:')
    if director_elems is not None:
        director = director_elems.next_sibling.next_sibling.text
        if database.check_director(director) is not True:
            try:
                database.insert_director(director)
            except Exception as e:
                print('facing exception when insert director {}'.format(director))
                print(e)
                raise AvExpt(99, 'database_insert', director, str(e), 'director_insert')

    else:
        director = 'NULL'

    producer_elems = soup.find('span', text='製作商:')
    if producer_elems is not None:
        producer = producer_elems.next_sibling.next_sibling.text
        if database.check_producer(producer) is not True:
            try:
                database.insert_producer(producer)
            except Exception as e:
                print('facing exception when insert producer {}'.format(producer))
                print(e)
                raise AvExpt(98, 'database_insert', producer, str(e), 'producer_insert')
    else:
        producer = 'NULL'

    publisher_elems = soup.find('span', text='發行商:')
    if publisher_elems is not None:
        publisher = publisher_elems.next_sibling.next_sibling.text
        if database.check_publisher(publisher) is not True:
            try:
                database.insert_publisher(publisher)
            except Exception as e:
                print('facing exception when insert publisher {}'.format(publisher))
                print(e)
                raise AvExpt(98, 'database_insert', publisher, str(e), 'publisher_insert')
    else:
        publisher = 'NULL'

    movie = Movie(av_num, title,
                  cover_img, thumb_img_url,
                  date, length, director,
                  producer, publisher)

    return movie


def get_movie_list(soup):
    """ Get view page link list form main page soup"""

    # select all movie box
    url_elements = soup.select('a[class="movie-box"]')

    # url_list = []

    for u in url_elements:
        p = [u['href'], u.img['src']]
        yield p
        # url_list.append(u['href'])

    # print the number of movies in this page
    # print('项目数：' + str(len(url_list)))

    # return url_list


def get_av_num(link):
    """ get av num"""

    av_num = link.split('/')[-1]

    return av_num


def get_page_num(link):
    # get page number
    page_num = os.path.basename(link)
    return page_num


def skip_page(this_page_url, genreid):
    """ Get next page url from given url"""

    next_num = int(os.path.basename(this_page_url)) + 1
    next_page_url = get_page_url(next_num, genreid)

    return next_page_url


def get_page_url(page_num, genreid):
    """ generate a javbus page url when assign different page for multi-threading"""
    page_url = ENTRY + genreid + '/' + str(page_num)

    return page_url


def get_star_iter(soup):
    """ Get star iterator from view page"""

    star_elements = soup.select('div[class="star-name"]')
    for s in star_elements:
        p = [s.a['href'].split('/')[-1], s.text]
        yield p


def get_genre_iter(soup):
    """ Get genre iterator from view page"""

    genre_elems = soup.find('p', text='類別:').next_sibling.next_sibling.select(
        'span[class="genre"]')
    for s in genre_elems:
        p = [s.a['href'].split('/')[-1], s.text]
        yield p


def get_download_iter(soup, home_url, av_num, proc_num):
    """ get download link iterator"""
    params_elems = soup.find_all('script')[8]
    # fix a bug of gain gid from text 2019-12-08
    text = params_elems.text.split(';')[0]
    gid = text[12:]
    req_url = REQURL
    user_agent = 'Mozilla / 5.0 (Windows NT 10.0; Win64; x64) AppleWebKit / 537.36(KHTML, ' \
                 'like Gecko) Chrome / 64.0.3282.140 Safari / 537.36 Edge / 18.17763 '
    header = {'User-agent': user_agent, 'referer': home_url}
    cookie = {'PHPSESSID': 'teb1oko5bov9e28q2jmol21ac4',
              '4fJN_2132_st_p': '0%7C1575772728%7C463568f165e5991035943b71f5af6528',
              '4fJN_2132_viewid': 'tid_53236',
              '4fJN_2132_saltkey': 'LU1pEGfe',
              '4fJN_2132_lastact': '1575772728%09forum.php%09viewthread',
              'existisgenres': 'gr_single',
              'cnadd24': 'off',
              'existmag': 'mag',
              'starinfo': 'glyphicon%20glyphicon-minus',
              '4fJN_2132_sid': 'YErRWK',
              '4fJN_2132_lastvisit': '1574084947',
              '4fJN_2132_visitedfid': '36D2',
              '__cfduid': 'd58a6a3269d23fecef8c4a668c67d6aec1570587492'
              }
    payload = {'gid': gid, 'uc': 0}
    try:
        table_res = requests.get(req_url, params=payload, cookies=cookie, headers=header,
                                 timeout=10)
        table_res.raise_for_status()
    except Exception as e:
        raise AvExpt(proc_num, 'link_page', home_url, str(e), av_num)
    magnet_soup = bs4.BeautifulSoup(table_res.text, 'html.parser')
    magnet_list = magnet_soup.select('a[style="color:#333"]')
    m = 0
    pattern = r'\r|\n| |\t'

    while m < len(magnet_list):
        magnet_link = magnet_list[m].get('href')
        movie_file = re.sub(pattern, '', magnet_list[m].text)
        m += 1
        movie_size = re.sub(pattern, '', magnet_list[m].text)
        m += 1
        movie_date = re.sub(pattern, '', magnet_list[m].text)
        if movie_date == '0000-00-00':
            movie_date = '1900-01-01'
        m += 1
        p = Link(av_num, magnet_link, movie_file, movie_size, movie_date)
        yield p


def get_sample_img_iter(soup):
    """ Get sample image iterator from view page"""
    sample_img_list = soup.select('a[class="sample-box"]')

    for n in sample_img_list:
        yield n['href']


def get_star(star_info, proc_num):
    page_url = STAR + star_info[0]

    user_agent = 'Mozilla / 5.0 (Windows NT 10.0; Win64; x64) AppleWebKit / 537.36(KHTML, ' \
                 'like Gecko) Chrome / 64.0.3282.140 Safari / 537.36 Edge / 18.17763 '
    headers = {'User-agent': user_agent}
    # get star website
    try:
        res = requests.get(page_url, headers=headers, timeout=10)
        res.raise_for_status()
    except Exception as e:
        raise AvExpt(proc_num, 'star_page', page_url, str(e), star_info[0])

    # make star soup
    soup = bs4.BeautifulSoup(res.text, 'lxml')
    star_infos = soup.select('div[class="avatar-box"]')[0].find_all('p')

    # init star infos
    star_id = star_info[0]
    star_name = star_info[1]
    star_birthday = None
    star_age = None
    star_height = None
    star_cups = None
    star_bust = None
    star_waist = None
    star_hip = None
    star_birth_place = None
    star_hobby = None

    star_portrait = soup.select('div[class="avatar-box"] > '
                                'div[class="photo-frame"] > img')[0].get('src')
    for elem in star_infos:
        item = elem.text.split(': ')
        if item[0] == '生日':
            star_birthday = item[-1]
        elif item[0] == '年齡':
            star_age = item[-1]
        elif item[0] == '身高':
            star_height = int(re.findall(r'\d+', item[-1])[0])
        elif item[0] == '罩杯':
            star_cups = item[-1]
        elif item[0] == '胸圍':
            star_bust = int(re.findall(r'\d+', item[-1])[0])
        elif item[0] == '腰圍':
            star_waist = int(re.findall(r'\d+', item[-1])[0])
        elif item[0] == '臀圍':
            star_hip = int(re.findall(r'\d+', item[-1])[0])
        elif item[0] == '出生地':
            star_birth_place = item[-1]
        elif item[0] == '愛好':
            star_hobby = item[-1]

    star = Star(star_id, star_name, star_birthday, star_age, star_height, star_cups, star_bust,
                star_waist, star_hip, star_birth_place, star_hobby, star_portrait)

    return star


def crawl_genre(genre_info, proc_num):
    url = GENRE
    user_agent = 'Mozilla / 5.0 (Windows NT 10.0; Win64; x64) AppleWebKit / 537.36(KHTML, ' \
                 'like Gecko) Chrome / 64.0.3282.140 Safari / 537.36 Edge / 18.17763 '
    headers = {'User-agent': user_agent}

    try:
        genre_res = requests.get(url, headers=headers, timeout=10)
        genre_res.raise_for_status()
    except Exception as e:
        raise AvExpt(proc_num, 'genre_page', url, str(e), genre_info[0])

    # init beautiful soup
    soup = bs4.BeautifulSoup(genre_res.text, 'lxml')
    href = GENRE + genre_info[0]
    elem = soup.find(name='a', attrs={'href': href})
    elem_parent = elem.parent
    p = Genre(genre_info[0], genre_info[1], elem_parent.previous_sibling.previous_sibling.text)
    return p
