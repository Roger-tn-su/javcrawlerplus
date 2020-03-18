import pymysql
from object_prototype import Movie
from object_prototype import Link
from object_prototype import Star
from object_prototype import Genre
from object_prototype import AvExpt
from settings import DOMAIN, ENTRY, STAR, GENRE, REQURL
import datetime
import os
from bs4 import BeautifulSoup
import requests


def db_connection_movies(db_func):
    def wrapper(*args, **kwargs):
        conn = pymysql.connect(host='localhost',
                               user='root',
                               password='19820612',
                               database='movie1026',
                               use_unicode=True,
                               charset='utf8')
        c = conn.cursor()

        with conn:
            return db_func(c, *args, **kwargs)

    return wrapper


@db_connection_movies
def init(c):
    # Now Handle by decorator
    # conn = sqlite3.connect('movies.db')
    # c = conn.cursor()

    c.execute(""" CREATE TABLE IF NOT EXISTS `movies` (`m_id` INT AUTO_INCREMENT PRIMARY KEY ,
                                                        `avNum` VARCHAR(40) UNIQUE , 
                                                        `title` TEXT, 
                                                        `coverImgUrl` TEXT, 
                                                        `thumbImgUrl` TEXT,
                                                        `release` DATE,
                                                        `length` INTEGER,
                                                        `director` VARCHAR(255),
                                                        `producer` VARCHAR(255),
                                                        `publisher` VARCHAR(255),
                                                        FOREIGN KEY (`director`) 
                                                        REFERENCES directors(`director`),
                                                        FOREIGN KEY (`producer`) 
                                                        REFERENCES producers(`producer`),
                                                        FOREIGN KEY (`publisher`) 
                                                        REFERENCES publishers(`publisher`))
                                                        ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;""")
    c.execute(""" CREATE TABLE IF NOT EXISTS `stars` (`s_id` INT AUTO_INCREMENT PRIMARY KEY ,
                                                    `starID` VARCHAR(40) UNIQUE , 
                                                    `name` TEXT, 
                                                    `birthday` DATE,
                                                    `age` INTEGER,
                                                    `height` INTEGER,
                                                    `cups` TEXT,
                                                    `bust` INTEGER,
                                                    `waist` INTEGER,
                                                    `hip` INTEGER,
                                                    `birthplace` TEXT,
                                                    `hobby` TEXT,
                                                    `portrait` VARCHAR(255))
                                                    ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;""")
    c.execute(""" CREATE TABLE IF NOT EXISTS `genre` (`g_id` INT AUTO_INCREMENT PRIMARY KEY ,
                                                    `genreID` VARCHAR(40) UNIQUE ,
                                                    `genreName` TEXT,
                                                    `category` TEXT)
                                                     ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;""")
    c.execute(""" CREATE TABLE IF NOT EXISTS `magnets` (`ma_id` INT AUTO_INCREMENT PRIMARY KEY,
                                                        `avNum` VARCHAR(40), 
                                                        `url` TEXT,
                                                        `filename` TEXT,
                                                        `size` TEXT,
                                                        `addTime` DATE,
                                                        FOREIGN KEY(`avNum`) 
                                                        REFERENCES movies(`avNum`))
                                                        ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;""")
    c.execute(""" CREATE TABLE IF NOT EXISTS `images` (`i_id` INT AUTO_INCREMENT PRIMARY KEY ,
                                                    `avNum` VARCHAR(40), 
                                                    `imgUrl` TEXT,
                                                    FOREIGN KEY (avNum) REFERENCES movies(`avNum`)
                                                    )ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;""")
    c.execute(""" CREATE TABLE IF NOT EXISTS `m_s` (`m_s_id` INT AUTO_INCREMENT PRIMARY KEY ,
                                                    `avNum` VARCHAR(40), 
                                                    `starID` VARCHAR(255),
                                                    FOREIGN KEY (avNum) REFERENCES movies(`avNum`),
                                                    FOREIGN KEY (starID) REFERENCES stars(`starID`),
                                                    UNIQUE(avNum, starID))
                                                    ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;""")
    c.execute(""" CREATE TABLE IF NOT EXISTS `m_g` (`m_g_id` INT AUTO_INCREMENT PRIMARY KEY ,
                                                    `avNum` VARCHAR(40), 
                                                    `genreID` VARCHAR(40), 
                                                    FOREIGN KEY (avNum) REFERENCES movies(avNum),
                                                    FOREIGN KEY (genreID) REFERENCES genre(genreID),
                                                    UNIQUE(avNum, genreID))
                                                    ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;""")
    c.execute(""" CREATE TABLE IF NOT EXISTS `directors` (`di_id` INT AUTO_INCREMENT PRIMARY KEY ,
                                                    `director` VARCHAR(255) UNIQUE)
                                                    ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;""")
    c.execute(""" CREATE TABLE IF NOT EXISTS `producers` (`pr_id` INT AUTO_INCREMENT PRIMARY KEY ,
                                                    `producer` VARCHAR(255) UNIQUE)
                                                    ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;""")
    c.execute(""" CREATE TABLE IF NOT EXISTS `publishers` (`pu_id` INT AUTO_INCREMENT PRIMARY KEY ,
                                                    `publisher` VARCHAR(255) UNIQUE )
                                                    ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;""")
    c.execute(""" CREATE TABLE IF NOT EXISTS `expt_log` (`expt_id` INT AUTO_INCREMENT PRIMARY KEY ,
                                                    `proc_num` integer,
                                                    `expt_type` varchar(40),
                                                    `expt_link` text,
                                                    `expt_info` text,
                                                    `expt_char` text)
                                                    ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;""")

@db_connection_movies
def insert_movie(c, movie):
    # conn = sqlite3.connect('movies.db')
    # c = conn.cursor()

    # with conn:
    c.execute("INSERT INTO `movies` (`avNum`, `title`, `coverImgUrl`, `thumbImgUrl`, `release`, "
              "`length`, `director`, `producer`, `publisher`) VALUES (%s, %s, %s, %s, %s, %s, %s, "
              "%s, %s) ",
              (movie.av_num,
               movie.title,
               movie.cover_img,
               movie.thumb_img,
               movie.release_date,
               movie.length,
               movie.director,
               movie.producer,
               movie.publisher))
    print('movie insert ok')

@db_connection_movies
def insert_star(c, star):
    # conn = sqlite3.connect('movies.db')
    # c = conn.cursor()
    #
    # with conn:
    c.execute(
        "INSERT INTO `stars`  (`starID`, `name`, `birthday`, `age`, `height`, `cups`, "
        "`bust`, `waist`, `hip`, `birthplace`, `hobby`, `portrait`)VALUES (%s, %s, %s, %s, %s, "
        "%s, %s, %s, %s, %s, %s, %s)",
        (star.starid,
         star.name,
         star.birthday,
         star.age,
         star.height,
         star.cups,
         star.bust,
         star.waist,
         star.hip,
         star.birthplace,
         star.hobby,
         star.portrait))


@db_connection_movies
def insert_genre(c, genre):
    # conn = sqlite3.connect('movies.db')
    # c = conn.cursor()
    #
    # with conn:
    c.execute(
        "INSERT INTO `genre` (`genreID`, `genreName`, `category`) VALUES (%s, %s, %s) ",
        (genre.genreid,
         genre.genrename,
         genre.category))


@db_connection_movies
def insert_magnet(c, link):
    # conn = sqlite3.connect('movies.db')
    # c = conn.cursor()
    #
    # with conn:
    c.execute("INSERT INTO `magnets` (`avNum`, `url`, `filename`, `size`, `addTime`) VALUES "
              "(%s, %s, %s, %s, %s)",
              (link.avnum,
               link.url,
               link.filename,
               link.size,
               link.addtime))


@db_connection_movies
def insert_img(c, img, num):
    # conn = sqlite3.connect('movies.db')
    # c = conn.cursor()
    #
    # with conn:
    c.execute("INSERT INTO `images` (`avNum`, `imgUrl`) VALUES (%s, %s) ",
              (num,
               img))


@db_connection_movies
def insert_m_s(c, num, sid):
    # conn = sqlite3.connect('movies.db')
    # c = conn.cursor()
    #
    # with conn:
    c.execute("INSERT INTO `m_s` (`avNum`, `starID`) VALUES (%s, %s)",
              (num,
               sid))


@db_connection_movies
def insert_m_g(c, num, gid):
    # conn = sqlite3.connect('movies.db')
    # c = conn.cursor()
    #
    # with conn:
    c.execute("INSERT INTO `m_g` (`avNum`, `genreID`) VALUES (%s, %s)",
              (num,
               gid))


@db_connection_movies
def insert_movie_info(c, m_link, t_link):
    # conn = sqlite3.connect('movies.db')
    # c = conn.cursor()
    #
    # with conn:
    c.execute("SELECT 1 FROM movie_info WHERE movie_link=%s",
              m_link)

    if c.fetchone() is not None:
        print(m_link, '已存在')
        return
    else:
        c.execute("INSERT INTO `movie_info` (`movie_link`, `thumb_link`) VALUES (%s, %s)",
                  (m_link,
                   t_link
                   )
                  )
        print(m_link, '已插入')


@db_connection_movies
def insert_director(c, director):
    # conn = sqlite3.connect('movies.db')
    # c = conn.cursor()
    #
    # with conn:
    c.execute("INSERT INTO `directors` (`director`) VALUES (%s)",
              director)


@db_connection_movies
def insert_producer(c, producer):
    # conn = sqlite3.connect('movies.db')
    # c = conn.cursor()
    #
    # with conn:
    c.execute("INSERT INTO `producers` (`producer`) VALUES (%s)",
              producer)


@db_connection_movies
def insert_publisher(c, publisher):
    # conn = sqlite3.connect('movies.db')
    # c = conn.cursor()
    #
    # with conn:
    c.execute("INSERT INTO `publishers` (`publisher`) VALUES (%s)",
              publisher)


@db_connection_movies
def insert_expt(c, av_expt):
    # conn = sqlite3.connect('movies.db')
    # c = conn.cursor()
    #
    # with conn:
    c.execute(
        "INSERT INTO `expt_log` (`proc_num`, `expt_type`, `expt_link`, `expt_info`, `expt_char`) "
        "VALUES (%s, %s, %s, %s, %s) ",
        (int(av_expt.proc_num),
         str(av_expt.expt_type),
         str(av_expt.expt_url),
         str(av_expt.expt_info),
         str(av_expt.expt_char)))


@db_connection_movies
def check_existence(c, av_num):
    # conn = sqlite3.connect('movies.db')
    # c = conn.cursor()
    #
    # with conn:
    c.execute("SELECT 1 FROM movies WHERE avNum=%s",
              av_num)

    if c.fetchone() is not None:
        return True
    else:
        # print('not exists')
        return False


@db_connection_movies
def check_links(c, av_num):
    # conn = sqlite3.connect('movies.db')
    # c = conn.cursor()
    #
    # with conn:
    c.execute("SELECT avNum FROM magnets WHERE avNum=%s", av_num)
    if c.fetchone() is not None:
        # print('link exists')
        return True
    else:
        # print('not exists')
        return False


@db_connection_movies
def check_links(c, av_num):
    # conn = sqlite3.connect('movies.db')
    # c = conn.cursor()
    #
    # with conn:
    c.execute("SELECT avNum FROM magnets WHERE avNum=%s", av_num)
    if c.fetchone() is not None:
        # print('link exists')
        return True
    else:
        # print('not exists')
        return False

@db_connection_movies
def check_stars(c, star_id):
    # conn = sqlite3.connect('movies.db')
    # c = conn.cursor()
    #
    # with conn:
    c.execute("SELECT * FROM stars WHERE starID=%s", star_id)
    if c.fetchone() is not None:
        # print('star exists')
        return True
    else:
        print('star not exists')
        return False


@db_connection_movies
def check_genres(c, genre_id):
    # conn = sqlite3.connect('movies.db')
    # c = conn.cursor()
    #
    # with conn:
    c.execute("SELECT * FROM genre WHERE genreID=%s", genre_id)
    if c.fetchone() is not None:
        # print('genre exists')
        return True
    else:
        print('genre not exists')
        return False


@db_connection_movies
def check_director(c, director):
    # conn = sqlite3.connect('movies.db')
    # c = conn.cursor()
    #
    # with conn:
    c.execute("SELECT * FROM directors WHERE director=%s", director)
    if c.fetchone() is not None:
        # print('genre exists')
        return True
    else:
        print('director not exists')
        return False


@db_connection_movies
def check_producer(c, producer):
    # conn = sqlite3.connect('movies.db')
    # c = conn.cursor()
    #
    # with conn:
    c.execute("SELECT * FROM producers WHERE producer=%s", producer)
    if c.fetchone() is not None:
        # print('genre exists')
        return True
    else:
        print('producer not exists')
        return False


@db_connection_movies
def check_publisher(c, publisher):
    # conn = sqlite3.connect('movies.db')
    # c = conn.cursor()
    #
    # with conn:
    c.execute("SELECT * FROM publishers WHERE publisher=%s", publisher)
    if c.fetchone() is not None:
        # print('genre exists')
        return True
    else:
        print('publisher not exists')
        return False

@db_connection_movies
def get_genre_id(c):
    # conn = sqlite3.connect('movies.db')
    # c = conn.cursor()
    #
    # with conn:
    c.execute("select genreid from genre")
    genres = list(c.fetchall())
    return genres


def update_genres(entry_url):
    url = entry_url + 'genre'
    res = requests.get(url)
    res.raise_for_status()

    genresoup = BeautifulSoup(res.text, 'lxml')
    categorys = genresoup.select('div[class="container-fluid pt10"]')
    themes = categorys[0].select('h4')
    for theme in themes:
        # print(theme.text)
        # print(target)
        genreList = theme.next_sibling.next_sibling.select('a')
        # print(genreList)
        # j = 0
        # print(genreList[j]['href'].split('/')[-1])
        for genre in genreList:
            if check_genres(genre['href'].split('/')[-1]) is not True:
                p = Genre(genre['href'].split('/')[-1], genre.text, theme.text)
                print(p)
                insert_genre(p)


if __name__ == '__main__':

    init()
    expt_1 = AvExpt(1, 'test', 'www.dmmsee.icu', 'test_expt')
    insert_expt(expt_1)
    # mov_1 = Movie('testMovie', 'test', 'test', '2019-06-20', 1, 'test', 'test', 'test')
    # star_1 = Star('None', 'None', '1982-06-12', 0, 0, '', 0, 0, 0, '', '', '')
    # genre_1 = Genre('None', 'None', 'None')
    # link_1 = Link('testMovie', 'test', 'test', 'test', datetime.date.today())

    # insert_movie(mov_1)
    # insert_star(star_1)
    # insert_genre(genre_1)

    # entry_url = DOMAIN
    # update_genres(entry_url)
    # insert_magnet(link_1)
    # insert_img('test','testMovie')
    # insert_m_s('testMovie', 'testStar')
    # insert_m_g('testMovie', 'testGenre')
    #
    # check_existence('testMovie')
    # check_stars('testStar')
