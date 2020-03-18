# Created at 2020-03-09 by Roger
# This is a censor av model module


class Movie:
    """ class for each censor movie in javbus website"""

    def __init__(self, av_num, title, cover_img_url, thumb_img_url, date, length, director,
                 producer, publisher):
        self._av_num = av_num
        self._title = title
        self._cover_img = cover_img_url
        self._thumb_img = thumb_img_url
        if date == '0000-00-00':
            self._release_date = '1900-10-10'
        else:
            self._release_date = date
        self._length = length
        self._director = director
        self._producer = producer
        self._publisher = publisher

    @property
    def av_num(self):
        return self._av_num

    @property
    def title(self):
        return self._title

    @property
    def cover_img(self):
        return self._cover_img

    @property
    def thumb_img(self):
        return self._thumb_img

    @property
    def release_date(self):
        return self._release_date

    @property
    def length(self):
        return self._length

    @property
    def director(self):
        return self._director

    @property
    def producer(self):
        return self._producer

    @property
    def publisher(self):
        return self._publisher

    def __repr__(self):
        return '{} {} {} {} {} {} {} {}'.format(self._av_num, self._title, self._cover_img,
                                                self._thumb_img, self._release_date, self._length,
                                                self._director, self._producer, self._publisher)


class Star:
    """ class for each censor star in javbus website"""

    def __init__(self, star_id,
                 name, birthday,
                 age, height,
                 cups, bust,
                 waist, hip,
                 birthplace, hobby, portrait):
        self._starid = star_id
        self._name = name
        self._birthday = birthday
        self._age = age
        self._height = height
        self._cups = cups
        self._bust = bust
        self._waist = waist
        self._hip = hip
        self._birthplace = birthplace
        self._hobby = hobby
        self._portrait = portrait

    @property
    def starid(self):
        return self._starid

    @property
    def name(self):
        return self._name

    @property
    def birthday(self):
        return self._birthday

    @property
    def age(self):
        return self._age

    @property
    def height(self):
        return self._height

    @property
    def cups(self):
        return self._cups

    @property
    def bust(self):
        return self._bust

    @property
    def waist(self):
        return self._waist

    @property
    def hip(self):
        return self._hip

    @property
    def birthplace(self):
        return self._birthplace

    @property
    def hobby(self):
        return self._hobby

    @property
    def portrait(self):
        return self._portrait

    def __repr__(self):
        return '{} {} {} {} {} {} {} {} {} {} {} {}'.format(self._starid, self._name,
                                                            self._birthday,
                                                            self.age, self._height, self.cups,
                                                            self._bust, self._waist, self._hip,
                                                            self._birthplace, self._hobby,
                                                            self._portrait)


class Genre:
    """ class for each censor genre in javbus website"""

    def __init__(self, genre_id, genre_name, category):
        self._genreid = genre_id
        self._genreName = genre_name
        self._category = category

    @property
    def genreid(self):
        return self._genreid

    @property
    def genrename(self):
        return self._genreName

    @property
    def category(self):
        return self._category

    def __repr__(self):
        return '{} {} {}'.format(self._genreid, self._genreName, self._category)


class Link:
    """ Class for magnet link"""

    def __init__(self, avnum, magnet, filename, size, add_time):
        self._avnum = avnum
        self._magnet = magnet
        self._filename = filename
        self._size = size
        self._addtime = add_time

    @property
    def size(self):
        return self._size

    @property
    def url(self):
        return self._magnet

    @property
    def avnum(self):
        return self._avnum

    @property
    def filename(self):
        return self._filename

    @property
    def addtime(self):
        return self._addtime

    def __repr__(self):
        return '{} {} {} {} {}'.format(self._avnum, self._magnet, self._filename, self._size,
                                       self._addtime)


class Counter:
    """ Class for counter"""

    def __init__(self):
        self._parsing_time = 0
        self._page_skip = 0
        self._movie_skip = 0

    @property
    def parsing_time(self):
        return self._parsing_time

    @property
    def page_skip(self):
        return self._page_skip

    @property
    def movie_skip(self):
        return self._movie_skip

    def reset_movie_skip(self):
        self._movie_skip = 0

    def increment_movie_skip(self):
        self._movie_skip += 1

    def increment_page_skip(self):
        self._page_skip += 1

    def increment_parse(self):
        self._parsing_time += 1


class AvExpt(Exception):
    """ Class for exception information"""

    def __init__(self, proc_num, expt_type, expt_url, expt_info, expt_char=''):
        # this is a function to initialize class error
        # there is five types of error:
        # genreid, list_page, movie_page, star_page, link_page
        self._proc_num = proc_num
        self._expt_type = expt_type
        self._expt_url = expt_url
        self._expt_info = expt_info
        self._expt_char = expt_char

    def change_type(self, new_expt_type):
        self._expt_type = new_expt_type
        return

    @property
    def proc_num(self):
        return self._proc_num

    @property
    def expt_type(self):
        return self._expt_type

    @property
    def expt_url(self):
        return self._expt_url

    @property
    def expt_info(self):
        return self._expt_info

    @property
    def expt_char(self):
        return self._expt_char

    def __str__(self):
        return '{} {} {} {} {}'.format(self._proc_num, self._expt_type,
                                       self._expt_url, self._expt_info, self._expt_char)
