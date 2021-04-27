# coding=UTF-8
from warcio.archiveiterator import ArchiveIterator
import sys
from bs4 import UnicodeDammit
from langdetect import detect_langs


def load_and_write(data, record, out):
    url = record.rec_headers.get_header('WARC-Target-URI')
    date = record.rec_headers.get_header('WARC-Date')
    length = record.rec_headers.get_header('Content-Length')
    dic = {"url": url, "date": date, "length": length, "data": data}
    out.write(str(dic) + "\n")


def detect_ch(name):
    i = 0
    with open(name, 'rb') as stream:
        with open(name + '.txt', 'w+', encoding="utf-8") as out:
            for record in ArchiveIterator(stream):
                data = record.content_stream().read()
                # code0 = chardet.detect(data)["encoding"]
                code = UnicodeDammit(data)
                if code.original_encoding is not None:
                    data = str(data, encoding=code.original_encoding)
                    if str(record.rec_headers.get_header('WARC-Identified-Content-Language')).find('zho') != -1:
                        try:
                            lang_type = detect_langs(data)
                        except Exception:
                            print(str(Exception))
                        if lang_type[0].lang == 'zh-cn' or lang_type[0].lang == 'zh-tw':
                            load_and_write(data, record, out)
                # print(record.rec_headers)
                i = i + 1
                print(i)
                if i == 0:
                    break


if __name__ == "__main__":
    detect_ch(sys.argv[1])
