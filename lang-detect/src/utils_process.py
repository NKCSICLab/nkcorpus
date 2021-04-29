# coding=UTF-8
import sys
import re

from langdetect import detect
from langdetect import detect_langs
from langdetect import lang_detect_exception
from warcio.archiveiterator import ArchiveIterator

def load_and_write(data, record, out):
    url = record.rec_headers.get_header('WARC-Target-URI')
    date = record.rec_headers.get_header('WARC-Date')
    length = record.rec_headers.get_header('Content-Length')
    dic = {"url":url,"date":date,"length":length,"data":data}
    out.write(str(dic)+"\n")

def is_ch(char): #中文字符及常用符号表
    if(char>'\u2000' and char<'\u206f' \
        or char>'\u3000' and char<'\u303f' \
        or char>'\u4e00' and char<'\u9fef' \
        or char>'\uff00' and char<'\uffef'):
        return 1
    else:
        return 0

def detect_ch(record):
    data=''
    if(str(record.rec_headers.get_header('WARC-Identified-Content-Language')).find('zho')!=-1):
        lines = record.content_stream().read()
        lines = str(lines, encoding='utf-8')
        lines = lines.split('\n')

        for line in lines:
            if(len(line)<=5):
                continue
            end_num = len(line)
            ch_num = 0
            for r in line:
                if(r>'0' and r<'9' or r==' ' or r=='.'):#对于数字、空格等不计入
                    end_num-=1
                else:
                    ch_num+=is_ch(r)
            if(ch_num>end_num*0.8 \
                or ch_num>end_num*0.7 and ch_num>50 \
                or ch_num>end_num*0.6 and ch_num>150):
                data+=line
                data+='\n'
    return data