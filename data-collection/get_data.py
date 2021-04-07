# coding=UTF-8

import requests
from requests.adapters import HTTPAdapter
from requests import exceptions
import os
import time
import traceback
import sys
from tqdm import tqdm
from urllib.request import urlopen

s = requests.Session()
s.mount('http://', HTTPAdapter(max_retries=5))
s.mount('https://', HTTPAdapter(max_retries=5))
begin_file = "begin_num_%s.txt"%sys.argv[1]
path_file = "wet_paths/CC-MAIN-2021-10/wet.paths"
url_front="https://commoncrawl.s3.amazonaws.com/" #url前缀
def request_big_data(file_name, num):
    #创建新文件夹
    split_position = file_name.rfind('/')
    file_path = file_name[0:split_position]
    mkdir(file_path)

    #参数
    url = url_front + file_name
    #获取下载内容
    file_chunk=0
    count=0
    while True:
        if count==0: f = open(file_name, 'wb')
        else: f = open(file_name, 'ab')
        if count<5:
            try:
                headers = {'Range': 'bytes=%d-' % (file_chunk*1024)}
                r = s.get(url, stream=True, headers=headers, timeout=10)
                print(file_name+"获取成功")

                #下载进度条
                file_size = int(urlopen(url).info().get('Content-Length', -1))
                pbar = tqdm(
                    total=file_size, initial=file_chunk*1024,
                    unit_scale=True, desc=file_name.split('/')[-1], ncols=100)
    
                #写入
                for chunk in r.iter_content(chunk_size = 1024):
                    if chunk:
                        f.write(chunk)
                        file_chunk+=1
                        pbar.update(1024)
                pbar.close()
                f.close()
                r.close()
                del(r) 
                break
            except exceptions.ConnectionError:
                f.close()
                pbar.close()
                r.close()
                del(r) 
                time.sleep(2)
                print("\n第%d次重试"%(count+1))
                count+=1
                continue
        else:
            raise Exception("五次网络超时")
            break

#创建文件夹
def mkdir(file_path):
    path = os.getcwd() + '/' + file_path
    folder = os.path.exists(path)
    if not folder:
        os.makedirs(path)
        print("--- new folder... ---")
        print(path)

if __name__=="__main__":
    i = 0
    keyboard_interrupt = False
    with open(path_file) as lines:
        f = open(begin_file,'r')
        range_i = eval(f.read())
        begin_num = range_i["begin_num"]
        end_num = range_i["end_num"]
        begin_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        for line in lines:
            i=i+1
            if(i<begin_num):
                continue
            if(i>end_num):
                break
            line=line.replace('\n','')
            print("--准备下载第%d个文件--"%i)
            while True:
                try:
                    request_big_data(line, i)
                    time.sleep(2)
                    break
                except KeyboardInterrupt as e:
                    f.close()
                    f = open(begin_file,'w')
                    f.write(str({"begin_num":i,"end_num":end_num}))
                    f.close()
                    end_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                    f = open("record_%s.txt"%sys.argv[1],'a')
                    f.write("%s-%s  %d-%d号文件下载完成 路径文件为%s\n"%(begin_time,end_time,begin_num,i-1,path_file))
                    f.close()
                    print('\n'+str(traceback.print_exc()))
                    keyboard_interrupt = True
                    break
                except Exception as e:
                    print(str(e))
                    print("遭遇异常，停止下载1分钟")
                    time.sleep(60)
                    continue
            if keyboard_interrupt: break
    if(i>end_num):
        end_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        f = open("record_%s.txt"%sys.argv[1],'a')
        f.write("%s-%s  %d-%d号文件下载完成 路径文件为%s\n"%(begin_time,end_time,begin_num,i-1,path_file))
        f.write("该任务下载完成\n")
        f.close()
    print("已写入日志文件")
