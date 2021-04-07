import re
import json
from mpi4py import MPI

def write_json(jlist):
      # 将bx列表写入json文件
      with open('train'+str(rank)+'.json', 'w') as f_obj:
          json.dump(jlist, f_obj)

comm = MPI.COMM_WORLD
rank=comm.Get_rank()

#读取过滤词汇并放入列表
result=[]
with open('remove_data.txt', "r") as f:
    remove_text=f.readlines()
    for line in remove_text:
        result.append(line.strip('\n'))

# print(result)

#多行的json处理
file = open('test'+str(rank)+'.json', 'r', encoding='utf-8')
papers = []
clean_data=[]
for line in file.readlines():
    dic = json.loads(line)
    papers.append(dic)

#正则表达式匹配过滤
for data in papers:
    is_clean=True
    for bad_data in result:
        #正则表达式,search是寻找整个文本
        temp=re.search(bad_data,str(data))
        if  temp:
            is_clean = False
            break
    if is_clean:
        clean_data.append(data)
write_json(clean_data)