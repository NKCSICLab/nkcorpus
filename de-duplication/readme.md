deleteshort.py：

参数：输入文件名，输出文件名，长度限制 (str,str,int)

删除长度小于长度限制的数据

····························································


find_duplicates：

参数：输入文件名，输出文件名，seednum, bandnum (str,str,int,int)

seednum为使用的minhash个数，bandnum为将文件指纹切块的块数。

作用为输出一个文档，文档内每一行都包括一对发生了哈希碰撞的数据的id。

需要满足seednum%bandnum=0

相同bandnum下，seednum越大，运行时间越长，去重效果越好。

相同seednum下，bandnum越大，运行时间越长，去重效果越好。


····························································

calculate_sim.py：

参数：由find_duplicates得到的文件的文件名，数据源文件名，输出文件名，jaccardmin(str,str,str,float)

jaccardmin的范围为0~1，对wiki数据集，推荐使用0.95作为阈值

作用从所有发生哈希碰撞的数据中，筛选出jaccard相似度高于jaccardmin的数据并输出将要删除的数据id。

····························································

deldup：

参数：数据源文件名，由calculate_sim获得的包含应删除数据id的文件名，输出文件名 (str,str,str)

作用为进行重复数据的删除操作。

需要保证数据源文件中id单调递增。

····························································


数据清洗流程：



1.使用find_duplicates.py进行hash碰撞

2.使用calculate_sim.py对发生碰撞的数据对进行相似度计算

3.使用deldup删除相似度高于阈值的数据。

4.使用build.py将json格式数据转化为训练用格式

输入json数据中，必须要有“text"和"id"字段。

····························································

update:add_fingerprint.py&new_find_dup.py

add_fingerprint.py:

参数：输入文件名，输出文件名，seednum, bandnum (str,str,int,int)

作用：为每条数据加入一个长度为seednum个minhash的数据指纹。加入后的文件格式仍为json文件，在每条数据最后加入'fingerprint'字段。

new_find_dup.py

参数：输入文件名，输出文件名，seednum, bandnum (str,str,int,int)

作用：如果数据没有加入minhash构成的数据指纹，正常进行哈希碰撞

如果数据已经加入数据指纹，直接读取数据指纹进行哈希碰撞。

在数据已加入数据指纹后，运行效率远高于直接进行哈希碰撞。