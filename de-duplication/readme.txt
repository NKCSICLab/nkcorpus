deleteshort.py：参数为【输入文件名】【输出文件名】，作用为删除长度不足5的数据
detectcn.py：使用langdetect库筛选中文数据
find_duplicates：参数为【输入文件名】【输出文件名】，作用为输出一个文档，文档内每一行都包含后续应当删除的数据的id（目前筛选条件：jaccard相似度高于0.95）
（如果将band进行调整，会影响处理效率和处理效果，目前band数：5）
res.py：正则匹配删除含敏感词数据
deldup：参数为【数据源文件名】【由find_duplicates获得的应删除文件名】【输出文件名】，作用为进行重复数据的删除操作
build.py：将json格式数据转为训练用格式

