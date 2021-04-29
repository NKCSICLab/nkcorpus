import hashlib

hash_table = set()
def detect_web(mes:str):
    mes = mes.split('\n')
    del mes[-1]
    hash_result = ''
    for line in mes:
        hash = hashlib.md5()
        hash.update(bytes(line,encoding='utf-8'))
        i = hash.hexdigest()
        if i in a:
            continue
        else:
            line+='\n'
            hash_result+=line
    return hash_result
