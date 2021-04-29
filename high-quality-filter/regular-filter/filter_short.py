end_char = ['，', '。', '？', '’', '”', '…'] #结束符

#汉语有16种符号，下面包括了除冒号：，括号（），间隔号<>，着重号（字下点），专名号以外的其他符号
ch_char = ['。', '，', '；', '、', '？', '！', '‘', '’', '…', '——', '《', '》']
FILTER_LENGTH = 150

def detect_short(mes:str):
    check_end = 0
    for i in range(len(mes)-1, -1, -1):
        s = mes[i]
        if(s in end_char):
           mes = mes[0:i+1]
           check_end = 1
           break
    if check_end:
        mes = mes.split('\n')
        mes_result = ''
        for line in mes:
            for line_char in line:
                if(line_char in ch_char):
                    line+='\n'
                    mes_result+=line
                    break
        if(len(mes_result)<FILTER_LENGTH):
            return ''
        return mes_result
    else:
        return ''