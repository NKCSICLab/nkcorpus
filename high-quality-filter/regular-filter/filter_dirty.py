from flashtext import KeywordProcessor

PER_DIRTY = 30
PER_DIRTY_SEX = 60
def load_dirtytable(file_name):
    is_title = 1
    table = {}
    with open(file_name, 'r', encoding='GBK') as lines:
        for line in lines:
            if is_title:
                is_title = 0
                continue
            line = line.replace('\n', '')
            min_word = line.split('\t')
            type_min = min_word[1]
            word_min = min_word[3]
            if type_min in table.keys():
                table[type_min].append(word_min)
            else:
                table.update({type_min:[word_min]})
    keyword_processor.add_keywords_from_dict(table)
    return

#创建不良词汇表
keyword_processor = KeywordProcessor()
load_dirtytable('min_word/min_word.txt')
DIRTY_TYPE_0 = '色情'
DIRTY_TYPE_1 = '反动'
DIRTY_TYPE_2 = '暴恐'
DIRTY_TYPE_3 = '民生'
DIRTY_TYPE_4 = '其他'
def detect_dirty(data):
    key_words_found = keyword_processor.extract_keywords(data)
    len_data = len(data)
    len_dirty = len(key_words_found)
    len_dirty_type = [key_words_found.count(DIRTY_TYPE_0),  \
                      key_words_found.count(DIRTY_TYPE_1),  \
                      key_words_found.count(DIRTY_TYPE_2),  \
                      key_words_found.count(DIRTY_TYPE_3),  \
                      key_words_found.count(DIRTY_TYPE_4)]
    
    if(len_dirty!=0 and len_data/len_dirty<PER_DIRTY):
        return ''
    elif(len_dirty_type[0]!=0 and len_data/len_dirty_type[0]<PER_DIRTY_SEX):
        return ''
    else:
        return data
