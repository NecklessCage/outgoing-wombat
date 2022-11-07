import re

class MMSegmentor:
    def __init__(self):
        # self.PACKAGE_PATH = 'D:\pdy\modules\MyanmarNLPTools\MyanmarNLPTools/'
        self.PACKAGE_PATH = '/mnt/d/PersonalProjects/MM NLP/modules/MyanmarNLPTools/MyanmarNLPTools/'
        self.syl_seg_regex = re.compile(
            r'(?:(?<!္)([\U00010000-\U0010ffffက-ဪဿ၊-၏]|[၀-၉]+|[^က-၏\U00010000-\U0010ffff]+)(?![ှျ]?[့္်]))',
            re.UNICODE)
        with open(self.PACKAGE_PATH + 'words/common-words.txt', encoding='utf8') as f:
            self.common_words = [l.strip() for l in f]
        with open(self.PACKAGE_PATH + 'words/dict-words.txt', encoding='utf8') as f:
            self.dict_words = [l.strip() for l in f]
        with open(self.PACKAGE_PATH + 'words/stop-words.txt', encoding='utf8') as f:
            self.stop_words = [l.strip() for l in f]
        with open(self.PACKAGE_PATH + 'words/znlp-stopwords.txt', encoding='utf8') as f:
            self.more_stop_words = [l.strip() for l in f]
            self.more_stop_words = self.more_stop_words[2:]

        self.all_stop_words = tuple(set(self.stop_words + self.more_stop_words))
        self.CURRENT_WORDS = tuple(set(
            self.common_words + self.dict_words + self.stop_words + self.more_stop_words))


    def stopwords(self):
        return self.all_stop_words


    def syllable_segment(self, text, remove_whitespace=False):
        # `remove_whitespace`: If true, all whitespaces are removed before syllabification, including word separators.
        text = ''.join(text.split()) if remove_whitespace else text
        return [syl.strip() for syl in self.syl_seg_regex.sub(r'𝕊\1', text).strip('𝕊').split('𝕊')]


    def word_segment(self, text, additionalWordList=[]):
        '''
        `text` is a str.
        '''
        text = self.syllable_segment(str(text).replace(' ', ''))

        result = []
        offset = 0
        LIMIT = 7

        while offset < len(text):
            chunk_end = offset + LIMIT
            chunk_found = False

            for i in range(chunk_end, offset, -1):
                chunk = ''.join(text[offset:i])

                if chunk in self.CURRENT_WORDS or chunk in additionalWordList:
                    # Found the word in data
                    chunk_found = True
                    result.append(chunk)

                    # Resetting offset to resume
                    offset = i
                    break

            # Didn't find the word of any
            # long-short combination in the chunk
            if not chunk_found:
                # Now, the current syllable is a word
                result.append(text[offset])
                offset += 1
        return result
