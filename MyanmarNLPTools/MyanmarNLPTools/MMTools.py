from myanmartools import ZawgyiDetector
from icu import Transliterator
import re


DETECTOR = ZawgyiDetector()
CONVERTER = Transliterator.createInstance('Zawgyi-my')

DIGIT_DICT = {
    'ဝ':'0', # wa lone -- myanmar character that looks identical to the zero in most fonts
    '၀':'0',
    '၁':'1',
    '၂':'2',
    '၃':'3',
    '၄':'4',
    '၅':'5',
    '၆':'6',
    '၇':'7',
    '၈':'8',
    '၉':'9'
}
DIGIT_DICT_EN = {
    k:v for k,v in zip(DIGIT_DICT.values(), DIGIT_DICT.keys())
}

def digit_mm2en(mmstr):
    return ''.join(DIGIT_DICT[s] if s in DIGIT_DICT.keys() else s for s in mmstr)


def digit_en2mm(enstr):
    return ''.join(DIGIT_DICT_EN[s] if s in DIGIT_DICT_EN.keys() else s for s in enstr)

def is_zawgyi_probability(text):
    return DETECTOR.get_zawgyi_probability(text)

def zg2uni(text):
    return CONVERTER.transliterate(text)

def normalize_unicode(text, probability_threshold=0.95):
    # `probability_threshold`: probability beyond which we decide it's written in zawgyi
    return CONVERTER.transliterate(text) if DETECTOR.get_zawgyi_probability(text) > probability_threshold else text





URL_EXTRACTOR = re.compile(r'(?P<url>https?://[^\s]+)')

def extract_urls(text):
    return URL_EXTRACTOR.findall(text)

def remove_urls(text):
    return URL_EXTRACTOR.sub('', text)