
from tqdm import tqdm
import pandas as pd
from nltk import ngrams
from MyanmarNLPTools import MMSegmentor
seg = MMSegmentor.MMSegmentor()


def search_by_syls(keyword, message):
    nsyls = len(keyword if isinstance(keyword, list)
                else seg.syllable_segment(keyword))
    syls = message if isinstance(
        message, list) else seg.syllable_segment(message)
    ng = [''.join(x) for x in ngrams(syls, nsyls)]
    return keyword in ng


def find_hs(df, hs_terms):
    hs_found = []

    for _, r in tqdm(df.iterrows()):
        row_lvl = []
        for hs_term in hs_terms:
            if search_by_syls(hs_term, r.msg_seg):
                row_lvl.append(hs_term)
        hs_found.append(row_lvl)

    df['hs_found'] = hs_found
    return df


def search_persons(df, people_terms):
    people_dict = {}

    # for each person
    for _, person_keywords in tqdm(people_terms.items()):
        person_en_name = person_keywords[1].replace(' ', '')
        people_dict[person_en_name] = []
        for _, r in df.iterrows():  # for each post
            row_lvl = []
            for term in person_keywords:  # for each keyword for the given person
                if search_by_syls(term, r.msg_seg):
                    row_lvl.append(term)
            people_dict[person_en_name].append(row_lvl)

    return people_dict


def format_df(df, people_dict):
    dct = {
        'date': df.datetime_posted.dt.date,
        'time': df.datetime_posted.dt.time,
        'post_url': df.post_url,
        'message': df.msg,
        'hs_terms_found': df.hs_found
    }
    terms_counts = {'nHsTermsFound': [len(l) for l in df.hs_found]}

    dct.update(people_dict)
    dct.update({
        f'n{person_en_name}': [len(l) for l in terms_found] for person_en_name, terms_found in people_dict.items()
    })
    dct.update(terms_counts)

    data = pd.DataFrame(dct).sort_values(['date', 'time'], ascending=False)
    return data
