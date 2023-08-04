# -*- coding: utf-8 -*-
from pynori.korean_analyzer import KoreanAnalyzer

nori = KoreanAnalyzer(
           decompound_mode='NONE', # DISCARD or MIXED or NONE
           infl_decompound_mode='NONE', # DISCARD or MIXED or NONE
           discard_punctuation=False,
           output_unknown_unigrams=False,
           pos_filter=False,
           synonym_filter=False, mode_synonym='NORM', # NORM or EXTENSION
)

# NNG, NNP (명사, 대명사) filter
# def _filter(term):
#     result = []
#     for _idx, _tag in enumerate(term['posTagAtt']):
#         if _tag in ['NNG', 'NNP', 'VA']:
#             result.append(term['termAtt'][_idx])
#     return result

# Analyzer
def _do_analysis(text):
    # return _filter(nori.do_analysis(text))
    return nori.do_analysis(text)

sample = '업무를 미루지 않고 매사에 최선을 다하는 모습 보기 좋습니다.'
full_analyzed_list = list(zip(nori.do_analysis(sample)['termAtt'], nori.do_analysis(sample)['posTagAtt']))
print(full_analyzed_list)