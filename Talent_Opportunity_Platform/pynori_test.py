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
def _filter(term):
    result = []
    for _idx, _tag in enumerate(term['posTagAtt']):
        if _tag in ['NNG', 'NNP']:
            result.append(term['termAtt'][_idx])
    return result

# Analyzer
def _do_analysis(text):
    return _filter(nori.do_analysis(text))

print(_do_analysis('많이 바쁘신걸로 알고 있는데..  바쁘신 와중에도 친절히 시스템 설명도 해주시고.. 늘 고맙습니다.   건강유의하세요, 타에 모범이 되시며, 본받을점이 많습니다.   열심히 그리고 적극적으로 업무처리 하시는 모습에 항상 감사드립니다. 칭찬합니다., 바쁘실텐데고 불구하고 어려운 점이나 모르는 일이 생겼을 때 해결해주시고 알려주셔서 정말 감사합니다. 앞으로도 대리님과 계속 같이 일하고 싶습니다~ ㅎㅎ, 말하지 않아도 다른 사람에게 도움이 되는 일을 알아서 척척 해 주시니 감동입니다.평가지원서비스도 그렇고, CDP분석도 그렇고, 넘 잘하셨고, 고마운 마음 가득이에요~, 아침부터 주주총회 안내 및 지원하느라고 고생 많았습니다.'))