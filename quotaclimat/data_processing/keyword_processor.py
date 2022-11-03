from flashtext import KeywordProcessor


class KeywordModel:

    def __init__(self,keyword_dict):

        self.model = KeywordProcessor()
        self.model.add_keywords_from_dict(keyword_dict)

    def extract_position_mentions(self,text):
        keywords = self.model.extract_keywords(text,span_info = True)
        keywords = [x[1]/len(text) for x in keywords]
        return keywords

    def extract_mentions(self,data):

        data["mentions"] = data["text"].map(self.extract_position_mentions)
        data["n_mentions"] = data["mentions"].map(len)
    
        return data