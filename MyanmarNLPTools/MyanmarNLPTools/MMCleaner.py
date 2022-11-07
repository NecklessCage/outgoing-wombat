class MMCleaner:
    def __init__(self):
        pass

    def web_clean(self, text):
        '''
        `text` is a str.
        '''
        # \u200* are 0-width space that come from webpages.
        # \xa0 etc. are generally ascii-unicode confused characters
        # there can be many more for cleaning
        return ' '.join(text.strip().replace(
            '\u200a','').replace(
            '\u200b','').replace(
            '\u200c','').replace(
            '\xa0','').replace(
            '\n','<newline>').split())
            