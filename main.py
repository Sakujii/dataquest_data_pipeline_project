"""
This is the main python file of the project.
We are building a pipeline based on previously created Pipeline class.
We read data from a json file and process it using Pipeline tasks.
"""

from datetime import datetime
import json
import io
import csv
import string

from pipeline import build_csv, Pipeline
from stop_words import stop_words

pipeline = Pipeline()


"""
This function reads json file to a dict of stories
Parameters: none
Return values: dict of stories
"""

@pipeline.task()
def file_to_json():
    with open('hn_stories_2014.json', 'r') as f:
        data = json.load(f)
        stories = data['stories']
    return stories



""" 
This function filters popular stories, that have more than 50 points, 
more than 1 comment and do not start with string "Ask HN"
Parameters: dict of stories list from file_to_json task
Return values: dict of filtered stories
"""

@pipeline.task(depends_on = file_to_json)
def filter_stories(stories):
    def is_popular(story):
        return story['points'] > 50 and story['num_comments'] > 1 and not story['title'].startswith('Ask HN')
    
    return (
        story for story in stories
        if is_popular(story)
    )



"""
This function strips needed data from stories 
and creates a StringIO CSV object for other functions to use as a 'file'
Parameters: filtered stories from filter_stories task
Return values: a StringIO CSV object
"""

@pipeline.task(depends_on = filter_stories)
def json_to_csv(stories):
    lines = []
    for story in stories:
        lines.append(
            (story['objectID'], datetime.strptime(story['created_at'], "%Y-%m-%dT%H:%M:%SZ"), story['url'], story['points'], story['title'])
        )
    return build_csv(lines, header=['objectID', 'created_at', 'url', 'points', 'title'], file=io.StringIO())



""" 
This function strips all the titles from the CSV file object
Parameters: StringIO CSV object from json_to_csv task
Return values: list of titles
"""

@pipeline.task(depends_on = json_to_csv)
def extract_titles(csv_file):
    reader = csv.reader(csv_file)
    header = next(reader)
    idx = header.index('title')
    
    return (line[idx] for line in reader)



"""
This function cleans the titles to lowercase and removes any punctutation
Parameters: titles from extract_titles task
Return values: list of cleaned titles
"""

@pipeline.task(depends_on = extract_titles)
def clean_title(titles):
    for title in titles:
        title = title.lower()
        # Punctuation marks defined in "string.punctuation" string
        title = ''.join(c for c in title if c not in string.punctuation)
        yield title



"""
This funtion counts word frequency for all the titles and returns a dict
Parameters: cleaned titles from clean_title task
Return values: dict of title words and their frequencies
"""

@pipeline.task(depends_on = clean_title)
def build_keyword_dict(titles):
    word_freq = {}
    for title in titles:
        for word in title.split(' '):
            # Ignoring most common stop words in English
            if word and word not in stop_words:
                if word not in word_freq:
                    word_freq[word] = 1
                word_freq[word] += 1
    return word_freq




"""
This function creates a tuple of words and their frequencies
Parameters: dict of word frequencies from build_keyword_dict task
Return values: tuple of 100 most frequent words and their frequencies
"""
@pipeline.task(depends_on = build_keyword_dict)
def top_keywords(word_freq):
    freq_tuple = [
        (word, word_freq[word])
        for word in sorted(word_freq, key=word_freq.get, reverse=True)
    ]
    return freq_tuple[:100]


run = pipeline.run()
print(run[top_keywords])