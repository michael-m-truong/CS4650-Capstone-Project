from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime, timedelta
import csv
import re
import emoji
import math            # for some stupid reason its the main csv file its floats instead of ints for followers
from collections import Counter
# import nltk
# from nltk.corpus import stopwords

class Tweet(MRJob):

    unique_tweets = set()
    viewingBTC_Price = False
    BTCPrices = dict()

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                reducer=self.shuffler),
            MRStep(reducer=self.reducer)
        ]
    
    def mapper(self, _, line):
        stop_words = [
            'the', 'and', 'a', 'of', 'in', 'to', 'that', 'it', 'with', 'for', 'on', 'is', 'can', 'below', 'him', 'some', 'against', 'the', 'did', "she's", 'which', 'but', 'yourself', 'if', 'y', 'what', "you've", 'is', 'myself', 'ours', 'further', 'out', 'own', 'most', "isn't", 'its', 'we', "haven't", 'mustn', 'by', "don't", 'didn', 'should', 'there', 'of', 'were', 'won', 'how', 'more', 'as', 'any', 'very', "couldn't", 'hasn', 'needn', 'now', 'ain', 'again', 'so', 'where', 'them', 'your', 'before', 'why', 'after', 'under', "should've", "won't", 'their', 'than', 'and', "that'll", 'whom', 'because', 'are', "aren't", 'on', 'weren', 'yours', 'doesn', "mustn't", 'nor', "hadn't", 'am', 'couldn', 'during', 'same', 'yourselves', 'mightn', 'was', 'up', 'ma', 've', "didn't", 'over', 're', 'once', "mightn't", 'who', 'these', 'those', 'he', 'only', 'm', 'being', 'wasn', 'shouldn', 'it', 'do', 'have', 'doing', 'my', "shouldn't", 'above', "hasn't", 'ourselves', 'no', 'both', 'a', 'such', 'between', 'not', "wouldn't", 'off', "you're", "you'll", 'then', 'd', 'o', 'does', 'or', 'hers', 'our', "wasn't", 'hadn', 'other', 'aren', 'will', 'about', 'don', 'shan', "it's", 'just', 'too', 'while', 'his', 'in', 'through', 'themselves', 'that', 'having', 'until', 'at', 'few', 'theirs', 'each', "needn't", 'for', 'you', 'herself', 'itself', 'she', 'has', 't', 'to', 'haven', 'this', 'here', 'with', 'himself', 'an', 's', 'isn', 'had', "you'd", 'into', 'they', 'll', 'her', 'when', 'wouldn', 'from', "weren't", 'all', "doesn't", "shan't", 'down', 'been', 'be', 'me', 'i','\\' 
        ]
        stop_words = set(stop_words)
        fields = line.strip().split(',')
        if line == ("2016-01-01,430.721008,436.246002,427.515015,434.334015,434.334015,36278900"):  #start of BTC-USD.csv
            Tweet.viewingBTC_Price = True
            return
        if Tweet.viewingBTC_Price:
            BTCDate, openPrice, high, low, closePrice, adjClose, volume = fields[0], fields[1], fields[2], fields[3], fields[4], fields[5], fields[6]
            #yield BTCDate, {"openPrice": openPrice, "closePrice": closePrice} 
            Tweet.BTCPrices[BTCDate] = {"openPrice": openPrice, "closePrice": closePrice} 
            #yield BTCDate, Tweet.BTCPrices


        elif len(fields) >= 13: # viewing bitcoin_tweets.csv;   need to make sure there are 13 columns in a line
            # Extract the desired fields
            if fields[0]== '':
                return
            user_name = fields[0]
            user_location = fields[1]
            user_description = fields[2]
            #yield "test", {"idk": fields[8], "idk2": fields[3]}
            try:
                #datetime.strptime(fields[3], '%Y-%m-%d %H:%M:%S')                #check
                dt_obj = datetime.strptime(fields[3], '%Y-%m-%d %H:%M:%S')
                date_str = dt_obj.strftime('%Y-%m-%d')
                user_created = date_str
            except ValueError:
                return               #discard lines that dont have date       
            try:
                user_followers = float(fields[4])
            except ValueError:     
                return           #discard lines that dont have proper type      
            try:
                user_friends = math.floor(float(fields[5]))
            except ValueError:
                return
            try:
                user_favourites = int(fields[6])  # thats how they spelled favorites in the csv
            except ValueError:
                user_favourites = None
                # return
            try:
                user_verified = bool(fields[7])  
            except ValueError:
                user_verified = None
                # return
            try:
                dt_obj = datetime.strptime(fields[8], '%Y-%m-%d %H:%M:%S')
                date_str = dt_obj.strftime('%Y-%m-%d')
                date = date_str
            except:
                return 
            text = re.sub(r'[^\x00-\x7F]+', '', fields[9])
            # Remove URLs
            text = re.sub(r'http\S+', '', text)
            # Convert to lowercase
            text = text.lower()
            # remove mentions
            text = re.sub(r'@\w+',' ', text)
            # remove utf-16 stuff
            text = re.sub(r'\\u\w{4}', '', text)
            # Remove stop words
            words = text.split()
            words = [word for word in words if word not in stop_words]
            cleaned_text = ' '.join(words)
            PUNC_RE = re.compile(r"[^a-z,]")
            cleaned_text = re.sub(PUNC_RE, ' ', cleaned_text)
            text = cleaned_text

            hashtags = re.findall(r'#\w+', text)  # get hashtags in description
            source = fields[11]
            try:
                is_retweet = bool(fields[12])  # thats how they spelled favorites in the csv lol
            except ValueError:
                return
            
            # Check if tweet is a duplicate
            tweet_key = (user_name, cleaned_text)
            if tweet_key in Tweet.unique_tweets:
                # yield user_name, {                 # there are a lot of duplicate tweets
                # "user_location": user_location,
                # "user_description": user_description,
                # "user_created": user_created,
                # "user_followers": user_followers,
                # "user_friends": user_friends,
                # "user_favourites": user_favourites,
                # "user_verified": user_verified,
                # "date": date,
                # "text": text,
                # "hashtags": hashtags,
                # "source": source,
                # "is_retweet": is_retweet
                # }
                return

            Tweet.unique_tweets.add(tweet_key)
            yield date, {
                "user_name": user_name,
                "user_location": user_location,
                "user_description": user_description,
                "user_created": user_created,
                "user_followers": user_followers,
                "user_friends": user_friends,
                "user_favourites": user_favourites,
                "user_verified": user_verified,
                "text": text,
                "hashtags": hashtags,
                "source": source,
                "is_retweet": is_retweet
            }
            #yield user_name, text

    def shuffler(self, key, values):
        # make the BTCDate the key and make the 
        #tweets = 

        # for tweet in values:
        #     yield key, tweet

        #yield key, Tweet.BTCPrices
        if key not in Tweet.BTCPrices:
            return

        openPrice = Tweet.BTCPrices[key]['openPrice']
        closePrice = Tweet.BTCPrices[key]['closePrice']
        for tweet in values:
            user_name = tweet['user_name']
            user_location = tweet['user_location']
            user_description = tweet['user_description']
            user_created = tweet['user_created']
            user_followers = tweet['user_followers']
            user_friends = tweet['user_friends']
            user_favourites = tweet['user_favourites']
            user_verified = tweet['user_verified']
            text = tweet['text']
            hashtags = tweet['hashtags']
            source = tweet['source']
            is_retweet = tweet['is_retweet']
            yield key, {
                "user_name": user_name,
                "text": text,
                "hashtags": hashtags,
                "openPrice": openPrice,
                "closePrice": closePrice
            }
            #yield key, tweet
    
    def reducer(self, key, values):
        # # Combine all the tweets from a user into a single list
        # tweets = list(values)
        
        # # Yield the user and their list of tweets
        # yield user_name, tweets

        # tweet_count = sum(1 for _ in values)
        # yield key, tweet_count
        date_obj = datetime.strptime(key, "%Y-%m-%d").date()
        next_day = date_obj + timedelta(days=1)   #date obj
        next_day = next_day.strftime("%Y-%m-%d") #string
        store = Counter()
        openPrice = float(Tweet.BTCPrices[next_day]['openPrice'])
        closePrice = float(Tweet.BTCPrices[next_day]['closePrice'])
        for val in values:
            words = re.sub(r'#\w+', '', val["text"])  # get hashtags in description
            words = words.split()
            store.update(words)
            # extract the relevant fields from the tweet
            # user_location = tweet['user_location']
            # user_description = tweet['user_description']
            # user_created = tweet['user_created']
            # user_followers = tweet['user_followers']
            # user_friends = tweet['user_friends']
            # user_favourites = tweet['user_favourites']
            # user_verified = tweet['user_verified']
            # date = tweet['date']
            # text = tweet['text']
            # hashtags = tweet['hashtags']
            # source = tweet['source']
            # is_retweet = tweet['is_retweet']

        word_count = sorted(store.items(), key=lambda x: x[1])#, reverse=True)   # sort dict
        # lower_bound = int(len(word_count) * 0.4)
        # upper_bound = int(len(word_count) * 0.6)
        # word_count = dict(sorted_word_count[lower_bound:upper_bound])
        word_count = dict(word_count)
        word_count = {k: v for k, v in word_count.items() if v >= 3}  # only include words with 3 or more count 
        yield {"TweetDate": key, "OpenPrice - ClosePrice for next day": (openPrice-closePrice)}, {"word_count": word_count }

        #yield key, text
        # for tweet in values:
        #     yield key, "test"


if __name__ == '__main__':
    Tweet.run()
