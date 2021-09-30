from instascrape import *

# Instantiate the scraper objects 
google = Profile('https://www.instagram.com/google/')
google_post = Post('https://www.instagram.com/p/CG0UU3ylXnv/')
google_hashtag = Hashtag('https://www.instagram.com/explore/tags/google/')

# Scrape their respective data 
google.scrape()
#google_post.scrape()
#google_hashtag.scrape()

print(google.followers)
#print(google_post['hashtags'])
#print(google_hashtag.amount_of_posts)

#abc = google_hashtag.get_recent_posts()
abcDict = {1:"abcd", 2:"defg"}
print(abcDict[1]) #should return abcd

print(abc[1].to_dict())