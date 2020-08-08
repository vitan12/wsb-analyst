from psaw import PushshiftAPI
import pickle
from collections import defaultdict
import datetime as dt
import sys
import time

# Body of Request
# parent_id,link_id,top_awarded_type,subreddit_id,author_flair_richtext,author_flair_text_color,author_patreon_flair,no_follow,author_flair_type,locked,retrieved_on,treatment_tags,author_flair_template_id,send_replies,awarders,author_flair_background_color,author_premium,all_awardings,id,body,author_flair_text,author_fullname,gildings,score,author_flair_css_class,stickied,permalink,total_awards_received,is_submitter,created,subreddit,created_utc,author,collapsed_because_crowd_control,associated_award
# t1_g0qllo3,t3_i5l1m1,,t5_2th52,"

comments_batch = 10000
filename = '{start}-{end}.pickle'
api = PushshiftAPI()
subreddit = 'WallStreetBets'
fields = ['parent_id', 'id', 'score', 'body', 'created_utc']

def get_epoch(month, day, year):
    date = dt.date(month=month, day=day, year=year)
    return int(time.mktime(date.timetuple()))

def download_comments(start, end):
    current = start
    while current < end:
        gen = api.search_comments(after=current, before=end, filter=fields, subreddit=subreddit, sort='asc')
        cache = []
        for c in gen:
            cache.append(c.d_)
            if len(cache) >= comments_batch:
                break
        last_comment = cache[-1]
        pickle.dump(cache, open(filename.format(start=current, end=last_comment['created_utc']), 'wb')) 
        current = last_comment['created_utc']

def main(interval_num):
    # initial_time = get_epoch(2, 1, 2012)
    initial_time = get_epoch(8, 3, 2020)
    final_time = get_epoch(8, 6, 2020)
    difference = final_time - initial_time
    interval = difference // 4
    custom_start = initial_time + (interval * interval_num)
    custom_end = custom_start + interval
    download_comments(custom_start, custom_end)

if __name__ == '__main__':
    interval_num = int(sys.argv[1])
    main(interval_num)
