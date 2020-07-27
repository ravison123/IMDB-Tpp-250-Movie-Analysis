def movie_check(current_id, movie_name, movie_rating):
    x = session.query(Table.top250_id, Table.title, Table.rating).filter(Table.title == movie_name).all()
    if len(x) < 1:
        return False
    return True


def find_movie_id(name):
    z = session.query(Table.top250_id).filter(Table.title == name).all()
    return z[0][0]

def find_movie_rating(name):
    rating_extract = session.query(Table.rating).filter(Table.title == name).all()
    return rating_extract[0][0]
    
def database_check(num):
    y = session.query(Table.top250_id).filter(Table.top250_id == num).all()
    # True: Database row is empty / does not exist
    # False: Database row is filled (need to delete the row)
    if len(y) < 1:
        return True
    return False
    
def delete_movie(num):
    print('Deleting existing movie in database in place {}'.format(num))
    movie = session.query(Table).filter(Table.top250_id == num).one()
    session.delete(movie)



import bs4
import requests
from time import sleep
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, String

engine = create_engine('sqlite:///movie_data1.db?check_same_thread=False')
Base = declarative_base()

class Table(Base):
    __tablename__ = 'imdb_top_250'
    top250_id = Column(Integer, primary_key = True)
    title = Column(String, primary_key = True)
    rating = Column(String)
    year = Column(Integer)
    num_of_rating = Column(Integer)
    director = Column(String)
    writer = Column(String)
    actors = Column(String)
    meta_score = Column(Integer)
    awards = Column(String)
    run_time = Column(String)
    genre = Column(String)
    country = Column(String)
    language = Column(String)
    detailed_release_date = Column(String)
    budget = Column(String)
    usa_collection = Column(String)
    worldwide_collection = Column(String)
    
    
    def __repr__(self):
        return self.title

Base.metadata.create_all(engine)
Session = sessionmaker(bind = engine)
session = Session()    
res = requests.get('https://www.imdb.com/chart/top/?ref_=nv_mv_250')
try:
    res.raise_for_status()
except:
    print('Error with parsing the webpage')
soup = bs4.BeautifulSoup(res.content, 'html.parser')
raw_titles = soup.find_all(class_ = 'titleColumn')
webpage_list = [i.a['href'] for i in raw_titles]
count = 0

for i in webpage_list:
    web_address = 'https://www.imdb.com/' + webpage_list[count]
    web_text = requests.get(web_address)
    try:
        web_text.raise_for_status()
    except:
        print('Error occured while parsing the movie webpage')
        print('Skipping the movie number {} from the top 250 list'.format(count + 1))
        count = count + 1
        continue
    print('Extracting information of movie no: {}'.format(count + 1))
    web_text_soup = bs4.BeautifulSoup(web_text.content, 'html.parser')
    
    
    # Extracting title block for title, rating, year of release and number of ratings    
    title_block = web_text_soup.find(class_ = 'title_block')
    title_wrapper = title_block.find(class_ = 'title_wrapper')
    title_year = title_block.h1.text
    title_year = title_year.replace(u'\xa0', u' ').strip()
    title_name = title_year.split("(")[0].strip()
    imdb_rating = title_block.find(class_ = 'ratingValue').strong.span.text
    current_movie_num = count + 1
    check_1 = movie_check(current_movie_num, title_name, imdb_rating)   # False: Movie doesn't exist, True: Movie exits
    check_2 = database_check(current_movie_num)     # Function to check if database position is empty or filled
    if check_1 == False and check_2 == True:
        print('Movie does not exist in database')
        print('Database row corresponding to movie rank is empty')
    elif check_1 == False and check_2 == False:
        print('Movie does not exist in database')
        print('Database row corresponding to movie id is filled')
        print('Deleting the database row')
        delete_movie(current_movie_num)
    elif check_1 == True and check_2 == True:
        print('Movie exists in the database')
        print('Database row corresponding to current movie rank is empty')
        print('Deleting another instance of the movie in database table')
        num = find_movie_id(title_name)
        delete_movie(num)
    elif check_1 == True and check_2 == False:
        print('Movie exists in database')
        print('Database row corresponding to current movie rank is filled')
        num = find_movie_id(title_name)
        found_rating = find_movie_rating(title_name)
        if num == current_movie_num and found_rating == imdb_rating:
            print('Movie exists in database and its rank and rating is unchanged')
            count = count + 1
            continue
        else:
            print('Movie exists in the database. Rank / rating has been changed since last data update')
            print('Deleting another instance of current movie')
            delete_movie(num)
            print('Deleting the movie in current rank position in table')
            delete_movie(current_movie_num)
        
    year_of_release = title_year.split('(')[1].strip(')').strip()
    ratings_count = title_block.a.span.text
    
    # Extracting plot summary and credit summary for director, actor, writer
    plot_summary = web_text_soup.find(class_ = 'plot_summary')
    credit_summary = plot_summary.find_all(class_ = 'credit_summary_item')
    director_name = credit_summary[0].a.text
    writer_name = credit_summary[1].a.text
    actors = credit_summary[2].find_all('a')
    actors_list = []
    for i in actors:
        if i.text != 'See full cast & crew':
            actors_list.append(i.text)
    actors_string = ','.join(actors_list)
    
    # Extracting title review bar for metacritic score
    title_review_bar = web_text_soup.find(class_  = 'titleReviewBar')
    try:
        metacritic_score_bar = title_review_bar.find(class_ = 'metacriticScore score_favorable titleReviewBarSubItem')
        metacritic_score_val = metacritic_score_bar.span.text
    except:
        print('Error getting metacritic score')
        metacritic_score_val = None
        
    # Extracting 'awards-blurb' class for award details
    try:
        awards_text = web_text_soup.find(class_ = 'awards-blurb').b.text.strip()
        awards_text_list = awards_text.split('\n')
        awards_text_list = [i.strip() for i in awards_text_list]
        awards_text_corrected = ' '.join(awards_text_list)
    except:
        print('Error getting awards')
        awards_text_corrected = 'Information not found'
        
    # Extracting title bar for movie run time and genre
    title_bar = web_text_soup.find(class_ = 'titleBar')
    title_subtext = title_bar.find(class_ = 'subtext')
    run_time_info= title_subtext.time.text.strip()
    genre_detailed_date = title_subtext.find_all('a')
    genre_info = genre_detailed_date[0].text.strip()
        

    # Extracting title details for detailed release date, country, language, budget, gross income
    title_details = web_text_soup.find_all('div', class_= 'txt-block')
    
    for i in title_details:
        if i.h4.text == 'Country:':
            country_details = i.a.text
            break
    for i in title_details:
        if i.h4.text == 'Language:':
            language_details = i.a.text
            break
    try:
        for i in title_details:
            if i.h4.text == 'Release Date:':
                release_date = i.text
                break
        release_date_corrected = release_date.strip()
        date_loc = release_date_corrected.find(':')
        see_loc = release_date_corrected.find('See')
        release_date_corrected = release_date_corrected[date_loc + 1 : see_loc]
        release_date_corrected = release_date_corrected.strip()
    except:
        print('Error in getting release date details')
        release_date_corrected = 'Information not available'
    
    
    try:
        for i in title_details:
            if i.h4.text == 'Budget:':
                budget_details = i.text.strip()
                break
        est_loc = budget_details.find('(')
        budget_details = budget_details[:est_loc].strip()
    except:
        print('Error in getting budget details')
        budget_details = 'Information not available'
        
    try:
        for i in title_details:
            if i.h4.text == 'Gross USA:':
                usa_collection_details = i.text.strip()
                break
        colon_loc = usa_collection_details.find(':')
        usa_collection_details = usa_collection_details[colon_loc + 1 :].strip()
    except:
        print('Error in getting USA collection details')
        usa_collection_details = 'Information not available'
        
    try:
        for i in title_details:
            if i.h4.text == 'Cumulative Worldwide Gross:':
                worldwide_collection_details = i.text.strip()
                break
        colon_loc = worldwide_collection_details.find(':')
        worldwide_collection_details = worldwide_collection_details[colon_loc + 1 :].strip()
    except:
        print('Error in getting worldwide collection details')
        worldwide_collection_details = 'Information not available'
    


    new_row = Table(top250_id = count + 1, title = title_name, rating = imdb_rating, year = year_of_release,
                    num_of_rating = ratings_count, director = director_name,
                    writer = writer_name, actors = actors_string,
                    meta_score = metacritic_score_val, awards = awards_text_corrected,
                    run_time = run_time_info, genre = genre_info, country = country_details,
                    language = language_details, detailed_release_date = release_date_corrected,
                    budget = budget_details, usa_collection = usa_collection_details,
                    worldwide_collection = worldwide_collection_details)
    session.add(new_row)
    session.commit()    
    print('Extracting information of movie no: {} is complete'.format(count + 1))
    count = count + 1
    
    print('Sleeping between requests for 2 seconds')
    sleep(2)