from collections import defaultdict

# 1) how many movies are there in the database?
movies = []
with open('./movies.csv', 'r') as file:
  for line in file:
    movies.append(line)
# take off 1 for headers row
num_movies = len(movies) - 1
print 'there are %s movies in the database' % num_movies

# 2) which movie has ID 5000?
with open('./movies.csv', 'r') as file:
  for line in file:
    if line.startswith('5000,'):
      _movie = line.split(',')[1]
      print '%s has id 5000' % _movie
      break

# 3) how many ratings are there for "Toy Story"?
with open('./movies.csv', 'r') as file:
  for line in file:
    if 'Toy Story (1995)' in line:
      toy_story_id = line.split(',')[0]
      break

counter = 0
with open('./ratings.csv', 'r') as file:
  for line in file:
    if ',%s,' % toy_story_id in line:
      counter += 1
print 'there are %s ratings for Toy Story' % counter

# 4) what is the average rating for Toy Story 3?
with open('./movies.csv', 'r') as file:
  for line in file:
    if 'Toy Story 3 (2010)' in line:
      toy_story_id = line.split(',')[0]
      break

ratings = []
with open('./ratings.csv', 'r') as ratings_file:
  for index, line in enumerate(ratings_file):
    if index == 0:
      continue
    _split = line.split(',')
    movie_id = int(_split[1])
    if movie_id == toy_story_id:
      rating = float(_split[2])
      ratings.append(rating)
avg = sum(ratings) / len(ratings) if ratings else None
if avg:
  print 'the average rating for toy story 3 is %s' % avg

# 5) what is the average rating for every movie?
movie_names_by_id = {}
with open('./movies.csv', 'r') as movies_file:
  for index, line in enumerate(movies_file):
    if index == 0:
      continue
    _split = line.split(',')
    movie_id = int(_split[0])
    movie_title = str(_split[1])
    movie_names_by_id[movie_id] = movie_title

ratings_by_movie_name = defaultdict(list)
with open('./ratings.csv', 'r') as ratings_file:
  for index, line in enumerate(ratings_file):
    if index == 0:
      continue
    _split = line.split(',')
    _id = int(_split[1])
    _rating = float(_split[2])
    _name = movie_names_by_id.get(_id)
    ratings_by_movie_name[_name].append(_rating)

averages_by_name = {}
for _name, ratings in ratings_by_movie_name.iteritems():
  avg_rating = sum(ratings) / len(ratings) \
    if ratings else None
  averages_by_name[_name] = float(avg_rating)

print 'the average ratings for each movie are as follows: %s' % averages_by_name

