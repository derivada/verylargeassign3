#!/usr/bin/env python
# -*- coding: utf-8 -*-

from haversine import haversine

from DbConnector import DbConnector
from tabulate import tabulate

import os
import csv
from datetime import datetime, date, time, timezone
import time
class Connection:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db



    def delete_tables(self):
        # We delete all the tables in the correct order
        self.db.user.drop()
        self.db.activity.drop()
        self.db.trackpoint.drop()

    def insert_data(self, dataset_path):
        # Find labeled users
        labeled_users = []
        with open(dataset_path + '\\labeled_ids.txt', newline='') as csvfile:
            reader = csv.reader(csvfile, delimiter='\t', quotechar='|')
            for row in reader:
                labeled_users.append(row[0])

        # Ger user names mapping the path to user folder string
        users = [f.path.split('\\')[2] for f in os.scandir(
            dataset_path + '\\data') if f.is_dir()]

        activity_counter = 1
        trackpoint_counter = 1
        total_time = 0
        trackpoint_batch_size = 100000
        trackpoints = []

        # Main user and activity loop
        for user_id in users:
            # Insert user into DB
            start = time.perf_counter()
            print('Current user = ', user_id)
            user = {}
            user['_id'] = user_id
            if labeled_users.count(user_id) > 0:
                user['has_labels'] = True
            else:
                user['has_labels'] = False

            # Insert the user
            self.db.user.insert_one(user)

            # Read labels file if we got a labeled user
            labels = []
            if labeled_users.count(user_id) > 0:
                with open(dataset_path + '\\data\\{}\\labels.txt'.format(user_id), newline='') as csvfile:
                    reader = csv.reader(csvfile, delimiter='\t', quotechar='|')
                    line_count = 0
                    for row in reader:
                        line_count += 1
                        if line_count == 1:
                            continue
                        labels.append(row)

            # Loop trough his activities
            for file in os.scandir(dataset_path + '\\data\\{}\\Trajectory'.format(user_id)):
                activity = {}
                trackpoints_data = []
                # Open activity file and parse as csv
                with open(file.path, newline='') as csvfile:
                    reader = csv.reader(csvfile, delimiter=',', quotechar='|')
                    line_count = 0
                    # Read TrackPoint lines
                    for row in reader:
                        line_count += 1
                        if(line_count <= 6):
                            continue  # Skip header
                        elif(line_count > 2506):
                            break  # File too large
                        trackpoints_data.append(row)

                    if(line_count > 2506):
                        continue  # File too large -> Next activity

                    # Insert activity and trackpoints into DB
                    # Timestamp format: YYYY-MM-DD hh:mm:ss
                    start_time_str = trackpoints_data[0][5] + ' ' + trackpoints_data[0][6]
                    end_time_str = trackpoints_data[len(trackpoints_data) - 1][5] + ' ' + trackpoints_data[len(trackpoints_data) - 1][6]
                    
                    # Get activity attributes (including transportation mode)
                    activity['_id'] = activity_counter
                    activity['user_id'] = user['_id']
                    activity['start_date_time'] = parse_time(start_time_str)
                    activity['end_date_time'] = parse_time(end_time_str)
                    activity['transportation_mode'] = None
                    # Find correct transportation mode label by matching starting and ending time
                    for label in labels:
                        if start_time_str == label[0].replace('/', '-') and end_time_str == label[1].replace('/', '-'):
                            activity['transportation_mode'] = label[2]

                    # Insert activity
                    self.db.activity.insert_one(activity)

                    for trackpoint_data in trackpoints_data:
                        trackpoint = {}
                        trackpoint['_id'] = trackpoint_counter
                        trackpoint['activity_id'] = activity['_id']
                        # We also put the user_id here, it adds redundancy but speeds up and simplifies some queries
                        trackpoint['user_id'] = activity['user_id']
                        # Added some try-excepts in case some of this fields formats are bad
                        try:
                            trackpoint['lat'] = float(trackpoint_data[0])
                        except:
                            trackpoint['lat'] = None
                        try:
                            trackpoint['lon'] = float(trackpoint_data[1])
                        except:
                            trackpoint['lon'] = None
                        try:
                            trackpoint['altitude'] = float(trackpoint_data[3])
                        except:
                            trackpoint['altitude'] = None
                        try:
                            trackpoint['date_time'] = parse_time(trackpoint_data[5] + ' ' + trackpoint_data[6])
                        except:
                            trackpoint['date_time'] = None

                        trackpoints.append(trackpoint)
                        trackpoint_counter += 1
                        # Batch insert into DB
                        if(trackpoint_counter % trackpoint_batch_size == 0):
                            self.db.trackpoint.insert_many(trackpoints)
                            trackpoints = []
                            print('Inserted trackpoints {} to {}'.format(
                                trackpoint_counter - trackpoint_batch_size, trackpoint_counter))

                    activity_counter += 1
            stop = time.perf_counter()
            total_time += stop - start

        # Insert remaining trackpoints
        self.db.trackpoint.insert_many(trackpoints)
        print('Data inserted in {:0.0f} minutes and  {:0.0f} seconds'.format(
            total_time / 60, total_time % 60))

    def execute_and_print(self, query, message):
        # Helper function that executes and prints the results of a query
        start = time.perf_counter()
        self.cursor.execute(query)
        stop = time.perf_counter()
        rows = self.cursor.fetchall()
        print(message + '\n')
        print(tabulate(rows, headers=self.cursor.column_names, tablefmt="simple"))
        if len(rows) == 1:
            print('\n1 row in set ({:.2f} sec)\n'.format(stop - start))
        else:
            print('\n{} rows in set ({:.2f} sec)\n'.format(
                len(rows), stop - start))

    def query_1(self):
        # 1: How many users, activities and trackpoints are there in the dataset (after it is inserted into the database).
        start = time.perf_counter()
        # For this query we simply perform an aggregate count on each collection
        users = self.db.user.aggregate([
            {
                "$count": "Users"
            }
        ])

        activities = self.db.activity.aggregate([
            {
                "$count": "Activities"
            }
        ])

        trackpoints = self.db.trackpoint.aggregate([
            {
                "$count": "Trackpoints"
            }
        ])

        stop = time.perf_counter()
        table1 = [[users.next()['Users'], activities.next()['Activities'], trackpoints.next()['Trackpoints']]]

        # Arrange into tabulate table
        print("Query 1 - How many users, trackpoints and activities are there in the dataset (after it is inserted into the database):\n")
        print(tabulate(table1, headers=["Total Users", "Total Activities", "Total Trackpoints"], tablefmt="simple"))
        print('\nQuery done in {:.2f} seconds\n'.format(stop - start))

    def query_2(self):
        # 2: Find the average number of activities per user.

        # For this query, we perform an aggregate by grouping over user IDs on the activity collection, and count how many rows we get for each group
        start = time.perf_counter()
        user_activities = self.db.activity.aggregate([
            {
                "$group":{
                    "_id":"$user_id",
                    "count":{
                        "$count":{

                        }
                    }
                }
            }
        ])
        total = 0
        count = 0
        # We can now get the average by simply looping trough every user number of activities
        for user in user_activities:
            total += user['count']
            count += 1
        total /= count

        stop = time.perf_counter()
        table = [[total]]
        print("\nQuery 2 - Find the average number of activities per user:\n")
        print(tabulate(table, headers=["Average"], tablefmt="simple"))
        print('\nQuery done in {:.2f} seconds\n'.format(stop - start))

    def query_3(self):
        # 3: Find the top 20 users with the highest number of activities.
        start = time.perf_counter()
        # For this query we group all activities on the user ID and then count how many of them are on each group
        user_act = self.db.activity.aggregate([
            {
                "$group":{
                    "_id":"$user_id",
                    "count":{
                        "$sum":1
                    }
                }
            },
            {
                "$sort":{
                    "count":-1
                }
            },
            {
                "$limit":20
            }
        ])
        stop = time.perf_counter()

        table = []
        for user in user_act:
            table.append([user['_id'], user['count']])

        print("\nQuery 3 - Find the top 20 users with the highest number of activities:\n")
        print(tabulate(table, headers=["Users", "Activities"], tablefmt="simple"))
        print('\nQuery done in {:.2f} seconds\n'.format(stop - start))

    def query_4(self):
        # 4: Find all users who have taken a taxi.
        # For this query, we perform an aggregate by first filtering out every activity without the taxi transportation mode and then grouping 
        # by user ID and counting how many groups we get
        start = time.perf_counter()
        taxi_users = self.db.activity.aggregate([
            {
                "$match":{
                    "transportation_mode":"taxi"
                }
            },
            {
                "$group":{
                    "_id":"$user_id"
                }
            },
            {
                "$count":"Taxi Users"
            }
        ])
        stop = time.perf_counter()

        result = taxi_users.next()
        table = [[result['Taxi Users']]]
        print("\nQuery 4 - Find all users who have taken a taxi:\n")
        print(tabulate(table, headers=["Taxi Users"], tablefmt="simple"))
        print('\nQuery done in {:.2f} seconds\n'.format(stop - start))

    def query_5(self):
        # 5: Find all types of transportation modes and count how many activities that are tagged with these transportation mode labels. Do not count the documents where the mode is null.
        start = time.perf_counter()
        # For this query we first filter all the activities without transportation mode and then 
        # we group all activities on transportation mode and count how many of them are on each group
        transportation_mode_types = self.db.activity.aggregate([
            {
                "$match":{
                    "transportation_mode":{
                        "$ne":"None"
                    }
                }
            },
            {
                "$group":{
                    "_id":"$transportation_mode",
                    "count":{
                        "$sum":1
                    }
                }
            }
        ])
        stop = time.perf_counter()

        table = []
        for transportation_mode in transportation_mode_types:
            table.append([transportation_mode['_id'], transportation_mode['count']])
        print("\nQuery 5 - Find all types of transportation modes and count how many activities that are tagged with these transportation mode labels. Do not count the documents where the mode is null:\n")
        print(tabulate(table, headers=["Transportation mode", "Count"], tablefmt="simple"))
        print('\nQuery done in {:.2f} seconds\n'.format(stop - start))

    def query_6(self):
        # 6:
        #   a) Find the year with the most activities.
        #   b) Is this also the year with most recorded hours?

        # For this task, we will make the simplification that a task initiated in a year also ends in that year. This is most notable on task b),
        # where the task hours only count towards the year in which it starts. Without this, the queries would become way harder to write and understand.
    
        # To perform the query, we do an aggregate on the activity collection grouping by the year of the start time. After that, we sum the number of documents in each group,
        # sort the results in a descending way and select the first one to get the year with the maximum amount of activities
        start = time.perf_counter()
        most_activity_year = self.db.activity.aggregate([
            {
                "$group":{
                    "_id":{
                        "$year":"$start_date_time"
                    },
                    "total":{
                        "$sum":1
                    }
                }
            },
            {
                "$sort":{
                    "total":-1
                }
            },
            {
                "$limit":1
            }
        ])
        result = most_activity_year.next()
        stop = time.perf_counter()

        table = [[result['_id'], result['total']]]
        print("\nQuery 6 - a) Find the year with the most activities:\n")
        print(tabulate(table, headers = ["Year", "Activities"], tablefmt="simple"))
        print('\nQuery done in {:.2f} seconds\n'.format(stop - start))

        # b) For this part, we modify the above query by now counting the sum of the difference of hours between each activity
        start = time.perf_counter()
        most_hours_year = self.db.activity.aggregate([
            {
                "$group":{
                    "_id":{
                        "$year":"$start_date_time"
                    },
                    "total":{
                        "$sum":{
                            "$dateDiff":{
                                "startDate":"$start_date_time",
                                "endDate":"$end_date_time",
                                "unit":"hour"
                            }
                        }
                    }
                }
            },
            {
                "$sort":{
                    "total":-1
                }
            },
            {
                "$limit":1
            }
        ])
        stop = time.perf_counter()

        result = most_hours_year.next()
        table = [[result['_id'], result['total']]]
        print("\nQuery 6 - b) Is this also the year with most recorded hours?:\n")
        print(tabulate(table, headers = ["Year", "Hours"], tablefmt="simple"))
        print('\nQuery done in {:.2f} seconds\n'.format(stop - start))

        print('\nAs we see, 2008 with the most activities, but 2009 has more hours recorded\n')

    def query_7(self):
        # 7: Find the total distance (in km) walked in 2008, by user with id=112.
        start = time.perf_counter()
        # For this query, we find all trackpaints in 2008 by comparing with the start and end of the year, then, we filter by user id
        coordinates = self.db.trackpoint.find(
            {
                "date_time":
                    {
                        "$gte" : parse_time("2008-01-01 00:00:00"),
                        "$lte" : parse_time("2008-12-31 23:59:59")
                    },
                "user_id": "112"
            },
            {
                "_id": 0,
                "lat": 1,
                "lon": 1
            }
        )
        dist = 0
        last = None
        # Sum all the distances using haversine
        for coordinate in coordinates:
            if last is not None:
                dist += haversine((coordinate['lat'], coordinate['lon']), last, unit='km')
            last = (coordinate['lat'], coordinate['lon'])
        stop = time.perf_counter()
        rows = [[dist]]
        print("\nQuery 7 - Find the total distance (in km) walked in 2008, by user with id=112:\n")
        print(tabulate(rows, headers=["Total Distance"], tablefmt="simple"))
        print('\nQuery done in {:.2f} seconds\n'.format(stop - start))

    def query_8(self):
        # 8: Find the top 20 users who have gained the most altitude meters.
        #   - Output should be a table with (id, total meters gained per user)
        #   - Remember that some altitude-values are invalid
        #   - Tip: SUM (tp_n.altitude - tp_n-1.altitude), tp_n.altitude > tp_n-1.altitude

        # For this query, we make use of the denormalization we employed and retrieve all the valid altitude trackpoints with both their ID, the activity ID, the user ID and the altitude
        start = time.perf_counter()
        trackpoints = self.db.trackpoint.find(    
            {
                "altitude":{
                    "$ne":-777
                }
            },
            {
                "_id":1,
                "activity_id":1,
                "altitude":1,
                "user_id":1
            }
        )

        # Now, we compute the substraction of consecutive pairs on all rows. The following code is all recycled from task 2        
        last = None
        user_altitudes = {}
        for trackpoint in trackpoints:
            if(last == None or last[3] > trackpoint['altitude'] or last[1] != trackpoint['activity_id'] or last[2] != trackpoint['user_id']):
                # First trackpoint OR change of user OR altitude difference negative OR change in activity / user
                last = (id, trackpoint['activity_id'], trackpoint['user_id'], trackpoint['altitude'])
                continue
            if(trackpoint['user_id'] not in user_altitudes):
                user_altitudes[trackpoint['user_id']] = trackpoint['altitude'] - last[3]
            else:
                user_altitudes[trackpoint['user_id']] += trackpoint['altitude'] - last[3]
            last = (id, trackpoint['activity_id'], trackpoint['user_id'], trackpoint['altitude'])
        
        # And we sort by most feet gained
        user_altitudes = dict(sorted(user_altitudes.items(), key=lambda item: item[1], reverse=1))
        stop = time.perf_counter()

        # The rest is just arranging the result as a table
        top = 1
        table = []
        for entry in user_altitudes:
            row = []
            row.append(top)
            row.append(entry)
            row.append(user_altitudes[entry])
            table.append(row)
            top += 1
            if(top > 20):
                break

        print("Query 8 - Find the top 20 users who have gained the most altitude meters:\n")
        print(tabulate(table, headers=["Top", "User ID", "Altitude gained"], tablefmt="simple"))
        print('\nQuery done in {:.2f} seconds\n'.format(stop - start))

    def query_9(self):
        # 9: Find all users who have invalid activities,and the number of invalid activities per user
        # An invalid activity is defined as an activity with consecutive trackpoints where the timestamps deviate with at least 5 minutes.

        # We get the id, user ID, activity ID and date time from the trackpoints collection
        start = time.perf_counter()
        trackpoints = self.db.trackpoint.find(
            {
            },
            {
                "_id":0,
                "user_id":1,
                "activity_id":1,
                "date_time":1
            }
        )
        (last_ts, last_act) = (None, None)  # Last trackpoint and activity
        # Dictionary user_id -> # of invalid acts.
        users_with_invalid_acts = {}
        last_invalid_act = None  # Last invalid activity

        for trackpoint in trackpoints:
            # We skip the first trackpoint and activities that have been already marked as invalid
            # We also skip when we change activities to avoid, can't compute time diff between them
            if trackpoint['activity_id'] != last_invalid_act and last_ts != None and trackpoint['activity_id'] == last_act:
                # Difference in seconds
                diff = (trackpoint['date_time'] - last_ts).total_seconds()
                if diff >= 300.0:
                    if trackpoint['user_id'] in users_with_invalid_acts:
                        users_with_invalid_acts[trackpoint['user_id']] += 1
                    else:
                        users_with_invalid_acts[trackpoint['user_id']] = 1
                    last_invalid_act = trackpoint['activity_id']  # Mark as invalid
            (last_ts, last_act) = (trackpoint['date_time'], trackpoint['activity_id'])
        stop = time.perf_counter()

        # Arrange as table
        rows = []
        for entry in users_with_invalid_acts:
            row = []
            row.append(entry)
            row.append(users_with_invalid_acts[entry])
            rows.append(row)
        print("\nQuery 9 - Find all users who have invalid activities, and the number of invalid activities per user:\n")
        print(tabulate(rows, headers=["User", "Number of invalid activities"], tablefmt="simple"))
        print('\nQuery done in {:.2f} seconds\n'.format(stop - start))

    def query_10(self):
        # 10: Find the users who have tracked an activity in the Forbidden City of Beijing.
        #   - In this question you can consider the Forbidden City to have
        #   coordinates that correspond to: lat 39.916, lon 116.397.

        # For this query, we also make use of the denormalization and perform an aggregate on the trackpoint collection, filtering by the specified coordinates and
        # grouping by the user ID to avoid duplicate users. We also do a sort on the user ID of the groups to get an ordered list.
        start = time.perf_counter()
        forbidden_city_users = self.db.trackpoint.aggregate([
            {
                "$match":{
                    "lat":{
                        "$gt":39.915,
                        "$lt":39.917
                    },
                    "lon":{
                        "$gt":116.396,
                        "$lt":116.398
                    }
                }
            },
            {
                "$group":{
                    "_id":"$user_id"
                }
            },
            {
                "$sort":{
                    "_id":1
                }
            }
        ])
        stop = time.perf_counter()

        # And we arrange the results into a table
        table = []
        for user in forbidden_city_users: 
            table.append([user['_id']])
     
        print("\nQuery 10 - Find users who have tracked an activity in the Forbidden City of Beijing:\n")
        print(tabulate(table, headers=["User ID"], tablefmt="simple"))
        print('\nQuery done in {:.2f} seconds\n'.format(stop - start))

    def query_11(self):
        # 11: Find all users who have registered transportation_mode and their most used transportation_mode.
        #   - The answer should be on format (user_id, most_used_transportation_mode) sorted on user_id.
        #   - Some users may have the same number of activities tagged with e.g. walk and car. In this case it is up to you to decide which transportation mode to include in your answer (choose one).
        #   - Do not count the rows where the mode is null.
        start = time.perf_counter()
        # For this query we first filter activities without a transportation mode and then group them on both the user ID and the transportation mode
        # we output the amount of rows on each group and both the user ID and transportation mode
        user_trans = self.db.activity.aggregate([
            {
                "$match":{
                    "transportation_mode":{
                        "$ne":"None"
                    }
                }
            },
            {
                "$group":{
                    "_id":{
                        "user":"$user_id",
                        "transportation_mode":"$transportation_mode"
                    },
                    "count":{
                        "$sum":1
                    }
                }
            },
            {
                "$sort":{
                    "_id":1
                }
            }
        ])
        # Dictionary user_id -> name of most used transportation mode.
        best_trans_modes = {}
        # Dictionary user_id -> # of activities with most used transportation mode
        most_used_trans = {}

        for user in user_trans:
            # We have a double dictionary structure on the output so we separate the fields here to make it more readable
            user_id = user['_id']['user']
            trans_mode = user['_id']['transportation_mode']
            count = user['count']
            
            if user_id not in best_trans_modes or most_used_trans[user_id] < count:
                best_trans_modes[user_id] = trans_mode
                most_used_trans[user_id] = count

        stop = time.perf_counter()
        # Arrange as table
        rows = []
        for entry in best_trans_modes:
            row = []
            row.append(entry)
            row.append(best_trans_modes[entry])
            rows.append(row)
        print(
            "\nQuery 11 - Find all users who have registered transportation_mode and their most used transportation_mode:\n")
        print(tabulate(rows, headers=[
              "User", "Most used transportation mode"], tablefmt="simple"))
        print('\nQuery done in {:.2f} seconds\n'.format(stop - start))

def parse_time(string):
    return datetime.strptime(string,  "%Y-%m-%d %H:%M:%S")

def main():
    try:
        program = Connection()                       # Initialize database connection
        # program.delete_tables()                     # Delete all tables and data
        # Parse dataset and insert data into tables
        # program.insert_data('dataset')
        # Execute the queries
        program.query_1()
        program.query_2()
        program.query_3()
        program.query_4()
        program.query_5()
        program.query_6()
        program.query_7()
        program.query_8()
        program.query_9()
        program.query_10()
        program.query_11()

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            # Close database connection after program has finished or failed
            program.connection.close_connection()

if __name__ == "__main__":
    main()
