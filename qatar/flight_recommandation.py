import sys
import psycopg2
import numpy as np
import pandas as pd
from sklearn.cluster import KMeans

import seaborn as sns
import matplotlib.pyplot as plt


def create_connection():
    connection = psycopg2.connect("dbname=qatar user=postgres password=0000")
    return connection


def release_connection(connection):
    connection.close()


def create_destination_map(curr):
    dest_map = dict()
    curr.execute("""
        select distinct source from flight
        union
        select distinct destination from flight
    """)
    destination_index = 0
    for destination in curr.fetchall():
        dest_map[destination[0]] = destination_index
        destination_index += 1

    return dest_map


def create_user_map(curr, destination_map):
    user_map = dict()
    min_age = 999
    max_age = 0
    curr.execute("select code, age, gender from people")
    for code, age, gender in curr.fetchall():

        age = int(age)
        if age > max_age:
            max_age = age

        if age < min_age:
            min_age = age

        if gender == "male":
            user_map[code] = [age, 1]
        elif gender == "female":
            user_map[code] = [age, 0]
        else:
            user_map[code] = [age, 2]

    age_normalization_factor = max_age - min_age
    dest_count_init_arr = [0] * len(destination_map.keys())
    for code, vector in user_map.items():
        age = vector[0]
        normalized_age = (age - min_age) / age_normalization_factor
        user_map[code][0] = normalized_age
        user_map[code].extend(dest_count_init_arr)

    return user_map


def add_destination_data_to_user_map(curr, user_data, dest_map):
    curr.execute("""
        select 
            p.code, f.destination, count(*)
        from 
            people p
        inner join flight f
        on p.code = f.people_code
        group by 1, 2
        order by 1
    """)

    db_data = curr.fetchall()
    dest_travel_max_count_arr = [0] * len(dest_map.keys())
    dest_travel_min_count_arr = [999] * len(dest_travel_max_count_arr)
    dest_travel_normalization_arr = [0] * len(dest_travel_max_count_arr)
    for code, destination, count in db_data:
        user_data[code][dest_map[destination] + 2] = count

        if dest_travel_min_count_arr[dest_map[destination]] > count:
            dest_travel_min_count_arr[dest_map[destination]] = count

        if dest_travel_max_count_arr[dest_map[destination]] < count:
            dest_travel_max_count_arr[dest_map[destination]] = count

    for idx in range(len(dest_travel_max_count_arr)):
        dest_travel_normalization_arr[idx] = dest_travel_max_count_arr[idx] - dest_travel_min_count_arr[idx]

    for code in user_data.keys():
        for idx in range(len(dest_travel_max_count_arr)):
            user_data[code][2 + idx] = max(0, (user_data[code][2 + idx] - dest_travel_min_count_arr[idx]) /
                                           dest_travel_normalization_arr[idx])

    return user_data


def total_metric_sum_user_data(curr, user_data):
    curr.execute("""    
        select 
            p.code, sum(f.distance), sum(f.price), sum(time)
        from 
            people p
        inner join flight f
        on p.code = f.people_code
        group by 1
        order by 1            
    """)

    db_data = curr.fetchall()

    curr.execute("""
        select code from people
        except
        select distinct people_code from flight
    """)

    no_travel_person = curr.fetchall()

    min_metric = [sys.maxsize, sys.maxsize, sys.maxsize]
    max_metric = [0, 0, 0]
    reg_matrix = [0, 0, 0]

    for _, distance, price, time in db_data:

        if min_metric[0] > distance:
            min_metric[0] = distance

        if max_metric[0] < distance:
            max_metric[0] = distance

        if min_metric[1] > price:
            min_metric[1] = price

        if max_metric[1] < price:
            max_metric[1] = price

        if min_metric[2] > time:
            min_metric[2] = time

        if max_metric[2] < time:
            max_metric[2] = time

    for i in range(3):
        reg_matrix[i] = max_metric[i] - min_metric[i]

    for code, distance, price, time in db_data:
        user_data[code].extend(
            [(distance - min_metric[0]) / reg_matrix[0], float((price - min_metric[1]) / reg_matrix[1]),
             (time - min_metric[2]) / reg_matrix[2]])

    for code in no_travel_person:
        user_data[code[0]].extend([0, 0, 0])

    return user_data


def get_year_wise_travel_data(curr, user_data):
    curr.execute("""    
        select 
            p.code, to_char(f.date, 'mm'), count(*)
        from 
            people p
        inner join flight f
        on p.code = f.people_code
        group by 1, 2
        order by 1, 2           
    """)

    max_visit_month_wise = [0] * 12
    min_visit_month_wise = [sys.maxsize] * 12
    reg_visit_month_wise = [0] * 12

    db_data = curr.fetchall()
    for _, month, visit in db_data:

        month = int(month) - 1
        if max_visit_month_wise[month] < visit:
            max_visit_month_wise[month] = visit

        if min_visit_month_wise[month] > visit:
            min_visit_month_wise[month] = visit

    for i in range(12):
        reg_visit_month_wise[i] = max_visit_month_wise[i] - min_visit_month_wise[i]

    for code in user_data.keys():
        user_data[code].extend([0] * 12)

    for code, month, visit in db_data:
        month = int(month) - 1
        user_data[code][14 + month] = (visit - min_visit_month_wise[month]) / reg_visit_month_wise[month]

    return user_data


def create_model_and_train(user_data):
    user_vector = np.zeros(shape=(len(user_data.keys()), len(user_data[0])))

    for idx in user_data.keys():
        user_vector[idx] = user_data[idx]

    """
    
    # elbow analysis

    inertia = []
    for k in range(1, 10):
        k_means = KMeans(n_clusters=k).fit(user_vector)
        k_means.fit(user_vector)
        inertia.append(k_means.inertia_)

    plt.plot([i for i in range(1, 10)], inertia, 'bx-')
    plt.xlabel('k')
    plt.ylabel('Inertia')
    plt.show()
    
    """

    k_means = KMeans(n_clusters=4).fit(user_vector)
    labels = k_means.labels_

    cluster = {0: [], 1: [], 2: [], 3: [], 4: []}

    for idx, value in enumerate(labels):
        cluster[value].append(idx)

    with open("output.txt", "w") as file:
        for cluster_id in cluster.keys():
            file.write(str(cluster_id) + ":\n")
            file.write(str(cluster[cluster_id]) + "\n")


def main():
    conn = create_connection()
    cursor = conn.cursor()

    destination_map = create_destination_map(cursor)
    user_data = create_user_map(cursor, destination_map)
    user_data = add_destination_data_to_user_map(cursor, user_data, destination_map)
    user_data = total_metric_sum_user_data(cursor, user_data)
    user_data = get_year_wise_travel_data(cursor, user_data)

    # conn.commit()
    cursor.close()
    release_connection(conn)

    create_model_and_train(user_data)


if __name__ == "__main__":

    try:
        main()
    except Exception as e:
        print("Exception occurred during execution : {}".format(e))
        raise e
