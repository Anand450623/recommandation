import psycopg2
import pandas as pd


def create_connection():
    connection = psycopg2.connect("dbname=qatar user=postgres password=0000")
    return connection


def release_connection(connection):
    connection.close()


def create_destination_map(curr):

    curr.execute("""
        select distinct source from flight
        union
        select distinct destination from flight
        order by 1 
    """)

    dest_map = dict()
    destination_index = 0
    for destination in curr.fetchall():
        dest_map[destination[0]] = destination_index
        destination_index += 1

    return dest_map


def create_user_map(curr, destination_map):

    user_map = dict()
    curr.execute("select code, age, name, gender, company from people")

    dest_count_init_arr = [0] * (2 * len(destination_map.keys()))
    for code, age, name, gender, company in curr.fetchall():
        user_map[code] = [code, name, age, gender, company]
        user_map[code].extend(dest_count_init_arr)

    return user_map


def add_source_data_to_user_map(curr, user_data, dest_map):

    curr.execute("""
        select 
            p.code, f.source, count(*)
        from 
            people p
        inner join flight f
        on p.code = f.people_code
        group by 1, 2
    """)

    db_data = curr.fetchall()

    for code, source, count in db_data:
        user_data[code][dest_map[source] + 5] = count

    return user_data


def add_destination_data_to_user_map(curr, user_data, dest_map):

    curr.execute("""
        select 
            p.code, f.destination, count(*)
        from 
            people p
        inner join flight f
        on p.code = f.people_code
        group by 1, 2
    """)

    db_data = curr.fetchall()
    offset = len(dest_map.keys()) + 5

    for code, destination, count in db_data:
        user_data[code][dest_map[destination] + offset] = count

    return user_data


def add_flight_type_data(curr, user_data, destination_data):

    curr.execute("""
        select 
            p.code, f.flight_type, count(*)
        from 
            people p
        inner join flight f
        on p.code = f.people_code
        group by 1, 2
    """)

    for code in user_data.keys():
        user_data[code].extend([0] * 3)

    flight_map = {'economic': 0, 'firstClass': 1, 'premium': 2}
    offset = (2 * len(destination_data.keys())) + 5

    for code, flight_type, count in curr.fetchall():
        user_data[code][flight_map[flight_type] + offset] = count

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
    """)

    db_data = curr.fetchall()

    curr.execute("""
        select code from people
        except
        select distinct people_code from flight
    """)

    no_travel_person = curr.fetchall()

    for code, distance, price, time in db_data:
        user_data[code].extend([distance, price, time])

    for code in no_travel_person:
        user_data[code[0]].extend([0, 0, 0])

    return user_data


def get_month_wise_travel_data(curr, user_data, offset):

    curr.execute("""    
        select 
            p.code, to_char(f.date, 'mm'), count(*)
        from 
            people p
        inner join flight f
        on p.code = f.people_code
        group by 1, 2           
    """)

    db_data = curr.fetchall()

    for code in user_data.keys():
        user_data[code].extend([0] * 12)

    for code, month, visit in db_data:
        month = int(month) - 1
        user_data[code][offset + month] = visit

    return user_data


def create_and_save_results(user_data, destination_map):

    column_list = ['code', 'age', 'name', 'gender', 'company']

    rev_destination_map = dict()
    for destination, index in destination_map.items():
        rev_destination_map[index] = destination

    for idx in range(len(rev_destination_map.keys())):
        column_list.append(rev_destination_map[idx]+'_as_source')

    for idx in range(len(rev_destination_map.keys())):
        column_list.append(rev_destination_map[idx]+'_as_destination')

    column_list.extend(['economic_count', 'firstClass_count', 'premium_count'])
    column_list.extend(['total_distance', 'total_price', 'total_time'])

    column_list.extend(['january_as_travel_month', 'february_as_travel_month', 'march_as_travel_month',
                        'april_as_travel_month', 'may_as_travel_month', 'june_as_travel_month', 'july_as_travel_month',
                        'august_as_travel_month', 'september_as_travel_month', 'october_as_travel_month',
                        'november_as_travel_month', 'december_as_travel_month'])

    user_df = pd.DataFrame(data=user_data.values())
    user_df.columns = column_list

    user_df.to_csv('user_analysis.csv', index=False)


def main():
    conn = create_connection()
    cursor = conn.cursor()

    destination_map = create_destination_map(cursor)
    user_data = create_user_map(cursor, destination_map)
    user_data = add_source_data_to_user_map(cursor, user_data, destination_map)
    user_data = add_destination_data_to_user_map(cursor, user_data, destination_map)
    user_data = add_flight_type_data(cursor, user_data, destination_map)
    user_data = total_metric_sum_user_data(cursor, user_data)
    user_data = get_month_wise_travel_data(cursor, user_data, (2*len(destination_map.keys())) + 11)

    # conn.commit()
    cursor.close()
    release_connection(conn)

    create_and_save_results(user_data, destination_map)


if __name__ == "__main__":

    try:
        main()
    except Exception as e:
        print("Exception occurred during execution : {}".format(e))
        raise e
