from psycopg2 import Error
import faker
from connect import create_connection, database

fake_data = faker.Faker()

def create_user(conn, user):
    """
    Create a new user into the users table
    :param conn:
    :param user:
    :return: user id
    """
    sql = '''
    INSERT INTO users(fullname, email) VALUES(%s,%s);
    '''
    cur = conn.cursor()
    try:
        cur.execute(sql, user)
        conn.commit()
    except Error as e:
        print(e)
    finally:
        cur.close()

    return cur.lastrowid

def create_status(conn, status):
    """
    Create a new status
    :param conn:
    :param status:
    :return:
    """

    sql = '''
    INSERT INTO status(name) VALUES(%s);
    '''
    cur = conn.cursor()
    try:
        cur.execute(sql, status)
        conn.commit()
    except Error as e:
        print(e)
    finally:
        cur.close()

    return cur.lastrowid

def create_task(conn, task):
    """
    Create a new task into the tasks table
    :param conn:
    :param user:
    :return: user id
    """
    sql = '''
    INSERT INTO tasks(title, description, status_id, user_id) VALUES(%s,%s,%s,%s);
    '''
    cur = conn.cursor()
    try:
        cur.execute(sql, task)
        conn.commit()
    except Error as e:
        print(e)
    finally:
        cur.close()

    return cur.lastrowid

if __name__ == '__main__':
    with create_connection(database) as conn:
# create users
        user_1 = (fake_data.name(), fake_data.email())
        user_1_id = create_user(conn, user_1)
        print(user_1_id)

        user_2 = (fake_data.name(), fake_data.email())
        user_2_id = create_user(conn, user_2)
        print(user_2_id)

        user_3 = (fake_data.name(), fake_data.email())
        user_3_id = create_user(conn, user_3)
        print(user_3_id)

# create statuses
        status_1 = ('new',)
        status_1_id = create_status(conn, status_1)
        print(status_1_id)

        status_2 = ('in progress',)
        status_2_id = create_status(conn, status_2)
        print(status_2_id)

        status_3 = ('completed',)
        status_3_id = create_status(conn, status_3)
        print(status_3_id)
        
# create tasks
        print(create_task(conn, ('first', fake_data.text(), 1, 1)))
        print(create_task(conn, ('second', fake_data.text(), 2, 2)))
        print(create_task(conn, ('third', fake_data.text(), 3, 3)))