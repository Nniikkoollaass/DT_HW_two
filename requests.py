from psycopg2 import Error

from connect import create_connection, database

#1 Отримати всі завдання певного користувача. 
# Використайте SELECT для отримання завдань конкретного користувача 
#за його user_id
def get_user_tasks(conn, user):
    sql = '''
    SELECT * FROM tasks 
    WHERE tasks.user_id = users.id;
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

#2 Вибрати завдання за певним статусом. 
#Використайте підзапит для вибору завдань з конкретним статусом, 
#наприклад, 'new'
def get_task_with_status(conn, status):
    sql = f'''
    SELECT * FROM tasks 
    WHERE tasks.status_id = status.id
    AND status.name = {status};
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

#3 Оновити статус конкретного завдання. 
# Змініть статус конкретного завдання на 'in progress' або інший статус
def update_task_status(conn, task_id, status_id):
    sql = f'''
    UPDATE tasks 
    SET tasks.status_id = {status_id}
    WHERE tasks.id = {task_id};
    '''
    cur = conn.cursor()
    try:
        cur.execute(sql, task_id)
        conn.commit()
    except Error as e:
        print(e)
    finally:
        cur.close()
    return cur.lastrowid

#4 Отримати список користувачів, які не мають жодного завдання. 
#Використайте комбінацію SELECT, WHERE NOT IN і підзапит.
def get_users_with_no_task(conn):
    sql = f'''
    SELECT * FROM users 
    WHERE users.id NOT IN (SELECT user_id FROM tasks);
    '''
    cur = conn.cursor()
    try:
        cur.execute(sql)
        conn.commit()
    except Error as e:
        print(e)
    finally:
        cur.close()
    return cur.lastrowid

#5 Додати нове завдання для конкретного користувача. 
# Використайте INSERT для додавання нового завдання.
def add_new_task_to_user(conn, user_id):
    sql = f'''
    INSERT INTO tasks (title, description, status_id, user_id)
    VALUES ('to do', 'some', 1, {user_id});
    '''
    cur = conn.cursor()
    try:
        cur.execute(sql, user_id)
        conn.commit()
    except Error as e:
        print(e)
    finally:
        cur.close()
    return cur.lastrowid

#6 Отримати всі завдання, які ще не завершено.
# Виберіть завдання, чий статус не є 'завершено'.
def get_not_completed_tasks(conn):
    sql = '''
    SELECT * FROM tasks 
    WHERE tasks.status_id NOT IN 
    (SELECT status.id FROM status
    WHERE status.name = 'completed');
    '''
    cur = conn.cursor()
    try:
        cur.execute(sql)
        conn.commit()
    except Error as e:
        print(e)
    finally:
        cur.close()
    return cur.lastrowid

#7 Видалити конкретне завдання. 
#Використайте DELETE для видалення завдання за його id
def delete_task_by_id(conn, task_id):
    sql = f'''
    DELETE FROM tasks
    WHERE tasks.id = {task_id};
    '''
    cur = conn.cursor()
    try:
        cur.execute(sql, task_id)
        conn.commit()
    except Error as e:
        print(e)
    finally:
        cur.close()
    return cur.lastrowid

#8 Знайти користувачів з певною електронною поштою.
# Використайте SELECT із умовою LIKE для фільтрації 
#за електронною поштою
def find_users_with_such_email(conn):
    sql = '''
    SELECT * FROM users
    WHERE email LIKE '%45@%';
    '''
    cur = conn.cursor()
    try:
        cur.execute(sql)
        conn.commit()
    except Error as e:
        print(e)
    finally:
        cur.close()
    return cur.lastrowid

#9 Оновити ім'я користувача. 
#Змініть ім'я користувача за допомогою UPDATE
def update_user_name(conn, user_id, name):
    sql = f'''
    UPDATE users 
    SET users.fullname = {name}
    WHERE users.id = {user_id};
    '''
    cur = conn.cursor()
    try:
        cur.execute(sql, user_id)
        conn.commit()
    except Error as e:
        print(e)
    finally:
        cur.close()
    return cur.lastrowid

#10 Отримати кількість завдань для кожного статусу.
# Використайте SELECT, COUNT, GROUP BY для групування завдань
# за статусами
def count_tasks_status(conn):
    sql = '''
    SELECT COUNT(status_id), status_id
    FROM tasks
    GROUP BY status_id;
    '''
    cur = conn.cursor()
    try:
        cur.execute(sql)
        conn.commit()
    except Error as e:
        print(e)
    finally:
        cur.close()
    return cur.lastrowid

#11 Отримати завдання, які призначені користувачам 
#з певною доменною частиною електронної пошти. 
#Використайте SELECT з умовою LIKE в поєднанні з JOIN,
# щоб вибрати завдання, призначені користувачам, 
#чия електронна пошта містить певний домен (наприклад, '%@example.com').
def get_tasks_with_such_user_email(conn):
    sql = '''
    SELECT * 
    FROM tasks AS t
    INNER JOIN users AS u
    ON u.id = t.user_id
    WHERE u.email LIKE '%@gmail%';
    '''
    cur = conn.cursor()
    try:
        cur.execute(sql)
        conn.commit()
    except Error as e:
        print(e)
    finally:
        cur.close()
    return cur.lastrowid

#12 Отримати список завдань, що не мають опису. 
#Виберіть завдання, у яких відсутній опис
def get_tasks_without_description(conn):
    sql = '''
    SELECT * FROM tasks 
    WHERE tasks.description IS NULL;
    '''
    cur = conn.cursor()
    try:
        cur.execute(sql)
        conn.commit()
    except Error as e:
        print(e)
    finally:
        cur.close()
    return cur.lastrowid

#13 Вибрати користувачів та їхні завдання, 
#які є у статусі 'in progress'.
# Використайте INNER JOIN для отримання списку користувачів
# та їхніх завдань із певним статусом
def get_users_and_tasks_in_progress(conn, status):
    sql = f'''
    SELECT u.fullname, t.status_id
    FROM users AS u
    INNER JOIN tasks AS t
    ON u.id = t.user_id
    WHERE t.status_id IN
    (SELECT status.id FROM status
    WHERE status.name = {status});
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

#14 Отримати користувачів та кількість їхніх завдань. 
#Використайте LEFT JOIN та GROUP BY для вибору користувачів 
#та підрахунку їхніх завдань.
def get_users_and_their_counted_tasks(conn):
    sql = f'''
    SELECT u.id, u.fullname, COUNT(t.id)
    FROM users AS u
    LEFT JOIN tasks AS t 
    ON u.id = t.user_id
    GROUP BY u.id;
    '''
    cur = conn.cursor()
    try:
        cur.execute(sql,)
        conn.commit()
    except Error as e:
        print(e)
    finally:
        cur.close()
    return cur.lastrowid

if __name__ == '__main__':
    with create_connection(database) as conn:
        # requests 1-14
        print(get_user_tasks(conn, 1))
        print(get_task_with_status(conn, 'new'))
        print(update_task_status(conn, 2, 2))
        print(get_users_with_no_task(conn))
        print(add_new_task_to_user(conn, 1))
        print(get_not_completed_tasks(conn))
        print(delete_task_by_id(conn, 1))
        print(find_users_with_such_email(conn))
        print(update_user_name(conn, 1, 'Bobby'))
        print(count_tasks_status(conn))
        print(get_tasks_with_such_user_email(conn))
        print(get_tasks_without_description(conn))
        print(get_users_and_tasks_in_progress(conn, 'in progress'))
        print(get_users_and_their_counted_tasks(conn))
