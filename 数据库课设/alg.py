import pandas as pd
import pyodbc
import mysql.connector
from collections import defaultdict


class Classroom:
    def __init__(self, clname, capacity):
        self.clname = clname
        self.capacity = capacity
        # Initialize a schedule for each classroom for 20 weeks, 5 days a week, 4 sessions per day
        self.schedule = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

class Course:
    def __init__(self, id, course_id, course_name, count, student_count, teacher_name, teacher_id, clid):
        self.id = id
        self.course_id = course_id
        self.course_name = course_name
        self.count = count
        self.studen_count = student_count
        self.teacher_name = teacher_name
        self.teacher_id = teacher_id
        self.clid = clid
        self.hours = 5
        self.weekly_sessions = 1  # 每周安排的课时
        self.scheduled_hours = 0  # 添加这行来初始化已排课时数
        self.scheduled_week_hours = 0  # 初始化每周已排课时

# 数据库配置
db_config = {
    'host': 'localhost',
    'database': 'KS',
    'user': 'root',
    'password': '15678563182LXHlxh.'
}

# # Read Excel data
excel_path = '/Users/xuhuanlu/Desktop/副本上课教室安排测试数据new.xlsx'
classrooms_df = pd.read_excel(excel_path, sheet_name='教室基本信息')
courses_df = pd.read_excel(excel_path, sheet_name='教师上课信息')

# Create Classroom and Course instances
classrooms = [Classroom(**row) for index, row in classrooms_df.iterrows()]
courses = [Course(**row) for index, row in courses_df.iterrows()]

def schedule_classes(classrooms, courses):
    for course in courses:
        # 计算总共需要的周数
        total_weeks_needed = -(-course.hours // course.weekly_sessions)
        assigned_classroom = None  # 记录已经安排的教室
        assigned_days = set()  # 记录已经安排该课程的天

        for week in range(1, total_weeks_needed + 1):
            course.scheduled_week_hours = 0  # 将每周已排课时重置
            # 如果本周课时已经排满或整个课程的课时已经排满，则跳过这周
            if course.scheduled_week_hours >= course.weekly_sessions:
                continue

            for day in range(1, 6):  # 周一到周五
                if day in assigned_days:  # 如果这天已经安排过该课程，则跳过
                    continue

                for session in range(1, 4):  # 每天的三个时间段
                    if course.scheduled_week_hours >= course.weekly_sessions:
                        break  # 如果本周已排满，则停止安排

                    if day in assigned_days:
                        break;

                    for classroom in classrooms:
                        if classroom.capacity >= course.count:
                            # 检查是否已有安排或者该时间段是否空闲
                            if (not assigned_classroom or assigned_classroom == classroom) and classroom.schedule[week][day][session] == []:
                                classroom.schedule[week][day][session].append(course.course_name)
                                course.scheduled_hours += 1
                                course.scheduled_week_hours += 1
                                if not assigned_classroom:
                                    assigned_classroom = classroom  # 记录已经安排的教室
                                assigned_days.add(day)
                                break

            # 清空已安排的天，为下一周准备
            assigned_days.clear()

def insert_schedule_to_db(classrooms, courses, db_config):
    '''

    :param classrooms:
    :param courses:
    :param db_config:
    :return:成功导入返回1 否则返回0
    '''
    try:
        # # 连接数据库
        # conn = mysql.connector.connect(**db_config)
        # 构建连接字符串
        conn_str = f'DRIVER={{SQL Server}};SERVER={db_config["server"]};DATABASE={db_config["database"]};UID={db_config["username"]};PWD={db_config["password"]}'

        # 连接数据库
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # 为了能够根据课程名称找到相应的课程对象，创建一个字典
        courses_dict = {course.course_name: course for course in courses}

        # 遍历每个教室的排课信息
        for classroom in classrooms:
            for week, days in classroom.schedule.items():
                for day, sessions in days.items():
                    for session, course_names in sessions.items():
                        for course_name in course_names:
                            # 从字典中获取课程对象
                            course = courses_dict.get(course_name)
                            if course:
                                # 准备 SQL 插入语句
                                insert_stmt = """
                                    INSERT INTO teacher_classroom_course (tid, course_name, cid, wk, tday, clth, clname)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                                    ON DUPLICATE KEY UPDATE
                                    tid = VALUES(tid),
                                    course_name = VALUES(course_name),
                                    cid = VALUES(cid),
                                    wk = VALUES(wk),
                                    tday = VALUES(tday),
                                    clth = VALUES(clth),
                                    clname = VALUES(clname)
                                """
                                # 执行插入或更新操作
                                cursor.execute(insert_stmt, (
                                    course.teacher_id,
                                    course.course_name,
                                    course.course_id,
                                    week,
                                    day,
                                    session,
                                    classroom.clname  # 假设 classroom 实例有一个 clname 属性
                                ))

        # 提交更改
        conn.commit()
        print("Schedule successfully inserted/updated into database.")
        result = 1
    except Exception as e:
        print(f"Error inserting/updating schedule into database: {e}")
        result = 0
    finally:
        # 确保关闭数据库连接
        if conn.is_connected():
            cursor.close()
            conn.close()
    return result

def verify_user_login(username, password, ad, db_config):
    """
    验证用户登录信息。

    :param username: 用户名
    :param password: 密码
    :param ad: 管理员标识
    :param db_config: 登陆配置
    :return: 1表示用户名、密码和标识都正确，-1表示用户名和密码正确但标识不正确，0表示失败
    """
    try:
        # # 连接数据库
        # conn = mysql.connector.connect(**db_config)
        # 构建连接字符串
        conn_str = f'DRIVER={{SQL Server}};SERVER={db_config["server"]};DATABASE={db_config["database"]};UID={db_config["username"]};PWD={db_config["password"]}'

        # 连接数据库
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        # cursor = conn.cursor()

        # 首先检查用户名和密码是否正确
        # query = "SELECT COUNT(*) FROM account WHERE username =%s AND pw = %s"
        query = "SELECT COUNT(*) FROM account WHERE username =? AND pw = ?"
        cursor.execute(query, (username, password))
        result = cursor.fetchone()[0]

        if result > 0:
            # 用户名和密码正确，进一步检查管理员标识
            query = "SELECT COUNT(*) FROM account WHERE username = ? AND pw = ? AND ad = ?"
            cursor.execute(query, (username, password, ad))
            result = cursor.fetchone()[0]
            return 1 if result > 0 else -1
        else:
            # 用户名或密码不正确
            return 0
    except Exception as e:
        print(f"Error: {e}")
        return 0
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()



# 加载基础表
def load_data_from_excel_to_db(excel_path, db_config):
    """

    :param excel_path: excel文件的路径
    :param db_config: 数据库配置文件
    :return:
    """
    def import_classroom_info(excel_path, db_config):
        df = pd.read_excel(excel_path, sheet_name='教室基本信息')
        # # 连接数据库
        # conn = mysql.connector.connect(**db_config)
        # 构建连接字符串
        conn_str = f'DRIVER={{SQL Server}};SERVER={db_config["server"]};DATABASE={db_config["database"]};UID={db_config["username"]};PWD={db_config["password"]}'

        # 连接数据库
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        for index, row in df.iterrows():
            # insert_stmt = "INSERT INTO classroom (clname, crp) VALUES (%s, %s)"
            insert_stmt = "INSERT INTO classroom (clname, crp) VALUES (?, ?)"
            cursor.execute(insert_stmt, (row['clname'], row['capacity']))

        conn.commit()
        conn.close()
        print("教室信息已经成功导入数据库")

    def import_teacher_info(excel_path, db_config):
        df = pd.read_excel(excel_path, sheet_name='教师信息表')  # 请确认sheet名称是否正确
        # # 连接数据库
        # conn = mysql.connector.connect(**db_config)
        # 构建连接字符串
        conn_str = f'DRIVER={{SQL Server}};SERVER={db_config["server"]};DATABASE={db_config["database"]};UID={db_config["username"]};PWD={db_config["password"]}'

        # 连接数据库
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        for index, row in df.iterrows():
            insert_stmt = "INSERT INTO teacher (tid, tname, tsex) VALUES (?, ?, ?)"
            # insert_stmt = "INSERT INTO teacher (tid, tname, tsex) VALUES (%s, %s, %s)"
            # 确保字段对应的顺序与数据库中的表结构一致
            cursor.execute(insert_stmt, (row['teacher_id'], row['teacher_name'], row['teacher_sex']))

        conn.commit()
        conn.close()
        print("教师信息已经成功导入数据库")

    def import_teaching_info(excel_path, db_config):
        # 读取Excel文件中的"教师上课信息"表
        df = pd.read_excel(excel_path, sheet_name='教师上课信息')

        # # 连接数据库
        # conn = mysql.connector.connect(**db_config)
        # 构建连接字符串
        conn_str = f'DRIVER={{SQL Server}};SERVER={db_config["server"]};DATABASE={db_config["database"]};UID={db_config["username"]};PWD={db_config["password"]}'

        # 连接数据库
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # 遍历DataFrame的每一行
        for index, row in df.iterrows():
            # 准备SQL插入语句，确保字段与数据库中的字段匹配
            insert_stmt = """ 
                INSERT INTO course 
                (id, cid, cname, count, student_count, teacher_name, tid, clid) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            # 执行SQL插入语句
            cursor.execute(insert_stmt, (
                row['id'], row['course_id'], row['course_name'], row['count'],
                row['student_count'], row['teacher_name'], row['teacher_id'], row['clid']
            ))

        # 提交到数据库执行
        conn.commit()

        # 关闭游标和连接
        cursor.close()
        conn.close()

        print("教师上课信息已成功导入数据库。")

    import_classroom_info(excel_path, db_config)
    import_teacher_info(excel_path, db_config)
    import_teaching_info(excel_path, db_config)

# 查询教室是否空闲，返回矩阵
def query_classroom_schedule_matrix(db_config, classroom_name, week):
    '''

    :param db_config:
    :param classroom_name:
    :param week:
    :return: 返回一个矩阵，有课为0，无课为1
    '''
    # 初始化一个3x5的矩阵，初始值为1（空闲）
    schedule_matrix = [[1 for _ in range(5)] for _ in range(3)]

    try:
        # # 连接数据库
        # conn = mysql.connector.connect(**db_config)
        # 构建连接字符串
        conn_str = f'DRIVER={{SQL Server}};SERVER={db_config["server"]};DATABASE={db_config["database"]};UID={db_config["username"]};PWD={db_config["password"]}'

        # 连接数据库
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # 构建查询SQL
        query = """
            SELECT tday, clth
            FROM teacher_classroom_course
            WHERE clname = ? AND wk = ?
        """

        # 执行查询
        cursor.execute(query, (classroom_name, week))

        # 处理查询结果
        for tday, clth in cursor:
            day_index = tday - 1  # 星期转换为索引
            session_index = clth - 1   # 时间段转换为索引
            schedule_matrix[session_index][day_index] = 0  # 标记为占用

        # 关闭数据库连接
        cursor.close()
        conn.close()

    except Exception as e:
        print("Error querying classroom schedule:", e)

    return schedule_matrix


def query_classroom_schedule(db_config, classroom_name, week):
    '''

    :param db_config:
    :param classroom_name:
    :param week:
    :return: 返回一个嵌套字典，外层字典的键是星期几（例如，1 表示周一），而每个键对应的值是另一个字典，
            表示该天的课程安排。内层字典的键是课程时间段（例如，1、2、3 等），每个时间段的值是在那个时间段上的课程名称。
    '''
    schedule = {day: {} for day in range(1, 6)}  # 假设一周有5天，从周一到周五

    try:
        # # 连接数据库
        # conn = mysql.connector.connect(**db_config)
        # 构建连接字符串
        conn_str = f'DRIVER={{SQL Server}};SERVER={db_config["server"]};DATABASE={db_config["database"]};UID={db_config["username"]};PWD={db_config["password"]}'

        # 连接数据库
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # 查询指定教室和周次的课程安排
        query = """
            SELECT tday, clth, course_name
            FROM teacher_classroom_course
            WHERE clname = ? AND wk = ?
        """
        cursor.execute(query, (classroom_name, week))

        # 处理查询结果
        for tday, clth, course_name in cursor:
            if clth not in schedule[tday]:
                schedule[tday][clth] = course_name
            else:
                schedule[tday][clth] += ", " + course_name

        cursor.close()
        conn.close()

    except Exception as e:
        print("Error querying classroom schedule:", e)

    return schedule


# 查询教师课表
def query_teacher_schedule(db_config, teacher_id):
    '''

    :param db_config:
    :param teacher_id:
    :return: 返回一个嵌套字典，将每周的课程安排存储在一个嵌套的字典中，其中外层字典的键是周次，内层字典的键是节次
    '''
    # 初始化字典来存储课程安排
    schedule = {}

    try:
        # # 连接数据库
        # conn = mysql.connector.connect(**db_config)
        # 构建连接字符串
        conn_str = f'DRIVER={{SQL Server}};SERVER={db_config["server"]};DATABASE={db_config["database"]};UID={db_config["username"]};PWD={db_config["password"]}'

        # 连接数据库
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # 查询指定教师的课程安排
        query = """
            SELECT wk, clth, course_name, clname
            FROM teacher_classroom_course
            WHERE tid = %s
            ORDER BY wk, clth
        """
        cursor.execute(query, (teacher_id,))

        # 处理查询结果
        for wk, clth, course_name, clname in cursor:
            if wk not in schedule:
                schedule[wk] = {}
            schedule[wk][clth] = {'course_name': course_name, 'classroom': clname}

        cursor.close()
        conn.close()

    except Exception as e:
        print("Error querying teacher's schedule:", e)

    return schedule



# # 输入数据到基础表中
# load_data_from_excel_to_db(excel_path, db_config)
#
# # 安排课程表
# schedule_classes(classrooms, courses)
# # 将数据导入数据库
# insert_schedule_to_db(classrooms, courses, db_config)


# # 测试函数 query_classroom_schedule_matrix()
# classroom_name = "教2-201"
# week = 1  # 指定的周次
# schedule = query_classroom_schedule_matrix(db_config, classroom_name, week)
# # 打印课程表
# for row in schedule:
#     print(row)



# # 测试函数query_teacher_schedule(db_config, teacher_id)
# teacher_id = '1442'
# teacher_schedule = query_teacher_schedule(db_config, teacher_id)
#
# # 打印结果
# for wk, day_schedule in teacher_schedule.items():
#     print(f"Week {wk}:")
#     for clth, details in day_schedule.items():
#         print(f"  Session {clth}: {details['course_name']} in {details['classroom']}")
#


# # 测试登陆函数
# resule = verify_user_login('xuhuan', '123456', '0', db_config)
# print(resule)

