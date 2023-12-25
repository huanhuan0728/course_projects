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

# Read Excel data
excel_path = '/Users/xuhuanlu/Desktop/副本上课教室安排测试数据new.xlsx'
classrooms_df = pd.read_excel(excel_path, sheet_name='教室基本信息')
courses_df = pd.read_excel(excel_path, sheet_name='教师上课信息')

# Create Classroom and Course instances
classrooms = [Classroom(**row) for index, row in classrooms_df.iterrows()]
courses = [Course(**row) for index, row in courses_df.iterrows()]

# 把基本表加入到数据库中
def load_data_from_excel_to_db(excel_path, db_config):
    """

    :param excel_path: excel文件的路径
    :param db_config: 数据库配置文件
    :return:
    """
    def import_classroom_info(excel_path, db_config):
        df = pd.read_excel(excel_path, sheet_name='教室基本信息')
        # 构建连接字符串
        conn_str = f'DRIVER={{SQL Server}};SERVER={db_config["server"]};DATABASE={db_config["database"]};UID={db_config["username"]};PWD={db_config["password"]}'

        # 连接数据库
        conn = pyodbc.connect(conn_str)        
        cursor = conn.cursor()

        for index, row in df.iterrows():
            insert_stmt = "INSERT INTO classroom (clname, crp) VALUES (%s, %s)"
            cursor.execute(insert_stmt, (row['clname'], row['capacity']))

        conn.commit()
        conn.close()
        print("教室信息已经成功导入数据库")

    def import_teacher_info(excel_path, db_config):
        df = pd.read_excel(excel_path, sheet_name='教师信息表')  # 请确认sheet名称是否正确
        # 构建连接字符串
        conn_str = f'DRIVER={{SQL Server}};SERVER={db_config["server"]};DATABASE={db_config["database"]};UID={db_config["username"]};PWD={db_config["password"]}'
        # 连接数据库
        conn = pyodbc.connect(conn_str)        
        cursor = conn.cursor()

        for index, row in df.iterrows():
            insert_stmt = "INSERT INTO teacher (tid, tname, tsex) VALUES (%s, %s, %s)"
            # 确保字段对应的顺序与数据库中的表结构一致
            cursor.execute(insert_stmt, (row['teacher_id'], row['teacher_name'], row['teacher_sex']))

        conn.commit()
        conn.close()
        print("教师信息已经成功导入数据库")

    def import_teaching_info(excel_path, db_config):
        # 读取Excel文件中的"教师上课信息"表
        df = pd.read_excel(excel_path, sheet_name='教师上课信息')

        # 连接到数据库
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

def verify_user_login(username, password):
    '''

    :param username: 用户名
    :param password: 密码
    :return: 1表示成功，0表示失败
    '''
    try:
        # 连接到数据库
        # 构建连接字符串
        conn_str = f'DRIVER={{SQL Server}};SERVER={db_config["server"]};DATABASE={db_config["database"]};UID={db_config["username"]};PWD={db_config["password"]}'
        # 连接数据库
        conn = pyodbc.connect(conn_str)          
        cursor = conn.cursor()
        query = "SELECT COUNT(*) FROM account WHERE username = %s AND pw = %s"
        cursor.execute(query, (username, password))
        result = cursor.fetchone()[0]
        conn.close()
        return 1 if result > 0 else 0
    except Exception as e:
        print(f"Error: {e}")
        return 0

# 安排教室的算法
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



