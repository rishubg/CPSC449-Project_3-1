import contextlib
import sqlite3
import typing
import collections
import logging.config
import boto3
import redis

from fastapi import Depends, HTTPException, APIRouter, status, Request
from enrollment.enrollment_schemas import *
from enrollment.enrollment_dynamo import PartiQL
from enrollment.enrollment_redis import Waitlist

settings = Settings()
router = APIRouter()
dropped = []

CLASS_TABLE = "enrollment_class"
USER_TABLE = "enrollment_user"
DEBUG = False
FREEZE = False
MAX_WAITLIST = 3
#Remove when all endpoints are updated
database = "enrollment/enrollment.db"

def get_logger():
    return logging.getLogger(__name__)

# Connect to the old database
# Remove when all endpoints are updated
def get_db(logger: logging.Logger = Depends(get_logger)):
    with contextlib.closing(sqlite3.connect(database, check_same_thread=False)) as db:
        db.row_factory = sqlite3.Row
        db.set_trace_callback(logger.debug)
        yield db

# Connect to DynamoDB
def get_dynamodb():
    return boto3.resource('dynamodb', endpoint_url='http://localhost:5500')

def get_table_resource(dynamodb, table_name):
    return dynamodb.Table(table_name)

# Create wrapper for PartiQL queries
def get_wrapper(dynamodb):
    return PartiQL(dynamodb)

# Connect to Redis
r = redis.Redis(db=1)

# Called when a student is dropped from a class / waiting list
# and the enrollment place must be reordered
def reorder_placement(cur, total_enrolled, placement, class_id):
    counter = 1
    while counter <= total_enrolled:
        if counter > placement:
            cur.execute("""UPDATE enrollment SET placement = placement - 1 
                WHERE class_id = ? AND placement = ?""", (class_id,counter))
        counter += 1
    cur.execute("""UPDATE class SET current_enroll = current_enroll - 1
                WHERE id = ?""",(class_id,))


# Used for the search endpoint
SearchParam = collections.namedtuple("SearchParam", ["name", "operator"])
SEARCH_PARAMS = [
    SearchParam(
        "uid",
        "=",
    ),
    SearchParam(
        "name",
        "LIKE",
    ),
    SearchParam(
        "role",
        "LIKE",
    ),
]


logging.config.fileConfig(settings.enrollment_logging_config, disable_existing_loggers=False)


#==========================================students==================================================


#gets available classes for a student
@router.get("/students/{student_id}/classes", tags=['Student']) 
def get_available_classes(student_id: int, request: Request):

    db = get_dynamodb()
    # initialize the wrapper for partiql. There are two separate functions for partiql,
    # run_partiql, and run_partiql_statement. Use run_partiql if you are using a statement and parameters.
    # Use run_partiql_statement if you are just using a statement. Both are used in this endpoint if you
    # need examples on how to use them. You may also look in the 'enrollment_dynamo.py' file for how it was
    # implemented.
    wrapper = get_wrapper(db)

    # User Authentication
    if request.headers.get("X-User"):
        current_user = int(request.headers.get("X-User"))
    
        roles_string = request.headers.get("X-Roles")
        current_roles = roles_string.split(",")

        r_flag = True
        # Check if the current user's role matches 'registrar'
        for role in current_roles:
            if role == 'registrar':
                r_flag = False
    
        # Check if the current user's id matches the requested student_id
        if r_flag:
            if current_user != student_id:
                raise HTTPException(status_code=403, detail="Access forbidden, wrong user")
    
    user_table = get_table_resource(db, USER_TABLE)
    # Fetch student data from db
    response = user_table.get_item(
        Key={
            'id': student_id
        }
    )

    student_data = response.get('Item')

    #Check if exist
    if not student_data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Student not found")
    
    waitlist_count = Waitlist.get_waitlist_count(student_id)

    # If max waitlist, don't show full classes with open waitlists
    if waitlist_count >= MAX_WAITLIST:
        output = wrapper.run_partiql_statement(
            f'SELECT * FROM "{CLASS_TABLE}" WHERE current_enroll <= max_enroll'
        )

    # Else show all open classes or full classes with open waitlists
    else:
        # All classes have a max_enroll value of 30, and a max waitlist value of 15,
        # so 30 + 15 = 45. Technically classes can be created with any max_enroll value,
        # but I cant use partiql with arithmatic, for example I cant do
        # "WHERE current_enroll < (max_enroll + 15)". So for now its just 45
        output = wrapper.run_partiql_statement(
            f'SELECT * FROM "{CLASS_TABLE}" WHERE current_enroll < 45',
        )

    # Create a list to store the Class instances
    class_instances = []

    # Iterate through the query results and create Class instances
    for item in output['Items']:
        # get instructor information
        result = wrapper.run_partiql(
            f'SELECT * FROM "{USER_TABLE}" WHERE id=?',
            [item['instructor_id']]
        )
        # Get waitlist information
        if item['current_enroll'] > item['max_enroll']:
            current_enroll = item['max_enroll']
            waitlist = item['current_enroll'] - item['max_enroll']
        else:
            current_enroll = item['current_enroll']
            waitlist = 0
        # Create the class instance
        class_instance = Class_Enroll(
            id=item['id'],
            name=item['name'],
            course_code=item['course_code'],
            section_number=item['section_number'],
            current_enroll=current_enroll,
            max_enroll=item['max_enroll'],
            department=item['department'],
            instructor=Instructor(id=item['instructor_id'], name=result['Items'][0]['name']),
            current_waitlist=waitlist,
            max_waitlist=15
        )
        class_instances.append(class_instance)

    return {"Classes": class_instances}


# Enrolls a student into an available class,
# or will automatically put the student on an open waitlist for a full class
@router.post("/students/{student_id}/classes/{class_id}/enroll", tags=['Student'])
def enroll_student_in_class(student_id: int, class_id: int, request: Request):
    
    db = get_dynamodb() 
    wrapper = get_wrapper(db)

    # User Authentication
    if request.headers.get("X-User"):

        current_user = int(request.headers.get("X-User"))
    
        roles_string = request.headers.get("X-Roles")
        current_roles = roles_string.split(",")

        r_flag = True
        # Check if the current user's role matches 'registrar'
        for role in current_roles:
            if role == 'registrar':
                r_flag = False
    
        # Check if the current user's id matches the requested student_id
        if r_flag:
            if current_user != student_id:
                raise HTTPException(status_code=403, detail="Access forbidden, wrong user")

   # Fetch student data from db
    user_table = get_table_resource(db,USER_TABLE)
    response_1 = user_table.get_item(
        Key={
            'id': student_id
        }
    )
    student_data = response_1.get('Item')

    # Fetch class data from db
    class_table = get_table_resource(db, CLASS_TABLE)
    response_2 = class_table.get_item(
        Key={
            'id': class_id
        }
    )
    class_data = response_2.get('Item')

    # Check if the class and student exists in the database
    if not student_data or not class_data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Student or Class not found")
    
    # Check if student is already enrolled in the class
    # get student information
    student_enrollment = wrapper.run_partiql(
        f'SELECT * FROM "{CLASS_TABLE}" WHERE id=?',
        [class_id]
    )
    # check the information in the table
    for item in student_enrollment["Items"]:
        if student_id in item.get('enrolled', []):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Student is already enrolled in this class or currently on waitlist")

    # Increment enrollment number in the database
    new_enrollment = class_data.get('current_enroll', 0) + 1

    class_table.update_item(
        Key={
            'id': class_id
        },
        UpdateExpression='SET current_enroll = :new_enrollment',
        ExpressionAttributeValues={':new_enrollment': new_enrollment}
    )

    # Add student to enrolled class in the database
    class_table.update_item(
        Key={
            'id': class_id
        },
        UpdateExpression='SET enrolled = list_append(enrolled, :student_id)',
        ExpressionAttributeValues={':student_id': [student_id]}
    )

    # get class information
    student_enrolled = wrapper.run_partiql(
        f'SELECT * FROM "{CLASS_TABLE}" WHERE id=?',[class_id]
    )
    
    # Remove student from dropped table if valid
    for item in student_enrolled['Items']:
        get_dropped = item.get('dropped', [])
        if student_id in get_dropped:
            # remove student from dropped
            get_dropped.remove(student_id)
            # udpate enrolled table with the removed student
            class_table.update_item(
                Key={
                    'id': class_id
                },
                UpdateExpression='SET dropped = :dropped',
                ExpressionAttributeValues={':dropped': get_dropped}
            )

    # Check if the class is full, add student to waitlist if no
    ## code goes here
    if new_enrollment >= class_data.get('max_enroll', 0):
        # freeze is in place
        if not FREEZE:
            waitlist_count = Waitlist.get_waitlist_count(student_id)
            if waitlist_count < MAX_WAITLIST and new_enrollment < class_data.get('max_enroll', 0) + 15:
                user_table.update_item(
                    Key={'id': student_id},
                    UpdateExpression='SET waitlist_count = if_not_exists(waitlist_count, :zero) + :waitlist_count',
                    ExpressionAttributeValues={':zero': 0, ':waitlist_count': 1}
                )
                return {"message": "Student added to the waitlist"}
            else:
                return {"message": "Unable to add student to waitlist due to already having the maximum number of waitlists"}
        else:
            return {"message": "Unable to add student to waitlist due to administrative freeze"}

    return {"message": "Student successfully enrolled in class"}


# Have a student drop a class they're enrolled in
@router.put("/students/{student_id}/classes/{class_id}/drop/", tags=['Student'])
def drop_student_from_class(student_id: int, class_id: int, request: Request):
    
    db = get_dynamodb() 
    wrapper = get_wrapper(db)
    
    # user authentication
    if request.headers.get("X-User"):
        current_user = int(request.headers.get("X-User"))
    
        roles_string = request.headers.get("X-Roles")
        current_roles = roles_string.split(",")

        r_flag = True
        # Check if the current user's role matches 'registrar'
        for role in current_roles:
            if role == 'registrar':
                r_flag = False
    
        # Check if the current user's id matches the requested student_id
        if r_flag:
            if current_user != student_id:
                raise HTTPException(status_code=403, detail="Access forbidden, wrong user")
    

    # fetch data for the suer
    user_table = get_table_resource(db, USER_TABLE)
    user_response = user_table.get_item(
        Key={
            'id': student_id
        }
    )
    student_data = user_response.get('Item')

    # fetch data for the class
    class_table = get_table_resource(db, CLASS_TABLE)
    class_response = class_table.get_item(
        Key={
            'id': class_id
        }
    )
    class_data = class_response.get('Item')
    
    # Check if the class and student exists in the database
    if not student_data or not class_data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Student or Class not found")

    # fetch enrollment information
    enrollment_data = wrapper.run_partiql(
        f'SELECT * FROM "{CLASS_TABLE}" WHERE id=?',[class_id]
    )

    # fetch waitlist information
    waitlist_data = Waitlist.is_student_on_waitlist(student_id, class_id)

    # check if the student is enrolled or on the waitlist
    for item in enrollment_data['Items']:
        check_enroll = item.get('enrolled', [])
        if student_id not in check_enroll or waitlist_data:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Student is not enrolled in the class")
    
    # remove student from class
    for item in enrollment_data['Items']:
        # store the student that is enrolled
        student_enroll = item.get('enrolled', [])
        if student_id in student_enroll:
            # remove student from enrolled
            student_enroll.remove(student_id)
            # udpate enrolled table with the removed student
            class_table.update_item(
                Key={
                    'id': class_id
                },
                UpdateExpression='SET enrolled = :enrolled',
                ExpressionAttributeValues={':enrolled': student_enroll}
            )

    # Update dropped table
    class_table.update_item(
        Key={
            'id': class_id
        },
        UpdateExpression='SET dropped = list_append(dropped, :student_id)',
        ExpressionAttributeValues={':student_id': [student_id]}
    )
    
    return {"message": "Student successfully dropped class"}


#==========================================wait list========================================== 


# Get all waiting lists for a student
@router.get("/waitlist/students/{student_id}", tags=['Waitlist'])
def view_waiting_list(student_id: int, request: Request):

    db = get_dynamodb() 
    wrapper = get_wrapper(db)
    
    if request.headers.get("X-User"):
        current_user = int(request.headers.get("X-User"))
    
        roles_string = request.headers.get("X-Roles")
        current_roles = roles_string.split(",")

        r_flag = True
        # Check if the current user's role matches 'registrar'
        for role in current_roles:
            if role == 'registrar':
                r_flag = False
    
        # Check if the current user's id matches the requested student_id
        if r_flag:
            if current_user != student_id:
                raise HTTPException(status_code=403, detail="Access forbidden, wrong user")
    
    # Retrieve waitlist entries for the specified student from the databas
    student_key = "student:{}:waitlists"
    waitlist_data = r.zrange(student_key.format(student_id), 0, -1, withscores=True)

    # cursor.execute("SELECT waitlist_count FROM waitlist WHERE student_id = ? AND waitlist_count > 0", (student_id,))
    # waitlist_data = cursor.fetchall()

    # Check if exist
    if not waitlist_data:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Student is not on a waitlist")  

    # fetch all relevant waitlist information for student
    output = wrapper.run_partiql_statement(
        f'SELECT * FROM "{CLASS_TABLE}" WHERE current_enroll >= max_enroll'
    )
    # cursor.execute("""
    #     SELECT class.id AS class_id, class.name AS class_name, class.course_code,
    #             class.section_number, department.id AS department_id,
    #             department.name AS department_name,
    #             users.uid AS instructor_id, users.name AS instructor_name,
    #             enrollment.placement - class.max_enroll AS waitlist_position
    #     FROM enrollment
    #     JOIN class ON enrollment.class_id = class.id
    #     JOIN users ON enrollment.student_id = users.uid
    #     JOIN department ON class.department_id = department.id
    #     JOIN instructor_class ON class.id = instructor_class.class_id
    #     WHERE users.uid = ? AND class.current_enroll > class.max_enroll
    #     """, (student_id,)
    # )
    # waitlist_data = cursor.fetchall()

    # Create a list to store the Waitlist_Student instances
    waitlist_list = []

    # Iterate through the query results and create Waitlist_Student instances
    for item in output['Items']:
        result = wrapper.run_partiql(
            f'SELECT * FROM "{USER_TABLE}" where_id=?',
            [item['instructor_id']]
        )
        # get waitlist information
        waitlist_info = Waitlist_Student(
            id=item['id'],
            name=item['name'],
            course_code=item['cours e_code'],
            section_number=item['section_number'],
            department=item['department'],
            instructor=Instructor(id=item['instructor_id'], name=result['Items'][0]['name']),
            waitlist_position=item['waitlist_position']
        )
        waitlist_list.append(waitlist_info)
    
    # for row in waitlist_data:
    #     waitlist_info = Waitlist_Student(
    #         id=row['class_id'],
    #         name=row['class_name'],
    #         course_code=row['course_code'],
    #         section_number=row['section_number'],
    #         department=Department(id=row['department_id'], name=row['department_name']),
    #         instructor=Instructor(id=row['instructor_id'], name=row['instructor_name']),
    #         waitlist_position=row['waitlist_position']
    #     )
    #     waitlist_list.append(waitlist_info)

    return {"Waitlists": waitlist_list}


# remove a student from a waiting list
@router.put("/waitlist/students/{student_id}/classes/{class_id}/drop", tags=['Waitlist'])
def remove_from_waitlist(student_id: int, class_id: int, request: Request, db: sqlite3.Connection = Depends(get_db)):
    
    if request.headers.get("X-User"):
        current_user = int(request.headers.get("X-User"))
    
        roles_string = request.headers.get("X-Roles")
        current_roles = roles_string.split(",")

        r_flag = True
        # Check if the current user's role matches 'registrar'
        for role in current_roles:
            if role == 'registrar':
                r_flag = False
    
        # Check if the current user's id matches the requested student_id
        if r_flag:
            if current_user != student_id:
                raise HTTPException(status_code=403, detail="Access forbidden, wrong user")
    
    cursor = db.cursor()
    
    # check if exist
    cursor.execute(
        """
        SELECT * FROM users
        JOIN user_role ON users.uid = user_role.user_id
        JOIN role ON user_role.role_id = role.rid
        WHERE uid = ? AND role = ?
        """, (student_id, 'student')
    )
    student_data = cursor.fetchone()

    cursor.execute("SELECT * FROM class WHERE id = ?", (class_id,))
    class_data = cursor.fetchone()

    if not student_data or not class_data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Student or Class not found")

    cursor.execute("""SELECT class.current_enroll, enrollment.placement
                    FROM enrollment 
                    JOIN class ON enrollment.class_id = class.id
                    JOIN users ON enrollment.student_id
                    WHERE student_id = ? AND class_id = ?
                    AND enrollment.placement > class.max_enroll
                    """, (student_id, class_id))
    waitlist_entry = cursor.fetchone()

    if waitlist_entry is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Student is not on the waiting list for this class")

    # Delete student from waitlist enrollment
    cursor.execute("DELETE FROM enrollment WHERE student_id = ? AND class_id = ?", (student_id, class_id))
    cursor.execute("""UPDATE waitlist SET waitlist_count = waitlist_count - 1
                    WHERE student_id = ?""", (student_id,))
    
    # Reorder enrollment placements
    reorder_placement(cursor, waitlist_entry['current_enroll'], waitlist_entry['placement'], class_id)
    db.commit()

    return {"message": "Student removed from the waiting list"}


# Get a list of students on a waitlist for a particular class that
# a specific instructor teaches
@router.get("/waitlist/instructors/{instructor_id}/classes/{class_id}",tags=['Waitlist'])
def view_current_waitlist(instructor_id: int, class_id: int, request: Request, db: sqlite3.Connection = Depends(get_db)):
    
    if request.headers.get("X-User"):
        current_user = int(request.headers.get("X-User"))
    
        roles_string = request.headers.get("X-Roles")
        current_roles = roles_string.split(",")

        r_flag = True
        # Check if the current user's role matches 'registrar'
        for role in current_roles:
            if role == 'registrar':
                r_flag = False
    
        # Check if the current user's id matches the requested instructor_id
        if r_flag:
            if current_user != instructor_id:
                raise HTTPException(status_code=403, detail="Access forbidden, wrong user")
    
    cursor = db.cursor()

   # check if exist
    cursor.execute(
        """
        SELECT * FROM users
        JOIN user_role ON users.uid = user_role.user_id
        JOIN role ON user_role.role_id = role.rid
        WHERE uid = ? AND role = ?
        """, (instructor_id, 'instructor')
    )
    instructor_data = cursor.fetchone()

    cursor.execute("SELECT * FROM class WHERE id = ?", (class_id,))
    class_data = cursor.fetchone()

    if not instructor_data or not class_data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Instructor or Class not found")  

    cursor.execute(
        """
        SELECT * FROM instructor_class
        WHERE instructor_id = ? AND class_id = ?
        """, (instructor_id, class_id)
    )
    instructor_class_data = cursor.fetchone()

    if not instructor_class_data:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Instructor not assigned to this class")
    
    # fetch all relevant waitlist information for instructor
    cursor.execute("""
        SELECT enrollment.student_id AS student_id,
        users.name AS student_name,
        enrollment.placement - class.max_enroll AS waitlist_position
        FROM enrollment
        JOIN users ON enrollment.student_id = users.uid
        JOIN class ON enrollment.class_id = class.id
        JOIN instructor_class ON class.id = instructor_class.class_id
        JOIN department ON class.department_id = department.id
        WHERE instructor_class.instructor_id = ? AND class.id = ?
        AND enrollment.placement > class.max_enroll
        """, (instructor_id, class_id)
    )
    waitlist_data = cursor.fetchall()

    #Check if exist
    if not waitlist_data:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Class does not have a waitlist")

    # Create a list to store the Waitlist_Instructor instances
    waitlist_list = []

    # Iterate through the query results and create Waitlist_Instructor instances
    for row in waitlist_data:
        waitlist_info = Waitlist_Instructor(
            student=Student(id=row['student_id'], name=row['student_name']),
            waitlist_position=row['waitlist_position']
        )
        waitlist_list.append(waitlist_info)

    return {"Waitlist": waitlist_list}


#==========================================Instructor==================================================


#view current enrollment for class
@router.get("/instructors/{instructor_id}/classes/{class_id}/enrollment", tags=['Instructor'])
def get_instructor_enrollment(instructor_id: int, class_id: int, request: Request, db: sqlite3.Connection = Depends(get_db)):
    
    if request.headers.get("X-User"):
        current_user = int(request.headers.get("X-User"))
    
        roles_string = request.headers.get("X-Roles")
        current_roles = roles_string.split(",")

        r_flag = True
        # Check if the current user's role matches 'registrar'
        for role in current_roles:
            if role == 'registrar':
                r_flag = False
    
        # Check if the current user's id matches the requested instructor_id
        if r_flag:
            if current_user != instructor_id:
                raise HTTPException(status_code=403, detail="Access forbidden, wrong user")
    
    cursor = db.cursor()

    #check if exist
    cursor.execute(
        """
        SELECT * FROM users
        JOIN user_role ON users.uid = user_role.user_id
        JOIN role ON user_role.role_id = role.rid
        WHERE uid = ? AND role = ?
        """, (instructor_id, 'instructor')
    )
    instructor_data = cursor.fetchone()

    cursor.execute("SELECT * FROM class WHERE id = ?", (class_id,))
    class_data = cursor.fetchone()

    if not instructor_data or not class_data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Instructor and/or class not found")

    cursor.execute(
        """
        SELECT * FROM instructor_class
        WHERE instructor_id = ? AND class_id = ?
        """, (instructor_id, class_id)
    )
    instructor_class_data = cursor.fetchone()

    if not instructor_class_data:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Instructor not assigned to this class")

    #Fetch relavent data for instructor
    cursor.execute("""SELECT users.uid AS student_id,
                    users.name AS student_name, enrollment.placement
                    FROM enrollment 
                    JOIN class ON enrollment.class_id = class.id
                    JOIN users ON enrollment.student_id = users.uid
                    JOIN instructor_class ON class.id = instructor_class.class_id
                    WHERE instructor_class.instructor_id = ?
                    AND instructor_class.class_id = ? 
                    AND enrollment.placement <= class.max_enroll
                    AND class.current_enroll > 0""", (instructor_id, class_id))
    enrolled_data = cursor.fetchall()

    #Check if exist
    if not enrolled_data:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Class has no students enrolled")

    # Create a list to store the Waitlist_Instructor instances
    enrolled_list = []

    # Iterate through the query results and create Waitlist_Instructor instances
    for row in enrolled_data:
        enrolled_info = Enrolled(
            student=Student(id=row['student_id'], name=row['student_name']),
            position=row['placement']
        )
        enrolled_list.append(enrolled_info)

    return {"Enrolled": enrolled_list}


#view students who have dropped the class
@router.get("/instructors/{instructor_id}/classes/{class_id}/drop", tags=['Instructor'])
def get_instructor_dropped(instructor_id: int, class_id: int, request: Request, db: sqlite3.Connection = Depends(get_db)):
    
    if request.headers.get("X-User"):
        current_user = int(request.headers.get("X-User"))
    
        roles_string = request.headers.get("X-Roles")
        current_roles = roles_string.split(",")

        r_flag = True
        # Check if the current user's role matches 'registrar'
        for role in current_roles:
            if role == 'registrar':
                r_flag = False
    
        # Check if the current user's id matches the requested instructor_id
        if r_flag:
            if current_user != instructor_id:
                raise HTTPException(status_code=403, detail="Access forbidden, wrong user")
    
    cursor = db.cursor()

    #Check if exist
    cursor.execute(
        """
        SELECT * FROM users
        JOIN user_role ON users.uid = user_role.user_id
        JOIN role ON user_role.role_id = role.rid
        WHERE uid = ? AND role = ?
        """, (instructor_id, 'instructor')
    )
    instructor_data = cursor.fetchone()

    cursor.execute("SELECT * FROM class WHERE id = ?", (class_id,))
    class_data = cursor.fetchone()

    if not instructor_data or not class_data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Instructor and/or class not found")

    cursor.execute(
        """
        SELECT * FROM instructor_class
        WHERE instructor_id = ? AND class_id = ?
        """, (instructor_id, class_id)
    )
    instructor_class_data = cursor.fetchone()

    if not instructor_class_data:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Instructor not assigned to this class")
    
    cursor.execute("""SELECT dropped.student_id AS student_id, users.name AS student_name
                        FROM dropped 
                        JOIN users ON dropped.student_id = users.uid
                        WHERE dropped.class_id = ?""", (class_id,))
    dropped_data = cursor.fetchall()

    #Check if exist
    if not dropped_data:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Class has no dropped students")

    # Create a list to store the Student instances
    dropped_list = []

    # Iterate through the query results and create Waitlist_Instructor instances
    for row in dropped_data:
        student_info = Student(
            id=row['student_id'],
            name=row['student_name']
        )
        dropped_list.append(student_info)
    
    return {"Dropped": dropped_list}


#Instructor administratively drop students
@router.post("/instructors/{instructor_id}/classes/{class_id}/students/{student_id}/drop", tags=['Instructor'])
def instructor_drop_class(instructor_id: int, class_id: int, student_id: int, request: Request, db: sqlite3.Connection = Depends(get_db)):
    
    if request.headers.get("X-User"):
        current_user = int(request.headers.get("X-User"))
    
        roles_string = request.headers.get("X-Roles")
        current_roles = roles_string.split(",")

        r_flag = True
        # Check if the current user's role matches 'registrar'
        for role in current_roles:
            if role == 'registrar':
                r_flag = False
    
        # Check if the current user's id matches the requested instructor_id
        if r_flag:
            if current_user != instructor_id:
                raise HTTPException(status_code=403, detail="Access forbidden, wrong user")
    
    cursor = db.cursor()

    #Check if exist
    cursor.execute(
        """
        SELECT * FROM users
        JOIN user_role ON users.uid = user_role.user_id
        JOIN role ON user_role.role_id = role.rid
        WHERE uid = ? AND role = ?
        """, (instructor_id, 'instructor')
    )
    instructor_data = cursor.fetchone()
    
    cursor.execute(
        """
        SELECT * FROM users
        JOIN user_role ON users.uid = user_role.user_id
        JOIN role ON user_role.role_id = role.rid
        JOIN waitlist ON users.uid = waitlist.student_id
        WHERE uid = ? AND role = ?
        """, (student_id, 'student')
    )
    student_data = cursor.fetchone()

    if not instructor_data or not student_data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Instructor and/or student not found")

    cursor.execute(
        """
        SELECT * FROM instructor_class
        WHERE instructor_id = ? AND class_id = ?
        """, (instructor_id, class_id)
    )
    instructor_class_data = cursor.fetchone()

    if not instructor_class_data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Class not found, or instructor not assigned to this class")

    cursor.execute("""SELECT * FROM enrollment
                        JOIN class ON enrollment.class_id = class.id
                        WHERE class_id = ? AND student_id = ?
                    """,(class_id, student_id))
    enroll_data = cursor.fetchone()

    if not enroll_data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Student not enrolled in this class")
    
    # remove student from class
    cursor.execute("DELETE FROM enrollment WHERE student_id = ? AND class_id = ?", (student_id, class_id))
    reorder_placement(cursor, enroll_data['current_enroll'], enroll_data['placement'], class_id)

    db.commit()

    return {"Message" : "Student successfully dropped"}


#==========================================registrar==================================================


# Create a new class
@router.post("/registrar/classes/", tags=['Registrar'])
def create_class(class_data: Class_Registrar, db: sqlite3.Connection = Depends(get_db)):
    
    try:
        cursor = db.cursor()

        cursor.execute(
            """
            INSERT INTO class (name, course_code, section_number, current_enroll, max_enroll, department_id)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                class_data.name,
                class_data.course_code,
                class_data.section_number,
                class_data.current_enroll,
                class_data.max_enroll,
                class_data.department_id,
            )
        )
        
        # Get the last inserted row id (the id of the newly created class)
        class_id = cursor.lastrowid

        cursor.execute(
            """
            INSERT INTO instructor_class (instructor_id, class_id)
            VALUES (?, ?)
            """,
            (
                class_data.instructor_id,
                class_id,
            )
        )
        db.commit()

        # Construct the response JSON object
        response_data = {
            "class_id": class_id,
            "name": class_data.name,
            "course_code": class_data.course_code,
            "section_number": class_data.section_number,
            "current_enroll": class_data.current_enroll,
            "max_enroll": class_data.max_enroll,
            "department_id": class_data.department_id,
            "instructor_id": class_data.instructor_id
        }
        
        return response_data
    
    except sqlite3.IntegrityError as e:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"type": type(e).__name__, "msg": str(e)}
        )

# Remove a class
@router.delete("/registrar/classes/{class_id}", tags=['Registrar'])
def remove_class(class_id: int, db: sqlite3.Connection = Depends(get_db)):

    cursor = db.cursor()

    # Check if the class exists in the database
    cursor.execute("SELECT * FROM class WHERE id = ?", (class_id,))
    class_data = cursor.fetchone()

    if not class_data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Class not found")

    # Delete the class from the database
    cursor.execute("DELETE FROM class WHERE id = ?", (class_id,))
    db.commit()

    return {"message": "Class removed successfully"}


# Change the assigned instructor for a class
@router.put("/registrar/classes/{class_id}/instructors/{instructor_id}", tags=['Registrar'])
def change_instructor(class_id: int, instructor_id: int, db: sqlite3.Connection = Depends(get_db)):
    cursor = db.cursor()

    cursor.execute("SELECT * FROM class WHERE id = ?", (class_id,))
    class_data = cursor.fetchone()

    if not class_data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Class not found")

    cursor.execute(
        """
        SELECT * FROM users
        JOIN user_role ON users.uid = user_role.user_id
        JOIN role ON user_role.role_id = role.rid
        WHERE uid = ? AND role = ?
        """, (instructor_id, 'instructor')
    )
    instructor_data = cursor.fetchone()

    if not instructor_data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Instructor not found")

    cursor.execute("UPDATE instructor_class SET instructor_id = ? WHERE class_id = ?", (instructor_id, class_id))
    db.commit()

    return {"message": "Instructor changed successfully"}


# Freeze enrollment for classes
@router.put("/registrar/automatic-enrollment/freeze", tags=['Registrar'])
def freeze_automatic_enrollment():
    global FREEZE
    if FREEZE:
        FREEZE = False
        return {"message": "Automatic enrollment unfrozen successfully"}
    else:
        FREEZE = True
        return {"message": "Automatic enrollment frozen successfully"}


# Create a new user (used by the user service to duplicate user info)
@router.post("/registrar/create_user", tags=['Registrar'])
def create_user(user: Create_User, db: sqlite3.Connection = Depends(get_db)):
    
    if DEBUG:
        print("username: ",user.name)
        print("roles: ", user.roles)

    cursor = db.cursor()

    cursor.execute("INSERT INTO users (name) VALUES (?)", (user.name,))
    
    for role in user.roles:
        cursor.execute("SELECT rid FROM role WHERE role = ?", (role,))
        rid = cursor.fetchone()
        
        cursor.execute(
        """
        SELECT * FROM users WHERE name = ?
        """, (user.name,)
        )
        user_data = cursor.fetchone()
        
        if DEBUG:
            print("User ID: ", user_data['uid'])
        
        cursor.execute(
            """
            INSERT INTO user_role (user_id, role_id)
            VALUES (?, ?)
            """, (user_data['uid'], rid['rid'])
        )

    db.commit()

    return {"Message": "user created successfully"}

#==========================================Test Endpoints==================================================

# None of the following endpoints are required (I assume), but might be helpful
# for testing purposes

# Gets currently enrolled classes for a student
@router.get("/debug/students/{student_id}/enrolled", tags=['Debug'])
def view_enrolled_classes(student_id: int, db: sqlite3.Connection = Depends(get_db)):
    cursor = db.cursor()
    
    # Check if the student exists in the database
    cursor.execute(
        """
        SELECT * FROM users
        JOIN user_role ON users.uid = user_role.user_id
        JOIN role ON user_role.role_id = role.rid
        JOIN waitlist ON users.uid = waitlist.student_id
        WHERE uid = ? AND role = ?
        """, (student_id, 'student')
    )
    student_data = cursor.fetchone()

    if not student_data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Student not found")

    # Check if the student is enrolled in any classes
    cursor.execute("""
        SELECT class.id AS class_id, class.name AS class_name, class.course_code,
                class.section_number, class.current_enroll, class.max_enroll,
                department.id AS department_id, department.name AS department_name,
                users.uid AS instructor_id, users.name AS instructor_name
            FROM class
            JOIN department ON class.department_id = department.id
            JOIN instructor_class ON class.id = instructor_class.class_id
            JOIN users ON instructor_class.instructor_id = users.uid
            JOIN enrollment ON class.id = enrollment.class_id
            WHERE enrollment.student_id = ? AND class.current_enroll < class.max_enroll
        """, (student_id,))
    enrolled_data = cursor.fetchall()

    if not enrolled_data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Student not enrolled in any classes")

    # Create a list to store the Class_Info instances
    enrolled_list = []

    # Iterate through the query results and create Class_Info instances
    for row in enrolled_data:
        class_info = Class_Info(
            id=row['class_id'],
            name=row['class_name'],
            course_code=row['course_code'],
            section_number=row['section_number'],
            current_enroll=row['current_enroll'],
            max_enroll=row['max_enroll'],
            department=Department(id=row['department_id'], name=row['department_name']),
            instructor=Instructor(id=row['instructor_id'], name=row['instructor_name'])
        )
        enrolled_list.append(class_info)
    
    return {"Enrolled": enrolled_list}


# Get all classes with active waiting lists
@router.get("/debug/waitlist/classes", tags=['Debug'])
def view_all_class_waitlists(db: sqlite3.Connection = Depends(get_db)):
    cursor = db.cursor()  

    # fetch all relevant waitlist information
    cursor.execute("""
        SELECT class.id AS class_id, class.name AS class_name, class.course_code,
                class.section_number, class.max_enroll,
                department.id AS department_id, department.name AS department_name,
                users.uid AS instructor_id, users.name AS instructor_name,
                class.current_enroll - class.max_enroll AS waitlist_total
            FROM class
            JOIN department ON class.department_id = department.id
            JOIN instructor_class ON class.id = instructor_class.class_id
            JOIN users ON instructor_class.instructor_id = users.uid
            WHERE class.current_enroll > class.max_enroll
        """
    )
    waitlist_data = cursor.fetchall()

    # Check if exist
    if not waitlist_data:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No classes have waitlists")

    # Create a list to store the Waitlist_Info instances
    waitlist_list = []

    # Iterate through the query results and create Waitlist_Info instances
    for row in waitlist_data:
        waitlist_info = Waitlist_Info(
            id=row['class_id'],
            name=row['class_name'],
            course_code=row['course_code'],
            section_number=row['section_number'],
            max_enroll=row['max_enroll'],
            department=Department(id=row['department_id'], name=row['department_name']),
            instructor=Instructor(id=row['instructor_id'], name=row['instructor_name']),
            waitlist_total=row['waitlist_total']
        )
        waitlist_list.append(waitlist_info)

    return {"Waitlists": waitlist_list}


# Search for specific users based on optional parameters,
# if no parameters are given, returns all users
@router.get("/debug/search", tags=['Debug'])
def search_for_users(uid: typing.Optional[str] = None,
                 name: typing.Optional[str] = None,
                 role: typing.Optional[str] = None,
                 db: sqlite3.Connection = Depends(get_db)):
    
    users_info = []

    sql = """SELECT * FROM users
             LEFT JOIN user_role ON users.uid = user_role.user_id
             LEFT JOIN role ON user_role.role_id = role.rid"""
    
    conditions = []
    values = []
    arguments = locals()

    for param in SEARCH_PARAMS:
        if arguments[param.name]:
            if param.operator == "=":
                conditions.append(f"{param.name} = ?")
                values.append(arguments[param.name])
            else:
                conditions.append(f"{param.name} LIKE ?")
                values.append(f"%{arguments[param.name]}%")
    
    if conditions:
        sql += " WHERE "
        sql += " AND ".join(conditions)

    cursor = db.cursor()

    cursor.execute(sql, values)
    search_data = cursor.fetchall()

    if not search_data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No users found that match search parameters")

    previous_uid = None
    for user in search_data:
        cursor.execute(
            """
            SELECT role FROM users 
            JOIN role ON user_role.role_id = role.rid
            JOIN user_role ON users.uid = user_role.user_id
            WHERE uid = ?
            """,
            (user["uid"],)
        )
        roles_data = cursor.fetchall()
        roles = [role["role"] for role in roles_data]

        if previous_uid != user["uid"]:
            user_information = User_info(
                uid=user["uid"],
                name=user["name"],
                password=user["password"],
                roles=roles
            )
            users_info.append(user_information)
        previous_uid = user["uid"]

    return {"users" : users_info}


# List all classes
@router.get("/debug/classes", tags=['Debug'])
def list_all_classes(request: Request, db: sqlite3.Connection = Depends(get_db)):
    
    print(request.headers)
    
    cursor = db.cursor()
    cursor.execute("""
            SELECT class.id AS class_id, class.name AS class_name, class.course_code,
                class.section_number, class.current_enroll, class.max_enroll,
                department.id AS department_id, department.name AS department_name,
                users.uid AS instructor_id, users.name AS instructor_name
            FROM class
            JOIN department ON class.department_id = department.id
            JOIN instructor_class ON class.id = instructor_class.class_id
            JOIN users ON instructor_class.instructor_id = users.uid
        """)
    class_data = cursor.fetchall()

    if not class_data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No classes found")
    
    # Create a list to store the Class_Info instances
    class_info_list = []

    # Iterate through the query results and create Class_Info instances
    for row in class_data:
        class_info = Class_Info(
            id=row['class_id'],
            name=row['class_name'],
            course_code=row['course_code'],
            section_number=row['section_number'],
            current_enroll=row['current_enroll'],
            max_enroll=row['max_enroll'],
            department=Department(id=row['department_id'], name=row['department_name']),
            instructor=Instructor(id=row['instructor_id'], name=row['instructor_name'])
        )
        class_info_list.append(class_info)

    return {"Classes" : class_info_list}