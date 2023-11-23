import contextlib
import sqlite3
import typing
import collections
import logging.config
import boto3
import redis

from fastapi import Depends, HTTPException, APIRouter, status, Request
from enrollment.enrollment_schemas import *
from enrollment.enrollment_dynamo import Enrollment, PartiQL
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
dynamodb = boto3.resource('dynamodb', endpoint_url='http://localhost:5500')

def get_table_resource(dynamodb, table_name):
    return dynamodb.Table(table_name)

# Create wrapper for PartiQL queries
wrapper = PartiQL(dynamodb)

# Connect to Redis
r = redis.Redis(db=1)

# Create class items
wl = Waitlist
enrollment = Enrollment(dynamodb)

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
    student_data = enrollment.get_user_item(student_id)

    #Check if exist
    if not student_data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Student not found")
    
    waitlist_count = wl.get_waitlist_count(student_id)

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
def enroll_student_in_class(student_id: int, class_id: int, request: Request, db: sqlite3.Connection = Depends(get_db)):
    
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

    # Check if the class exists in the database
    cursor.execute("SELECT * FROM class WHERE id = ?", (class_id,))
    class_data = cursor.fetchone()

    if not student_data or not class_data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Student or Class not found")

    # Check if student is already enrolled in the class
    cursor.execute("""SELECT * FROM enrollment
                    JOIN class ON enrollment.class_id = class.id
                    WHERE class_id = ? AND student_id = ?
                    """, (class_id, student_id))
    existing_enrollment = cursor.fetchone()

    if existing_enrollment:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Student is already enrolled in this class or currently on waitlist")
    
    # Increment enrollment number in the database
    new_enrollment = class_data['current_enroll'] + 1
    cursor.execute("UPDATE class SET current_enroll = ? WHERE id = ?", (new_enrollment, class_id))

    # Add student to enrolled class in the database
    cursor.execute("INSERT INTO enrollment (placement, student_id, class_id) VALUES (?, ?, ?)", (new_enrollment, student_id, class_id))
    
    # Remove student from dropped table if valid
    cursor.execute("""SELECT * FROM dropped 
                    WHERE class_id = ? AND student_id = ?
                    """, (class_id, student_id))
    dropped_data = cursor.fetchone()
    if dropped_data:
        cursor.execute("""DELETE FROM dropped 
                    WHERE class_id = ? AND student_id = ?
                    """, (class_id, student_id))

    # Check if the class is full, add student to waitlist if no
    # freeze is in place
    if class_data['current_enroll'] >= class_data['max_enroll']:
        if not FREEZE:
            if student_data['waitlist_count'] < MAX_WAITLIST and class_data['current_enroll'] < class_data['max_enroll'] + 15:
                cursor.execute("""UPDATE waitlist 
                                SET waitlist_count = waitlist_count + 1
                                WHERE student_id = ?""",(student_id,))
                db.commit()
                return {"message": "Student added to the waitlist"}
            else:
                return {"message": "Unable to add student to waitlist due to already having max number of waitlists"}
        else:
            return {"message": "Unable to add student to waitlist due to administrative freeze"}
    
    db.commit()

    return {"message": "Student succesfully enrolled in class"}


# Have a student drop a class they're enrolled in
@router.put("/students/{student_id}/classes/{class_id}/drop/", tags=['Student'])
def drop_student_from_class(student_id: int, class_id: int, request: Request, db: sqlite3.Connection = Depends(get_db)):
    
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

    #check enrollment
    cursor.execute("SELECT * FROM enrollment WHERE student_id = ? AND class_id = ?", (student_id, class_id))
    enrollment_data = cursor.fetchone()

    cursor.execute("""SELECT * FROM enrollment
                    JOIN class ON enrollment.class_id = class.id
                    WHERE enrollment.student_id = ? AND class_id = ?
                    AND enrollment.placement > class.max_enroll""", (student_id, class_id))
    waitlist_data = cursor.fetchone()
    
    if not enrollment_data or waitlist_data:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Student is not enrolled in the class")

    # remove student from class
    cursor.execute("DELETE FROM enrollment WHERE student_id = ? AND class_id = ?", (student_id, class_id))
    reorder_placement(cursor, class_data['current_enroll'], enrollment_data['placement'], class_id)

    # Update dropped table
    cursor.execute(""" INSERT INTO dropped (class_id, student_id)
                    VALUES (?, ?)""",(class_id, student_id))
    db.commit()
    
    return {"message": "Student successfully dropped class"}


#==========================================wait list========================================== 


# Get all waiting lists for a student
@router.get("/waitlist/students/{student_id}", tags=['Waitlist'])
def view_waiting_list(student_id: int, request: Request, db: sqlite3.Connection = Depends(get_db)):
    
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

    # Retrieve waitlist entries for the specified student from the database
    cursor.execute("SELECT waitlist_count FROM waitlist WHERE student_id = ? AND waitlist_count > 0", (student_id,))
    waitlist_data = cursor.fetchall()

    # Check if exist
    if not waitlist_data:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Student is not on a waitlist")  

    # fetch all relevant waitlist information for student
    cursor.execute("""
        SELECT class.id AS class_id, class.name AS class_name, class.course_code,
                class.section_number, department.id AS department_id,
                department.name AS department_name,
                users.uid AS instructor_id, users.name AS instructor_name,
                enrollment.placement - class.max_enroll AS waitlist_position
        FROM enrollment
        JOIN class ON enrollment.class_id = class.id
        JOIN users ON enrollment.student_id = users.uid
        JOIN department ON class.department_id = department.id
        JOIN instructor_class ON class.id = instructor_class.class_id
        WHERE users.uid = ? AND class.current_enroll > class.max_enroll
        """, (student_id,)
    )
    waitlist_data = cursor.fetchall()

    # Create a list to store the Waitlist_Student instances
    waitlist_list = []

    # Iterate through the query results and create Waitlist_Student instances
    for row in waitlist_data:
        waitlist_info = Waitlist_Student(
            class_id=row['class_id'],
            waitlist_position=row['waitlist_position']
        )
        waitlist_list.append(waitlist_info)

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
def get_instructor_enrollment(instructor_id: int, class_id: int, request: Request):
    
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
# ========================================================================================
    # @ BRIEF: Getting the user table resource and using it to retrieve the instructors id
    user = get_table_resource(dynamodb,USER_TABLE)
    classes = get_table_resource(dynamodb,CLASS_TABLE)

    user_response = user.get_item(
        Key={"id": instructor_id}
    )
    instructor_data = user_response.get("Item")
    
    # @BREIF: Getting the Instructor class
    classes = get_table_resource(dynamodb,CLASS_TABLE)
    class_response = classes.get_item(
        Key={'id': class_id}
    )
    class_data = class_response.get("Item")
    # instructor_data = enrollment.get_class_item(instructor_id)
    # class_data = enrollment.get_class_item(class_id)

    # @BRIEF: FOllowing if statements check if both the instructor and class exist
    if not instructor_data or not class_data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Instructor and/or class not found")

    # checking if the instructor and class exists
    if not instructor_data or not class_data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Instructor and/or class not found")

    # @ BREIF: getting the instructor id and class id to verify if instructor teaches certain class
    instructor_data = wrapper.run_partiql(
        f'SELECT * FROM {CLASS_TABLE} WHERE instructor_id = ? AND id = ?',[instructor_id, class_id]
    )
    
    # @ BREIF: Checks if the instructor data is not empty as well as the contents inside
    if 'Items' in instructor_data and instructor_data['Items']:
        # Grabbing the first item in the list
        retrieved_instructor_id = instructor_data['Items'][0].get('instructor_id')
        # varifies that the instructor id matches the one provided
        if retrieved_instructor_id == instructor_id:
            print("Instructor assigned to the class.")
        else:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Instructor not assigned to this class")
    else:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Class not found or instructor not assigned to this class")
    
    # Getting list of enrolled students
    enrolled_students = wrapper.run_partiql(
        f'SELECT enrolled FROM {CLASS_TABLE} WHERE id = ?', [class_id]
    )

    if "Items" in enrolled_students and enrolled_students["Items"]:
        enrolled_data = enrolled_students['Items'][0].get('enrolled', [])

        enrolled_list = []

        # Matches student id with name and print it out
        for student_id in enrolled_data:
            response = user.get_item(Key={'id': student_id})
            student_data = response.get("Item")

            if student_data and 'id' in student_data and 'name' in student_data:
                student_info = {
                'id': student_data['id'],
                'name': student_data['name'],
            }
            enrolled_list.append(student_info)
        return {"Enrolled": enrolled_list}
    else:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Class has no dropped students")


#view students who have dropped the class
@router.get("/instructors/{instructor_id}/classes/{class_id}/drop", tags=['Instructor'])
def get_instructor_dropped(instructor_id: int, class_id: int, request: Request):

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
    #============================================================================================
    # Getting the instructor id
    user = get_table_resource(dynamodb, USER_TABLE)
    user_response = user.get_item(
        Key={'id': instructor_id}
    )
    instructor_data = user_response.get('Item')

    # Getting the Instructor class
    classes = get_table_resource(dynamodb,CLASS_TABLE)
    class_response = classes.get_item(
        Key={'id': class_id}
    )
    class_data = class_response.get("Item")

    # checking if the instructor and class exists
    if not instructor_data or not class_data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Instructor and/or class not found")

    # getting the instructor id and class id
    instructor_data = wrapper.run_partiql(
        f'SELECT * FROM {CLASS_TABLE} WHERE instructor_id = ? AND id = ?',[instructor_id, class_id]
    )
    
    # checking if the instructor is assigned to class
    if 'Items' in instructor_data and instructor_data['Items']:
        retrieved_instructor_id = instructor_data['Items'][0].get('instructor_id')
        if retrieved_instructor_id == instructor_id:
            print("Instructor assigned to the class.")
        else:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Instructor not assigned to this class")
    else:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Class not found or instructor not assigned to this class")

    # getting list of dropped students
    dropped_students = wrapper.run_partiql(
        f'SELECT dropped FROM {CLASS_TABLE} WHERE id = ?', [class_id]
    )

    if 'Items' in dropped_students and dropped_students['Items']:
        dropped_data = dropped_students['Items'][0].get('dropped', [])

        # Fetch user names for dropped students
        dropped_student_names = []

        for student_id in dropped_data:
            response = user.get_item(Key={'id': student_id})
            student_data = response.get("Item")

            # Check if the user with the given ID exists
            if student_data and 'id' in student_data and 'name' in student_data:
                student_info = {
                'id': student_data['id'],
                'name': student_data['name'],
            }
            dropped_student_names.append(student_info)
    
        return {"Dropped": dropped_student_names}
    else:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Class has no dropped students")


#Instructor administratively drop students
@router.post("/instructors/{instructor_id}/classes/{class_id}/students/{student_id}/drop", tags=['Instructor'])
def instructor_drop_class(instructor_id: int, class_id: int, student_id: int, request: Request):
    
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
    
    user = get_table_resource(dynamodb, USER_TABLE)
    user_response = user.get_item(
        Key={'id': instructor_id}
    )
    instructor_data = user_response.get('Item')
    

    # student_data = cursor.fetchone()
    student = get_table_resource(dynamodb,USER_TABLE)
    user_response = student.get_item(
        Key={'id': student_id}
        # need waitlist info from reddis
    )
    student_data = user_response.get('Item')

    if not instructor_data or not student_data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Instructor and/or student not found")

    instructor_data = wrapper.run_partiql(
        f'SELECT * FROM {CLASS_TABLE} WHERE instructor_id = ? AND id = ?',[instructor_id, class_id]
    )
    
    # checking if the instructor is assigned to class
    if 'Items' in instructor_data and instructor_data['Items']:
        retrieved_instructor_id = instructor_data['Items'][0].get('instructor_id')
        if retrieved_instructor_id == instructor_id:
            print("Instructor assigned to the class.")
        else:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Instructor not assigned to this class")
    else:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Class not found or instructor not assigned to this class")

    enroll = wrapper.run_partiql(
        f'SELECT enrolled FROM {CLASS_TABLE} WHERE id = ?',[class_id]
    )

    class_data = get_table_resource(dynamodb,CLASS_TABLE)

    for item in enroll.get('Items', []):
        enrolled_data = item.get('enrolled', [])
        
        if student_id in enrolled_data:
            # Remove student_id from the enrolled list
            enrolled_data.remove(student_id)

            try:
                # Update DynamoDB with the modified enrolled list
                class_data.update_item(
                    Key={'id': class_id},
                    UpdateExpression='SET enrolled = :enrolled',
                    ExpressionAttributeValues={':enrolled': enrolled_data}
                )
                print(f"Student {student_id} removed from enrolled list.")
            except Exception as e:
                print(f"Error updating enrolled list: {e}")
                raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Error updating enrolled list")
        else:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Student not enrolled in this class")
   

    return {"Message" : "Student successfully dropped"}


#==========================================registrar==================================================
# Create a new class
@router.post("/registrar/classes/", tags=['Registrar'])
def create_class(class_data: Class_Registrar):

    class_table = get_table_resource(dynamodb,CLASS_TABLE)

    existing_class = class_table.get_item(Key={'id': class_data.id})

    if existing_class.get("Item"):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Class with ID {class_data.id} already exists"
        )

    class_items = {
        'id': class_data.id,
        'name': class_data.name,
        'course_code': class_data.course_code,
        'section_number': class_data.section_number,
        'current_enroll': class_data.current_enroll,
        'max_enroll': class_data.max_enroll,
        'department_id': class_data.department_id,
        'instructor_id': class_data.instructor_id
    }

    try:
        class_response = class_table.put_item(Item = class_items)

        response_data = {
            'id': class_data.id,
            "name": class_data.name,
            "course_code": class_data.course_code,
            "section_number": class_data.section_number,
            "current_enroll": class_data.current_enroll,
            "max_enroll": class_data.max_enroll,
            "department_id": class_data.department_id,
            "instructor_id": class_data.instructor_id
        }

        return response_data

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
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