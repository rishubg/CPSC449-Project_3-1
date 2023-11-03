from pydantic import BaseModel
from pydantic_settings import BaseSettings
from typing import List

class Settings(BaseSettings, env_file=".env", extra="ignore"):
    enrollment_database: str
    enrollment_logging_config: str

class Department(BaseModel):
    id: int
    name: str

class Instructor(BaseModel):
    id: int
    name: str

class Student(BaseModel):
    id: int
    name: str

class Class_Info(BaseModel):
    id: int
    name: str
    course_code: str
    section_number: int
    current_enroll: int
    max_enroll: int
    department: Department
    instructor: Instructor

class Waitlist_Info(BaseModel):
    id: int
    name: str
    course_code: str
    section_number: int
    max_enroll: int
    department: Department
    instructor: Instructor
    waitlist_total: int

class Waitlist_Student(BaseModel):
    id: int
    name: str
    course_code: str
    section_number: int
    department: Department
    instructor: Instructor
    waitlist_position: int

class Waitlist_Instructor(BaseModel):
    student: Student
    waitlist_position: int

class Enrolled(BaseModel):
    student: Student
    position: int

class Class(BaseModel):
    name: str
    course_code: str
    section_number: int
    current_enroll: int
    max_enroll: int
    department_id: int

class Class_Registrar(BaseModel):
    name: str
    course_code: str
    section_number: int
    current_enroll: int
    max_enroll: int
    department_id: int
    instructor_id: int

class Enrollment(BaseModel):
    placement: int
    class_id: int
    student_id: int

class Dropped(BaseModel):
    class_id: int
    student_id: int

class User_info(BaseModel):
    uid: int
    name: str
    roles: List

class Create_User(BaseModel):
    name: str
    roles: List