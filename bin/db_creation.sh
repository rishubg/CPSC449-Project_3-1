#!/bin/bash

DBE_LOCATION="./enrollment/enrollment.db"
DBU_LOCATION="./var/primary/fuse/users.db"

python3 ./enrollment/populate_enrollment.py

if test -f $DBE_LOCATION; then
	echo "Enrollment service database has been created."
fi

python3 ./users/populate_users.py

if test -f $DBU_LOCATION; then
	echo "Users service database has been created."
fi