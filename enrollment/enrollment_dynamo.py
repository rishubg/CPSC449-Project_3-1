import logging

from botocore.exceptions import ClientError


# Configure the logger
logger = logging.getLogger(__name__)

table_prefix = "enrollment_"
DEBUG = False

class Enrollment:
    """Encapsulates an Amazon DynamoDB table of enrollment data."""

    def __init__(self, dyn_resource):
        """
        :param dyn_resource: A Boto3 DynamoDB resource.
        """
        self.dyn_resource = dyn_resource
        # The table variable is set during the scenario in the call to
        # 'exists' if the table exists. Otherwise, it is set by 'create_table'.
        self.table = None


    def create_table(self, table_name):
        """
        Creates an Amazon DynamoDB table. The table uses an id for the partition key.

        :param table_name: The name of the table to create.
        :return: The newly created table.
        """
        try:
            self.table = self.dyn_resource.create_table(
                TableName=table_prefix + table_name,
                KeySchema=[
                    {'AttributeName': 'id', 'KeyType': 'HASH'},  # Partition key
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'id', 'AttributeType': 'N'},
                ],
                ProvisionedThroughput={
                    "ReadCapacityUnits": 10,
                    "WriteCapacityUnits": 10,
                },
            )
            self.table.wait_until_exists()
        except ClientError as err:
            logger.error(
                "Couldn't create table %s. Here's why: %s: %s",
                table_name,
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            raise
        else:
            return self.table
    

    def delete_table(self, table_name):
        """
        Deletes an Amazon DynamoDB table.

        :param table_name: The name of the table to delete.
        """
        try:
            table = self.dyn_resource.Table(table_prefix + table_name)
            table.delete()
            table.wait_until_not_exists()
        except ClientError as err:
            logger.error(
                "Couldn't delete table %s. Here's why: %s: %s",
                table_name,
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            raise
    

    def add_class(self, class_data):
        """
        Adds a class to the table.

        :param class_data: a class object.
        """
        try:
            self.put_item(Item=dict(class_data))
        except ClientError as err:
            logger.error(
                "Couldn't add class %s to table %s. Here's why: %s: %s",
                class_data.id,
                self.table.name,
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            raise
    

    def add_user(self, user_data):
        """
        Adds a user to the table.

        :param user_data: a user object.
        """
        try:
            self.put_item(Item=dict(user_data))
        except ClientError as err:
            logger.error(
                "Couldn't add class %s to table %s. Here's why: %s: %s",
                user_data.id,
                self.table.name,
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            raise


    def get_enrollment_item(self, id):
        """
        Gets item data from the table for a specific id.

        :param id: The integer id for the item.
        :return: The data about the requested item.
        """
        try:
            response = self.table.get_item(Key={"id": id})
        except ClientError as err:
            logger.error(
                "Couldn't get movie %s from table %s. Here's why: %s: %s",
                id,
                self.table.name,
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            raise
        else:
            return response["Item"]
    

    def check_table_exists(self, table_name):
        """
        Check if a table exist in the database.

        :param table_name: name of the table that is being checked
        :return: Either true or false.
        """
        dynamodb_client = self.dyn_resource.meta.client
        try:
            dynamodb_client.describe_table(TableName=table_name)
            if DEBUG:
                print(f"Table {table_name} exists in DynamoDB.")
            return True
        except dynamodb_client.exceptions.ResourceNotFoundException:
            if DEBUG:
                print(f"Table {table_name} does not exist in DynamoDB.")
            return False


class PartiQL:
    """
    Encapsulates a DynamoDB resource to run PartiQL statements.
    """

    def __init__(self, dyn_resource):
        """
        :param dyn_resource: A Boto3 DynamoDB resource.
        """
        self.dyn_resource = dyn_resource


    def run_partiql(self, statement, params):
        """
        Runs a PartiQL statement. A Boto3 resource is used even though
        `execute_statement` is called on the underlying `client` object because the
        resource transforms input and output from plain old Python objects (POPOs) to
        the DynamoDB format. If you create the client directly, you must do these
        transforms yourself.

        :param statement: The PartiQL statement.
        :param params: The list of PartiQL parameters. These are applied to the
                       statement in the order they are listed.
        :return: The items returned from the statement, if any.
        """
        try:
            output = self.dyn_resource.meta.client.execute_statement(
                Statement=statement, Parameters=params
            )
        except ClientError as err:
            if err.response["Error"]["Code"] == "ResourceNotFoundException":
                logger.error(
                    "Couldn't execute PartiQL '%s' because the table does not exist.",
                    statement,
                )
            else:
                logger.error(
                    "Couldn't execute PartiQL '%s'. Here's why: %s: %s",
                    statement,
                    err.response["Error"]["Code"],
                    err.response["Error"]["Message"],
                )
            raise
        else:
            return output
    
    def run_partiql_statement(self, statement):
        """
        Runs a PartiQL statement. A Boto3 resource is used even though
        `execute_statement` is called on the underlying `client` object because the
        resource transforms input and output from plain old Python objects (POPOs) to
        the DynamoDB format. If you create the client directly, you must do these
        transforms yourself.

        :param statement: The PartiQL statement.
        :return: The items returned from the statement, if any.
        """
        try:
            output = self.dyn_resource.meta.client.execute_statement(
                Statement=statement
            )
        except ClientError as err:
            if err.response["Error"]["Code"] == "ResourceNotFoundException":
                logger.error(
                    "Couldn't execute PartiQL '%s' because the table does not exist.",
                    statement,
                )
            else:
                logger.error(
                    "Couldn't execute PartiQL '%s'. Here's why: %s: %s",
                    statement,
                    err.response["Error"]["Code"],
                    err.response["Error"]["Message"],
                )
            raise
        else:
            return output