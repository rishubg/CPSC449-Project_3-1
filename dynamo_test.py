import logging
import boto3

from decimal import Decimal
from botocore.exceptions import ClientError

# Configure the logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define dummy credentials
dummy_access_key = 'AKIAIOSFODNN7EXAMPLE'
dummy_secret_key = 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
dummy_region = 'us-west-2'

# Create a session with the dummy credentials
session = boto3.Session(
    aws_access_key_id=dummy_access_key,
    aws_secret_access_key=dummy_secret_key,
    region_name=dummy_region
)

# Create a DynamoDB client using the session
dynamodb = session.client('dynamodb')


class Movies:
    """Encapsulates an Amazon DynamoDB table of movie data."""

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
        Creates an Amazon DynamoDB table that can be used to store movie data.
        The table uses the release year of the movie as the partition key and the
        title as the sort key.

        :param table_name: The name of the table to create.
        :return: The newly created table.
        """
        try:
            self.table = self.dyn_resource.create_table(
                TableName=table_name,
                KeySchema=[
                    {"AttributeName": "year", "KeyType": "HASH"},  # Partition key
                    {"AttributeName": "title", "KeyType": "RANGE"},  # Sort key
                ],
                AttributeDefinitions=[
                    {"AttributeName": "year", "AttributeType": "N"},
                    {"AttributeName": "title", "AttributeType": "S"},
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

    def add_movie(self, title, year, plot, rating):
        """
        Adds a movie to the table.

        :param title: The title of the movie.
        :param year: The release year of the movie.
        :param plot: The plot summary of the movie.
        :param rating: The quality rating of the movie.
        """
        try:
            self.table.put_item(
                Item={
                    "year": year,
                    "title": title,
                    "info": {"plot": plot, "rating": Decimal(str(rating))},
                }
            )
        except ClientError as err:
            logger.error(
                "Couldn't add movie %s to table %s. Here's why: %s: %s",
                title,
                self.table.name,
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            raise

    def get_movie(self, title, year):
        """
        Gets movie data from the table for a specific movie.

        :param title: The title of the movie.
        :param year: The release year of the movie.
        :return: The data about the requested movie.
        """
        try:
            response = self.table.get_item(Key={"year": year, "title": title})
        except ClientError as err:
            logger.error(
                "Couldn't get movie %s from table %s. Here's why: %s: %s",
                title,
                self.table.name,
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            raise
        else:
            return response["Item"]
        
def dynamoTest():
     # Initialize a Boto3 DynamoDB resource
    dynamodb_resource = boto3.resource('dynamodb', region_name='us-west-2')

    # Initialize the Movies class with the DynamoDB resource
    movies = Movies(dynamodb_resource)

    # Name for the table you want to create
    table_name = "MoviesTable"

    # Create the DynamoDB table
    created_table = movies.create_table(table_name)

    print("Table Details:")
    print(f"{created_table}")

    # Add a movie to the table
    title = "Movie Title"
    year = 2022
    plot = "This is the movie plot."
    rating = 8.5
    movies.add_movie(title, year, plot, rating)

    # Read and print the movie from the table
    movie_data = movies.get_movie(title, year)
    print("Movie Details:")
    print(f"Title: {movie_data['title']}")
    print(f"Year: {movie_data['year']}")
    print(f"Plot: {movie_data['info']['plot']}")
    print(f"Rating: {movie_data['info']['rating']}")

if __name__ == "__main__":
    dynamoTest()
