import os  # Provides functions for interacting with the operating system | used here to create directories and manage file paths
import pandas as pd  # 🐼
from pandas.tseries.offsets import (
    BDay,
)  # Provides business day offsets | used here to calculate the next school day for ExitWithdrawDate
import json  # Provides functions for working with JSON data | used here to read the config file
import logging  # Provides a logging system for tracking the execution of the code | used here to log progress and errors
from do_not_look.koala_sis_api import (
    KoalaSisDataClient,
)  # A module for interacting with the KoalaSis API | used to download student, school, and enrollment data
from datetime import (
    datetime,
    timedelta,
)  # Provides classes for manipulating dates and times | used here for date calculations and transformations
from typing import (
    Union,
)  # Provides a way to specify a type that can be one of several types | used here for function argument typing
import re  # Provides regular expression operations | used here to format names
import string  # Provides common string operations | used here to format names

# Global constants for input and output paths
credentials = "landing_zone_credentials"

# Specify the directories where raw data is stored (input_path) and where transformed data will be saved (output_path)
input_path = "data/koala_sis"
output_path = "data/data_mart"


# Set up logging configuration
logging.basicConfig(filename="pipeline.log", level=logging.INFO)


def process_data_generator_to_dataframe(data_generator):

    # A generator is a special type of iterable in Python, similar to a list or a tuple. However, unlike lists and tuples,
    # generators do not store their items in memory. Instead, they generate each item on-the-fly as you iterate through them.
    # This makes generators more memory-efficient for working with large data sets or when the entire collection doesn't need to be loaded into memory.

    # converts the input generator object into a list by calling the list() function on the generator.
    # The generator will yield each item (JSON string) one by one until it's exhausted, and these items will be collected into a list called data_list.

    data_list = list(data_generator)

    # list comprehension to loop through each JSON string in data_list and convert it into a Python dictionary using the json.loads() function.
    # The resulting dictionaries are stored in the data_records list.

    data_records = [json.loads(data) for data in data_list]

    # list comprehension to create a list of pandas DataFrames, where each DataFrame is created from a dictionary in data_records.
    # The pd.concat() function is then used to concatenate all these DataFrames into a single DataFrame.
    # The ignore_index=True parameter tells pandas to reset the index of the resulting DataFrame,
    # so the index values are continuous and don't repeat from the individual DataFrames.

    return pd.concat([pd.DataFrame(data) for data in data_records], ignore_index=True)


def download_data_to_csv():
    """
    Download data from the KoalaSis API and save it as CSV files in the input_path.
    The data includes students, schools, and enrollments information.
    """
    # Initialize the KoalaSis API client
    client = KoalaSisDataClient(credentials)

    # In order to probe the api, we can start by using the help function 'help(client)'
    # here this provides very useful information about the client and its methods

    # help(client)

    # something that was initially unexpected is that the client.get_student_data() method returns a generator object, so printing it will not return the data

    ##print(client.get_student_data()) # returns <generator object KoalaSisDataClient._get_data at 0x7f4017ee59e0>

    # to get the data, we can use the list() function to convert the generator object to a list, and then use json.loads() to convert the json string to a dictionary via a separate function

    students_generator = client.get_student_data()
    schools_generator = client.get_schools_data()
    enrollments_generator = client.get_enrollment_data()

    students = process_data_generator_to_dataframe(students_generator)
    schools = process_data_generator_to_dataframe(schools_generator)
    enrollments = process_data_generator_to_dataframe(enrollments_generator)

    # this now allows us to print the data as neat dataframes

    print(students.head())
    print(schools.head())
    print(enrollments.head())

    # Save the fetched data as CSV files in the specified directory
    os.makedirs(input_path, exist_ok=True)
    students.to_csv(os.path.join(input_path, "students.csv"), index=False)
    schools.to_csv(os.path.join(input_path, "schools.csv"), index=False)
    enrollments.to_csv(os.path.join(input_path, "enrollments.csv"), index=False)


def read_csv_files() -> pd.DataFrame:
    """
    Read CSV files for students, schools, and enrollments from the input_path.

    Returns:
        tuple: A tuple containing DataFrames for students, schools, and enrollments.
    """
    students = pd.read_csv(os.path.join(input_path, "students.csv"))
    schools = pd.read_csv(os.path.join(input_path, "schools.csv"))
    enrollments = pd.read_csv(os.path.join(input_path, "enrollments.csv"))
    return students, schools, enrollments


def format_gender(gender: str) -> str:
    """
    The function then uses the get() method on the gender_map dictionary to look up the short-form representation of the input gender.
    If the input gender is not found in the dictionary keys ( not 'male' or 'female'), the get() method will return a default value 'X'.
    This is specified as the second argument of the get() method.

    """
    gender_map = {"male": "M", "female": "F"}
    return gender_map.get(gender, "X")


def calculate_exit_withdraw_date(
    enrollment_end_date: Union[str, pd.Timestamp],
    school_end_date: Union[str, pd.Timestamp],
) -> pd.Timestamp:
    """
    Calculate the exit/withdrawal date for a student. Used BDay() to calculate the next business day.
    If the enrollment end date is null, the exit/withdrawal date is the school end date.
    If the enrollment end dat is the last day of school, the exit/withdrawal date is the last day of school, as there is no next school day.

    In this function definition, Union[str, pd.Timestamp] is used as a type hint for the function's parameters enrollment_end_date and school_end_date.
    The Union type from the typing module allows for specifying that a parameter can accept multiple types.
    In this case, the function is designed to accept either a string or a pd.Timestamp object as the input for the enrollment_end_date and school_end_date parameters.

    Using Union[str, pd.Timestamp] provides the following benefits:

    Flexibility: It allows the function to be more flexible by handling both string and pd.Timestamp inputs. This can be useful when working with dates that may come from different sources or formats.
    Type hinting: Type hints help developers understand the expected input types for a function, which can improve code readability and make it easier to debug.


    Args:
        enrollment_end_date (Union[str, pd.Timestamp]): The enrollment end date as a string or Timestamp.
        school_end_date (Union[str, pd.Timestamp]): The school end date as a string or Timestamp.

    Returns:
        pd.Timestamp: The exit/withdrawal date"""

    enrollment_end_date = pd.to_datetime(enrollment_end_date, errors="coerce")
    school_end_date = pd.to_datetime(school_end_date, errors="coerce")

    if pd.isna(enrollment_end_date):
        return school_end_date
    else:
        next_day = enrollment_end_date + pd.Timedelta(days=1)
        next_school_day = next_day + BDay()

        if next_school_day > school_end_date:
            return school_end_date
        else:
            return next_school_day


def capitalize_name_parts(name: str) -> str:
    """
    Capitalize the first letter of each part of a name.

    This function handles names with apostrophes, hyphens, and other punctuation marks.
    It capitalizes the first letter of each part of a name while leaving the
    remaining characters unchanged.

    The regular expression pattern r"\b\w" is used to match the first letter of each word in a string. Let's break down the components of this pattern:
    \b: This is a word boundary anchor. It matches the position between a word character (usually a letter, digit, or underscore) and a non-word character.
    It can also match the position at the beginning or end of a string if the string starts or ends with a word character.
    The word boundary anchor does not consume any characters; it just marks a position.

    \w: This is a shorthand character class that matches any word character. In most regex flavors, a word character is defined as any alphanumeric character (letter or digit) or an underscore (_).
    Specifically, \w is equivalent to [a-zA-Z0-9_].

    When used together as \b\w, the pattern matches the first word character that appears immediately after a word boundary.
    In other words, it matches the first letter of each word in the string. This pattern is commonly used when you want to perform an operation on the first letter of each word, such as capitalizing it.

    Args:
        name (str): The input name as a string.

    Returns:
        str: The formatted name with the first letter of each part capitalized.
    """

    """This code defines a function `capitalize_part` that takes a regular expression match object as input and returns the matched 
        string with its first character capitalized. Let's break down the code into its components:
        1. `def capitalize_part(match: re.Match) -> str:`

        Here, we define a function called `capitalize_part` with a single parameter `match`. 
        We use type hints to indicate that `match` should be of type `re.Match`, 
        which is a match object from the `re` (regular expressions) library. 
        The function is expected to return a string, as indicated by the `-> str` type hint.

        2. `part = match.group()`

        The `group()` method is called on the `match` object. 
        The method returns the entire match as a string. 
        In this case, it retrieves the matched string from the regular expression match object and stores it in a variable called `part`.

        3. `return part[0].upper() + part[1:]`

        This line constructs and returns a new string with the first character of `part` capitalized. Here's how it works:

        - `part[0]`: This retrieves the first character of the `part` string.
        - `part[0].upper()`: The `upper()` method is called on the first character to convert it to uppercase.
        - `part[1:]`: This retrieves a substring from the second character of `part` to the end of the string. This remains unchanged.
        - `part[0].upper() + part[1:]`: Finally, the capitalized first character and the rest of the string are concatenated together 
                    to form the new capitalized string.

        In summary, the `capitalize_part` function takes a regular expression match object, extracts the matched string, 
        capitalizes its first character, and returns the modified string."""

    def capitalize_part(match: re.Match) -> str:
        part = match.group()
        return part[0].upper() + part[1:]

    return re.sub(r"\b\w", capitalize_part, name)


def format_display_name(first_name: str, last_name: str) -> str:
    return f"{last_name.capitalize()}, {first_name.capitalize()}"


def assign_data_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Assign data types to the columns of a Pandas DataFrame to avoid any ambiguity and possible errors from data entry.

    Args:
        df (pd.DataFrame): The input DataFrame whose columns' data types need to be assigned.

    Returns:
        pd.DataFrame: A new DataFrame with the specified data types assigned to its columns.
    """

    data_types = {
        "StudentUniqueId": "int64",
        "FirstName": "object",
        "LastSurname": "object",
        "grade": "object",
        "Gender": "category",
        "id": "int64",
        "SchoolId": "int64",
        "academic_year": "object",
        "EntryDate": "datetime64[ns]",
        "enrollment_end_date": "datetime64[ns]",
        "notes": "object",
        "NameOfInstitution": "object",
        "start_grade": "object",
        "end_grade": "object",
        "start_date": "datetime64[ns]",
        "end_date": "datetime64[ns]",
    }
    return df.astype(data_types)


def merge_and_transform_data(
    students: pd.DataFrame, schools: pd.DataFrame, enrollments: pd.DataFrame
) -> pd.DataFrame:
    """
    Merge students, schools, and enrollments DataFrames and transform the data
    to include only necessary columns and apply transformations to certain fields.

    Args:
        students (pd.DataFrame): The students DataFrame.
        schools (pd.DataFrame): The schools DataFrame.
        enrollments (pd.DataFrame): The enrollments DataFrame.

    Returns:
        pd.DataFrame: The transformed and merged DataFrame.
    """
    # Merge the data on appropriate keys
    merged_data = (
        students.merge(  # .rename(columns={'id': 'StudentUniqueId'})
            enrollments,
            left_on="id",
            right_on="student_id",
            how="inner",
            suffixes=("", "_enrollment"),
        ).merge(
            schools,
            left_on="school_id",
            right_on="id",
            how="inner",
            suffixes=("", "_school"),
        )
        # .drop(columns={'student_id', 'id_school'})
        .rename(
            columns={
                "local_student_id": "StudentUniqueId",
                "school_id": "SchoolId",
                "school_name": "NameOfInstitution",
                "last_name": "LastSurname",
                "first_name": "FirstName",
                "gender": "Gender",
                "enrollment_start_date": "EntryDate",
            }
        )
    )

    merged_data = assign_data_types(merged_data)
    print(merged_data.dtypes)

    # Apply transformations to selected fields
    merged_data["LastSurname"] = merged_data["LastSurname"].apply(capitalize_name_parts)
    merged_data["FirstName"] = merged_data["FirstName"].apply(capitalize_name_parts)

    # This line creates a new column called DisplayName in the DataFrame. It uses the apply() function with a lambda function that calls the custom
    # format_display_name function, taking the FirstName and LastSurname columns as arguments.
    # The axis=1 parameter tells pandas to apply the function row-wise.

    merged_data["DisplayName"] = merged_data.apply(
        lambda row: format_display_name(row["FirstName"], row["LastSurname"]), axis=1
    )

    # This line applies another custom function called format_gender to each value in the Gender column. This function is expected
    # to handle the formatting and standardization of gender values. The transformed values replace the original values in the Gender column.

    merged_data["Gender"] = merged_data["Gender"].apply(format_gender)

    # This line creates a new column called ExitWithdrawDate in the DataFrame. It uses the apply() function with a lambda function that calls the custom
    # calculate_exit_withdraw_date function, taking the enrollment_end_date and end_date columns as arguments.
    # The axis=1 parameter tells pandas to apply the function row-wise.

    """ A lambda function is used within the apply() method to create a new column ExitWithdrawDate in the merged_data DataFrame. 
        The lambda function is an anonymous function that takes a single argument row, representing a row of the DataFrame. 
        The axis=1 parameter in the apply() method indicates that the function should be applied row-wise.
        Inside the lambda function, the calculate_exit_withdraw_date() function is called with two arguments: row['enrollment_end_date'] and row['end_date']. 
        These arguments correspond to the values of the enrollment_end_date and end_date columns for the current row. 
        The calculate_exit_withdraw_date() function is expected to compute the value for the ExitWithdrawDate column based on the given arguments.

        In short, the lambda function here serves as a concise way to pass each row of the DataFrame to the calculate_exit_withdraw_date() function 
        and create the new ExitWithdrawDate column with the computed values."""

    merged_data["ExitWithdrawDate"] = merged_data.apply(
        lambda row: calculate_exit_withdraw_date(
            row["enrollment_end_date"], row["end_date"]
        ),
        axis=1,
    )

    print(merged_data.head())
    merged_data.to_csv(os.path.join(input_path, "merged_data.csv"), index=False)

    # Select only necessary columns for the final output
    transformed_data = merged_data[
        [
            "SchoolId",
            "NameOfInstitution",
            "StudentUniqueId",
            "LastSurname",
            "FirstName",
            "DisplayName",
            "Gender",
            "EntryDate",
            "ExitWithdrawDate",
        ]
    ]

    return transformed_data


def validate_data(transformed_data: pd.DataFrame):
    """
    This fucntion validates the data to ensure that it does not contain any missing values or duplicate rows.


    """

    if transformed_data.isnull().any().any():
        raise ValueError("Data contains missing values.")

    if transformed_data.duplicated().any():
        raise ValueError("Data contains duplicate rows.")


def save_transformed_data(transformed_data: pd.DataFrame):
    """
    This function saves the transformed data to a CSV file in the output folder.


    """
    os.makedirs(output_path, exist_ok=True)
    transformed_data.to_csv(
        os.path.join(output_path, "student_demographics_and_enrollment.csv"),
        index=False,
    )


def conform_data():
    """
    This function is the main function of the pipeline. It calls all the other functions in the pipeline to download, conform, and save the data.

    """
    students, schools, enrollments = read_csv_files()
    transformed_data = merge_and_transform_data(students, schools, enrollments)
    validate_data(transformed_data)
    save_transformed_data(transformed_data)

    return transformed_data


def main():
    logging.info("Pipeline started")

    try:
        download_data_to_csv()
        logging.info("Data downloaded successfully")
    except Exception as e:
        logging.error(f"Error downloading data: {e}")
        raise

    try:
        transformed_data = conform_data()
        logging.info("Data conformed successfully")
    except Exception as e:
        logging.error(f"Error conforming data: {e}")
        raise

    logging.info("Pipeline completed")


if __name__ == "__main__":
    main()
