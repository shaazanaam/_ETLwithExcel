import pandas as pd

def process_data(data):
    """
    Process the API data using Pandas DataFrame.

    Parameters:
        data (list of dict): The JSON data from the API response.

    Returns:
        pd.DataFrame: The processed DataFrame.
    """
    # Convert the JSON list to a Pandas DataFrame
    df = pd.DataFrame(data)

    # Print the DataFrame to visualize its structure
    print("Preview of the DataFrame:")
    print(df.head())  # Display the first 5 rows

    return df

