import requests
import pandas as pd
import certifi
print(certifi.where())
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Initialize an empty DataFrame
MAX_RECORDS = 10000
offset = 0
limit = 1000  # Number of records per page
all_data = pd.DataFrame()
column_mapping = {
    "sc_394": "Provided by Description",
    "sc_388": "Provided by",
    "sc_384": "Agency Website",
    "sc_1665": "Phone Number",
    "sc_823": "Current Status",
    "sc_1149": "Agency Service Description",
    "sc_3083": "Eligibility Description",
    "sc_3083_id" : "Applicable Age Group",
    
    "sc_986": "Office Hours",
    "sc_493_address_1": "Address Line 1",
    "sc_493_address_2": "Address Line 2",
    "sc_493_city": "City",
    "sc_493_county": "County",
    "sc_493_state": "State",
    "sc_493_zip": "Zip Code",
    "sc_493_country": "Country",
    "sc_493_notes": "Notes",
    "sc_493": "Full Address"
}



    # Define the API URL
url_template = f"https://211wisconsin.communityos.org/publicguidedsearch/results/limit/1000/offset/{offset}/order/site%5Csite_addressus%5Csite_addressus%5Czdr/direction/asc"


# Define the headers
headers = {
            "Accept": "application/json",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
            "Content-Type": "application/json",
            "Cookie": "s=4be88422-1b28-4b76-bcb9-46f61637f066; _ga=GA1.1.1785416669.1732424566; d=e272a5f3-5e54-4676-b273-0ae0fd6b2c5e; _ga_0MQQDKXZTY=GS1.1.1732424565.1.1.1732425683.60.0.0",
            "Host": "211wisconsin.communityos.org",
            "Origin": "https://211wisconsin.communityos.org",
            "Referer": "https://211wisconsin.communityos.org/publicguidedsearch/render/ds/%7B%22service%5C%5Cservice_taxonomy%5C%5Cmodule_servicepost%22%3A%7B%22value%22%3A%5B%7B%22taxonomy_id%22%3A412966%7D%5D%2C%22operator%22%3A%5B%22contains_array%22%5D%7D%2C%22agency%5C%5Cagency_system%5C%5Cname%22%3A%7B%22value%22%3A%22VLTEST%22%2C%22operator%22%3A%5B%22notequals%22%5D%7D%7D?localHistory=KDbUUnrHpDOP5PLeWwA0DQ",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
            "X-Attempt-Rawbody-First": "true",
            "X-Requested-With": "XMLHttpRequest",
            "sec-ch-ua": "\"Microsoft Edge\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\""
        }

        # Define the payload
        # payload = {
        #     "Vactive": "{\"value\":\"true\",\"operator\":[\"equals\"]}",
        #     "revision": {"revision": {"id": "", "record_name": "agency", "token": ""}},
        #     "service\\service_system\\active": "{\"value\":\"true\",\"operator\":[\"equals\"]}",
        #     "service\\service_taxonomy\\module_servicepost": "{\"value\":[{\"id\":412966,\"edit_stamp\":\"2024-11-14 03:49:29.844998-06\",\"audit_stamp\":null,\"create_stamp\":\"2024-11-14 03:49:29.844998-06\",\"create_session_id\":17426026,\"edit_session_id\":17426026,\"create_account_id\":801,\"edit_account_id\":801,\"type\":\"hsis\",\"taxo_version\":\"2016-11-17T15:00:21Z\",\"code\":\"RX-0400.1500-150\",\"name\":\"Central Intake/Assessment for Alcohol Use Disorder\",\"definition\":\"Programs that evaluate individuals who may have an alcohol use disorder and triage them for the limited number of subsidized beds that may be available in the community. Some programs may also offer access to medical detoxification services for people who need them. \",\"facet\":\"Service\",\"comments\":\"\",\"bibliographic_reference\":\"\",\"hsis_created_date\":\"2010-03-11\",\"hsis_modified_date\":\"2016-01-26\",\"status\":true,\"parent_id\":413331,\"full_name\":\"Mental Health and Substance Use Disorder Services - Substance Use Disorder Services - Assessment for Substance Use Disorders - Central Intake/Assessment for Substance Use Disorders - Central Intake/Assessment for Alcohol Use Disorder\",\"approved\":true,\"old_codes\":null,\"taxonomy_id\":412966,\"left_index\":11995,\"right_index\":11996,\"level\":5}],\"operator\":[\"contains_arrayall\"]}",
        #     "site\\site_addressus\\site_addressus": "{\"site\\\\site_addressus\\\\site_addressus\\\\zip\":{\"operator\":[\"contains\"]}}",
        #     "site\\site_system\\active": "{\"value\":\"true\",\"operator\":[\"equals\"]}"
        # }

        # Make the request
try:
        while len(all_data)<MAX_RECORDS:
            url =url_template.format(offset=offset)
            
           #send the request 
            response = requests.post(url, headers=headers,  verify=False)


        # Check the response
            if response.status_code == 200:
                print(f"Failed to fetch data. Status Code: {response.status_code}")
                data = response.json()

                # Extract the facets mapping 
                facets = data.get("facets",[])
                sc_3083_mapping ={}
                for facet in facets:
                    if "sc_3083" in facet:
                        sc_3083_mapping={
                            item["value"]: item["label"] for item in data["facets"][0]["sc_3083"]
                        }
                        break

                if "data" in data:
                    # Extract the list of records from the "data" key
                    records = data["data"]

                    # Define the keys you want to extract
                    keys_to_extract = [
                        "sc_394", "sc_388", "sc_384", "sc_1665", "sc_823", 
                        "sc_1149", "sc_3083","sc_3083_id", "sc_986", 
                        "sc_493_address_1", "sc_493_address_2", "sc_493_city", 
                        "sc_493_county", "sc_493_state", "sc_493_zip", 
                        "sc_493_country", "sc_493_notes", "sc_493"
                    ]
                    # Extract the specified keys for each record
                    extracted_data = []
                    for record in records:
                        row = {key: record.get(key, None) for key in keys_to_extract}
                        
                        # Map sc_3083_id to eligibility descriptions using facets
                        sc_3083_ids = record.get("sc_3083_id", [])
                        # eligibility_descriptions = [
                        #     sc_3083_mapping.get(label_,None) for label_ in sc_3083_ids
                        # ]
                        # row["Eligibility Mapping"] = ", ".join(filter(None,eligibility_descriptions))
                        # Replace the raw IDs in the APplicable Age Group " with their mapped labels"

                        age_group_labels=[
                            sc_3083_mapping.get(label_,None) for label_ in sc_3083_ids
                        ]
                        row["sc_3083_id"]=",".join(filter(None,age_group_labels))

                        # Append the row if it contains valid data
                        if any(row.values()):
                            extracted_data.append(row)
                        
                    # Convert to DataFrame
                    df = pd.DataFrame(extracted_data)

                    # Rename the columns using the mapping
                    df.rename(columns=column_mapping, inplace=True)
                    print(df)

                    # Append the new data to the existing DataFrame
                    all_data = pd.concat([all_data, df], ignore_index=True)

                    offset +=limit
                    print(f"Fetched {len(records)} records, total so far: {len(all_data)}")
                else:
                    print("The key 'data' was not found in the response.")
            else:
                print(f"Failed to fetch data. Status Code: {response.status_code}")
                print(f"Response: {response.text}")
            print(f"Total number of records fetched: {len(all_data)}")
except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")

    # Write the final DataFrame to an Excel file
output_file = "output_filtered_data.xlsx"
all_data.to_excel(output_file, index=False)
print(f"All data exported to '{output_file}'.")