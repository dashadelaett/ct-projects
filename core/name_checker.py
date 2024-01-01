import csv
import pandas as pd

class DrugNamesChecker:
    def __init__(self, csv_file):
        self.russian_names = set()
        self.english_names = set()
        self.latin_names = set()
        self.load_from_csv(csv_file)

    def load_from_csv(self, csv_file):
        try:
            with open(csv_file, 'r', encoding='utf-8') as file:
                csv_reader = csv.reader(file, delimiter=';')
                for row in csv_reader:
                    if len(row) >= 3:
                        russian_name, english_name, latin_name = map(str, row[:3])
                        self.russian_names.add(russian_name)
                        self.english_names.add(english_name)
                        self.latin_names.add(latin_name)
                    else:
                        print(f"Skipping row: {row} - not enough columns")
        except FileNotFoundError:
            print(f"File '{csv_file}' not found.")
        except Exception as e:
            print(f"An error occurred while reading the CSV file: {str(e)}")

    def get_russian_names(self):
        return self.russian_names

    def get_english_names(self):
        return self.english_names

    def get_latin_names(self):
        return self.latin_names

    def process_excel_file(self, excel_file, subject_screen_column_number=2, cmingrd_column_number=17, sheet_name="Общие формы"):
        try:
            # Load the Excel file
            excel_data = pd.read_excel(excel_file, sheet_name=sheet_name, engine='openpyxl')

            # Create an empty DataFrame to store incorrect cmingrd values
            incorrect_cmingrd_df = pd.DataFrame(columns=['subject_screen', 'incorrect_cmingrd'])

            # Extract the specified columns
            subject_screen_column = excel_data.iloc[:, subject_screen_column_number]
            cmingrd_column = excel_data.iloc[:, cmingrd_column_number]

            # Iterate through the rows and check if cmingrd values are not in any of the sets
            for i, cmingrd_value in enumerate(cmingrd_column[2:], start=2):
                cmingrd_value = str(cmingrd_value).lower()
                if cmingrd_value not in self.russian_names and cmingrd_value not in self.english_names and cmingrd_value not in self.latin_names:
                    incorrect_cmingrd_df = pd.concat([incorrect_cmingrd_df, pd.DataFrame({
                        'subject_screen': [subject_screen_column[i]],
                        'incorrect_cmingrd': [cmingrd_value]
                    })], ignore_index=True)

            # Print the DataFrame with incorrect cmingrd values
            print("DataFrame with Incorrect cmingrd Values:")
            print(incorrect_cmingrd_df)

            # You can return or further process the incorrect_cmingrd_df DataFrame as needed
            return incorrect_cmingrd_df

        except FileNotFoundError:
            print(f"File '{excel_file}' not found.")
        except Exception as e:
            print(f"An error occurred while processing the Excel file: {str(e)}")

