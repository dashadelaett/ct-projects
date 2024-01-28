import pandas as pd

class DatesTimeChecker:
    def __init__(self, file_name):
        self.file_name = file_name
        self.df = pd.read_excel(file_name, sheet_name='Визиты')
        self.df.drop([0], inplace=True)
        self.df.reset_index(drop=True)
        self.errors = {}

    def log_error(self, index, column):
        subj_id = self.df.at[index, 'SUBJ_ID']
        if subj_id not in self.errors:
            self.errors[subj_id] = set()
        self.errors[subj_id].add(column)

    def filter_errors(self):
        error_rows = []
        additional_columns = ['SITE_ID', 'SUBJ_SCREEN', 'SUBJECT_STATUS']
        for subj_id, columns in self.errors.items():
            error_row = self.df[self.df['SUBJ_ID'] == subj_id][['SUBJ_ID'] + additional_columns + list(columns)].iloc[0]
            error_rows.append(error_row)

        error_df = pd.DataFrame(error_rows)
        return error_df

    def check_dates(self, hospitalization_num):
        date_field = f'V{hospitalization_num}_01_SVSTDTC'
        fields_to_check = [f'V{hospitalization_num}_03_MBDAT', f'V{hospitalization_num}_04_LBDAT', 
                           f'V{hospitalization_num}_05_LBDAT', f'V{hospitalization_num}_06_LBDAT']

        for index, row in self.df.iterrows():
            hospitalization_date = pd.to_datetime(row[date_field]).date()  # Convert to date
            for field in fields_to_check:
                if pd.isnull(row[field]):
                    continue  # Skip if the field is NaN
                field_date = pd.to_datetime(row[field]).date()  # Convert to date
                if field_date != hospitalization_date:
                    self.log_error(index, field)

    def check_drug_intolerance_assessment_dates(self):
        visits = {
            1: ('V1_17_LIKERT_SCALE_5POINT_QSDAT', 'V1_08_EXDTC'),
            2: ('V2_16_LIKERT_SCALE_5POINT_QSDAT', 'V2_07_EXDTC')
        }
        for visit, (assessment_date_col, drug_date_col) in visits.items():
            for index, row in self.df.iterrows():
                assessment_date = pd.to_datetime(row[assessment_date_col]).date()
                drug_date = pd.to_datetime(row[drug_date_col]).date()
                if assessment_date <= drug_date:
                    self.log_error(index, assessment_date_col)

    def check_screening_procedure_dates(self):
        consent_date_col = 'V0_01_RFICDTC'
        procedure_cols = ['V0_02_MBDTC', 'V0_05_VSDTC', 'V0_06_PEDTC', 'V0_07_EGDTC', 'V0_08_LBDTC',
                          'V0_09_LBDTC', 'V0_10_LBDTC', 'V0_11_ISDTC', 'V0_12_LBDTC', 'V0_13_LBDTC',
                          'V0_14_LBDTC', 'V0_15_PDDTC']

        for index, row in self.df.iterrows():
            consent_date = pd.to_datetime(row[consent_date_col]).date()
            consent_time = pd.to_datetime(row[consent_date_col])
            for col in procedure_cols:
                if pd.isnull(row[col]):
                    continue  # Skip if the field is NaN
                procedure_date = pd.to_datetime(row[col]).date()
                procedure_time = pd.to_datetime(row[col])
                if procedure_date != consent_date and procedure_time >= consent_time:
                    self.log_error(index, col)

    def perform_checks(self):
        self.check_dates(1)
        self.check_dates(2)
        self.check_drug_intolerance_assessment_dates()
        self.check_screening_procedure_dates()

        if self.errors:
            error_df = self.filter_errors()
            error_df.to_excel('error_report.xlsx', index=False)
        else:
            print("No errors found.")
