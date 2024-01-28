import pandas as pd
from datetime import timedelta

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

    def check_sample_collection_dates(self):
        for visit_num in [1, 2]:
            hosp_date_col = f'V{visit_num}_01_SVSTDTC'
            sample_cols = [f'V{visit_num}_{"09" if visit_num == 1 else "08"}_{str(i).zfill(2)}_PCDTC' for i in range(1, 7)]

            for index, row in self.df.iterrows():
                hosp_date = pd.to_datetime(row[hosp_date_col]).date()
                for sample_col in sample_cols:
                    if pd.isnull(row[sample_col]):
                        continue  # Skip if the field is NaN
                    sample_date = pd.to_datetime(row[sample_col]).date()
                    if sample_date - hosp_date != timedelta(days=1):
                        self.log_error(index, sample_col)

    def check_first_sample_collection(self, hospitalization_num, section_num, section2_num):
        drug_admin_col = f'V{hospitalization_num}_0{section_num}_EXDTC'
        first_sample_col = f'V{hospitalization_num}_0{section2_num}_01_PCDTC'

        for index, row in self.df.iterrows():
            if pd.isnull(row[first_sample_col]) or pd.isnull(row[drug_admin_col]):
                continue  # Skip if either field is NaN
            first_sample_time = pd.to_datetime(row[first_sample_col])
            drug_admin_time = pd.to_datetime(row[drug_admin_col])

            if not (first_sample_time.date() == drug_admin_time.date() and first_sample_time < drug_admin_time):
                self.log_error(index, first_sample_col)

    def check_sample_intervals(self, hospitalization_num, section_num, section2_num):
        drug_admin_col = f'V{hospitalization_num}_0{section_num}_EXDTC'
        sample_intervals = {
            f'V{hospitalization_num}_0{section2_num}_02_PCDTC': (timedelta(hours=3), timedelta(minutes=2)),
            f'V{hospitalization_num}_0{section2_num}_03_PCDTC': (timedelta(hours=6), timedelta(minutes=2)),
            f'V{hospitalization_num}_0{section2_num}_04_PCDTC': (timedelta(hours=8), timedelta(minutes=2)),
            f'V{hospitalization_num}_0{section2_num}_05_PCDTC': (timedelta(hours=9), timedelta(minutes=2)),
            f'V{hospitalization_num}_0{section2_num}_06_PCDTC': (timedelta(hours=10), timedelta(minutes=2)),
            f'V{hospitalization_num}_0{section2_num}_07_PCDTC': (timedelta(hours=10, minutes=30), timedelta(minutes=2)),
            f'V{hospitalization_num}_0{section2_num}_08_PCDTC': (timedelta(hours=11), timedelta(minutes=2)),
            f'V{hospitalization_num}_0{section2_num}_09_PCDTC': (timedelta(hours=11, minutes=30), timedelta(minutes=2)),
            f'V{hospitalization_num}_0{section2_num}_10_PCDTC': (timedelta(hours=12), timedelta(minutes=2)),
            f'V{hospitalization_num}_0{section2_num}_11_PCDTC': (timedelta(hours=13), timedelta(minutes=2)),
            f'V{hospitalization_num}_0{section2_num}_12_PCDTC': (timedelta(hours=14), timedelta(minutes=2)),
            f'V{hospitalization_num}_0{section2_num}_13_PCDTC': (timedelta(hours=16), timedelta(minutes=2)),
            f'V{hospitalization_num}_0{section2_num}_14_PCDTC': (timedelta(hours=24), timedelta(minutes=5)),
            f'V{hospitalization_num}_0{section2_num}_15_PCDTC': (timedelta(hours=36), timedelta(minutes=10)),
            f'V{hospitalization_num}_0{section2_num}_16_PCDTC': (timedelta(hours=48), timedelta(minutes=10)),
            f'V{hospitalization_num}_0{section2_num}_17_PCDTC': (timedelta(hours=60), timedelta(minutes=10)),
            f'V{hospitalization_num}_0{section2_num}_18_PCDTC': (timedelta(hours=72), timedelta(minutes=10)),
        }

        for index, row in self.df.iterrows():
            drug_admin_time = pd.to_datetime(row[drug_admin_col])
            for sample_col, (expected_interval, deviation) in sample_intervals.items():
                if pd.isnull(row[sample_col]):
                    continue  # Skip if the field is NaN
                sample_time = pd.to_datetime(row[sample_col])
                lower_bound = drug_admin_time + expected_interval - deviation
                upper_bound = drug_admin_time + expected_interval + deviation
                if not (lower_bound <= sample_time <= upper_bound):
                    self.log_error(index, sample_col)

    def perform_checks(self):
        self.check_dates(1)
        self.check_dates(2)
        self.check_drug_intolerance_assessment_dates()
        self.check_screening_procedure_dates()
        self.check_sample_collection_dates()
        self.check_sample_intervals(hospitalization_num=1, section_num=8, section2_num=9)
        self.check_sample_intervals(hospitalization_num=2, section_num=7, section2_num=8)
        self.check_first_sample_collection(hospitalization_num=1, section_num=8, section2_num=9)
        self.check_first_sample_collection(hospitalization_num=2, section_num=7, section2_num=8)

        if self.errors:
            error_df = self.filter_errors()
            error_df.to_excel('error_report.xlsx', index=False)
        else:
            print("No errors found.")
