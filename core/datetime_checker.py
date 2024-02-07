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

    def check_intervals(self, sample_intervals, reference_col):
        for index, row in self.df.iterrows():
            drug_admin_time = pd.to_datetime(row[reference_col])
            for sample_col, (expected_interval, deviation) in sample_intervals.items():
                if pd.isnull(row[sample_col]):
                    continue  # Skip if the field is NaN
                sample_time = pd.to_datetime(row[sample_col])
                lower_bound = drug_admin_time + expected_interval - deviation
                upper_bound = drug_admin_time + expected_interval + deviation
                if not (lower_bound <= sample_time <= upper_bound):
                    self.log_error(index, sample_col)

    def check_1_dates(self, hospitalization_num):
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

    def check_3_drug_intolerance_assessment_dates(self):
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

    def check_4_5_screening_procedure_dates_to_consent(self):
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

    def check_6_sample_collection_dates(self):
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

    def check_7_first_sample_collection(self, hospitalization_num, section_num, section2_num):
        drug_admin_col = f'V{hospitalization_num}_0{section_num}_EXDTC'
        first_sample_col = f'V{hospitalization_num}_0{section2_num}_01_PCDTC'

        for index, row in self.df.iterrows():
            if pd.isnull(row[first_sample_col]) or pd.isnull(row[drug_admin_col]):
                continue  # Skip if either field is NaN
            first_sample_time = pd.to_datetime(row[first_sample_col])
            drug_admin_time = pd.to_datetime(row[drug_admin_col])

            if not (first_sample_time.date() == drug_admin_time.date() and first_sample_time < drug_admin_time):
                self.log_error(index, first_sample_col)

    def check_7_sample_intervals(self, hospitalization_num, section_num, section2_num):
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

        self.check_intervals(sample_intervals=sample_intervals, reference_col=drug_admin_col)

    def check_8_screening_procedure_dates_time(self):
        groups = {
            'covid': ['V0_02_MBDTC'],
            'jvp': ['V0_05_VSDTC', 'V0_06_PEDTC'],
            'ecg': ['V0_07_EGDTC'],
            'blood_work': ['V0_08_LBDTC', 'V0_09_LBDTC', 'V0_11_ISDTC', 'V0_15_PDDTC'],
            'pee_pee': ['V0_10_LBDTC', 'V0_12_LBDTC', 'V0_13_LBDTC'],
            'alcohol': ['V0_14_LBDTC']
        }

        for index, row in self.df.iterrows():
            sample_dates = {}
            for group, cols in groups.items():
                group_dates = set()
                for col in cols:
                    if pd.isnull(row[col]):
                        continue  # Skip if the field is NaN
                    date_time = pd.to_datetime(row[col])  # Consider only the date part
                    group_dates.add(date_time)

                for date in group_dates:
                    if date in sample_dates and sample_dates[date] != group:
                        print(f"Row {index}: Overlap in date {date} between {group} and {sample_dates[date]}")
                        for col in cols:
                            if pd.to_datetime(row[col]) == date:
                                self.log_error(index, col)
                    sample_dates[date] = group



    def check_9_blood_pee_72h(self, sample_intervals, reference_col):
        self.check_intervals(sample_intervals=sample_intervals, reference_col=reference_col)

    def check_unique_analyzis_values(self, blood_analysis, pee_analysis, fo_jvp):
        for index, row in self.df.iterrows():
            blood_values = set()
            pee_values = set()
            fo_jvp_values = set()

            # Collect values for blood analysis
            for col in blood_analysis:
                if pd.notnull(row[col]):
                    blood_values.add(pd.to_datetime(row[col]))

            # Collect values for pee analysis
            for col in pee_analysis:
                if pd.notnull(row[col]):
                    pee_values.add(pd.to_datetime(row[col]))

            # Collect values from fo_jvp columns
            for col in fo_jvp:
                if pd.notnull(row[col]):
                    fo_jvp_values.add(pd.to_datetime(row[col]))

            # Check if blood and pee analysis values are different from fo_jvp values
            if blood_values.intersection(fo_jvp_values) or pee_values.intersection(fo_jvp_values):
                common_values = blood_values.intersection(fo_jvp_values).union(pee_values.intersection(fo_jvp_values))
                for common_value in common_values:
                    print(f"Row {index}: Value {common_value} found in both analysis groups and {fo_jvp}")
                    for col in blood_analysis + pee_analysis + fo_jvp:
                        if pd.to_datetime(row[col]) == common_value:
                            self.log_error(index, col)
        
    def check_immunogenicity_samples(self):
        visits = {
            1: ('V1_08_EXDTC', 'V1_11_01_ISDTC', 'V1_11_02_ISDTC'),
            2: ('V2_07_EXDTC', 'V2_10_01_ISDTC', 'V2_10_02_ISDTC')
        }

        for visit, (drug_admin_col, sample1_col, sample2_col) in visits.items():
            for index, row in self.df.iterrows():
                drug_admin_time = pd.to_datetime(row[drug_admin_col])
                sample1_time = pd.to_datetime(row[sample1_col])
                sample2_time = pd.to_datetime(row[sample2_col])

                # Check Sample No. 1
                if not (drug_admin_time - timedelta(hours=2) <= sample1_time <= drug_admin_time):
                    print(f"Row {index}: Visit {visit} Sample 1 not within 2 hours before drug administration")
                    self.log_error(index, sample1_col)

                # Check Sample No. 2
                if not (drug_admin_time + timedelta(hours=71) <= sample2_time <= drug_admin_time + timedelta(hours=73)):
                    print(f"Row {index}: Visit {visit} Sample 2 not within 72 hours (±1 hour) after drug administration")
                    self.log_error(index, sample2_col)

    def check_pharmacodynamic_samples(self, hospitalization_num, section_num, section2_num, sample_section_num):
        drug_admin_col = f'V{hospitalization_num}_0{section_num}_EXDTC'
        meal_times = {
        'breakfast': f'V{hospitalization_num}_{section2_num}_04_MLSTDTC',
        'lunch': f'V{hospitalization_num}_{section2_num}_05_MLSTDTC',
        'dinner': f'V{hospitalization_num}_{section2_num}_06_MLSTDTC'
        }
        sample_intervals = [
            (15, 2, 'before_breakfast'), (30, 2), (60, 7), (90, 7), (120, 7), (150, 7), (240, 7), 
            (300, 7, 'before_lunch'), (360, 7), (420, 7), (480, 7), (660, 7, 'before_dinner'), 
            (720, 7), (780, 7), (840, 7), (1440, 7)
        ]

        for index, row in self.df.iterrows():
            if pd.isnull(row[drug_admin_col]):
                continue  # Skip if drug administration time is NaN

            drug_admin_time = pd.to_datetime(row[drug_admin_col])
            for i, (interval, deviation, *meal_check) in enumerate(sample_intervals, start=2):
                sample_col = f'V{hospitalization_num}_{sample_section_num}_{str(i).zfill(2)}_PDDTC'
                if pd.isnull(row[sample_col]):
                    continue  # Skip if the field is NaN

                sample_time = pd.to_datetime(row[sample_col])
                expected_time = drug_admin_time + timedelta(minutes=interval)
                lower_bound = expected_time - timedelta(minutes=deviation)
                upper_bound = expected_time + timedelta(minutes=deviation)

                # Check if the sample was collected within the allowed time window
                if not (lower_bound <= sample_time <= upper_bound):
                    self.log_error(index, sample_col)

                # Additional checks for meal times if applicable
                if meal_check:
                    meal_col = meal_times[meal_check[0].split('_')[1]]
                    if pd.isnull(row[meal_col]):
                        continue  # Skip if meal time is NaN
                    
                    meal_time = pd.to_datetime(row[meal_col])
                    if (meal_check[0] == 'before_breakfast' and sample_time >= meal_time) or \
                    (meal_check[0] == 'before_lunch' and sample_time >= meal_time) or \
                    (meal_check[0] == 'before_dinner' and sample_time >= meal_time):
                        self.log_error(index, sample_col)
    
    def check_screening_pharmacodynamics_sample(self):
        for index, row in self.df.iterrows():
            sample_time_col = 'V0_15_PDDTC'
            drug_admin_time_col = 'V1_08_EXDTC'

            # Ensure both sample collection and drug administration times are not null
            if pd.isnull(row[sample_time_col]) or pd.isnull(row[drug_admin_time_col]):
                continue

            sample_time = pd.to_datetime(row[sample_time_col])
            drug_admin_time = pd.to_datetime(row[drug_admin_time_col])

            # Check if sample collection time is before drug administration time
            if sample_time.date() != drug_admin_time.date() and sample_time < drug_admin_time:
                pass  # The condition is met, no action needed
            else:
                self.log_error(index, sample_time_col)

    def check_randomization_timing(self):
        for index, row in self.df.iterrows():
            randomization_time_col = 'V1_07_DSSTDTC'
            drug_admin_time_col = 'V1_08_EXDTC'
            hospitalization_time_col = 'V1_01_SVSTDTC'

            # Ensure the randomization time, drug administration time, and hospitalization time are not null
            if pd.isnull(row[randomization_time_col]) or pd.isnull(row[drug_admin_time_col]) or pd.isnull(row[hospitalization_time_col]):
                continue

            randomization_time = pd.to_datetime(row[randomization_time_col])
            drug_admin_time = pd.to_datetime(row[drug_admin_time_col])
            hospitalization_time = pd.to_datetime(row[hospitalization_time_col])

            # Check if randomization is on the same day as hospitalization or drug administration
            # and before the drug administration time
            if ((randomization_time.date() == drug_admin_time.date() or randomization_time.date() == hospitalization_time.date()) 
                and randomization_time < drug_admin_time):
                pass  # The condition is met, no action needed
            else:
                self.log_error(index, randomization_time_col)

    def check_drug_administration_timing(self):
        for visit_num in [1, 2]:
            drug_admin_col = f'V{visit_num}_{"08" if visit_num == 1 else "07"}_EXDTC'
            hospitalization_col = f'V{visit_num}_01_SVSTDTC'

            for index, row in self.df.iterrows():
                # Ensure both drug administration and hospitalization dates are not null
                if pd.isnull(row[drug_admin_col]) or pd.isnull(row[hospitalization_col]):
                    continue

                drug_admin_date = pd.to_datetime(row[drug_admin_col]).date()
                hospitalization_date = pd.to_datetime(row[hospitalization_col]).date()

                # Check if the drug administration date is exactly one day after the hospitalization date
                if (drug_admin_date - hospitalization_date).days != 1:
                    self.log_error(index, drug_admin_col)

    def check_catheter_placement_timing(self):
        for visit_num in [1, 2]:
            catheter_col = f'V{visit_num}_{"09" if visit_num == 1 else "08"}_PRCATHDTC'
            first_sample_col = f'V{visit_num}_{"09" if visit_num == 1 else "08"}_01_PCDTC'

            for index, row in self.df.iterrows():
                # Ensure both catheter placement and first blood sample times are not null
                if pd.isnull(row[catheter_col]) or pd.isnull(row[first_sample_col]):
                    continue

                catheter_time = pd.to_datetime(row[catheter_col])
                first_sample_time = pd.to_datetime(row[first_sample_col])

                # Calculate the time difference in minutes
                time_difference = (first_sample_time - catheter_time).total_seconds() / 60

                # Check if the catheter placement is within 5 to 10 minutes before the first blood sample collection
                if not 5 <= time_difference <= 10:
                    self.log_error(index, catheter_col)

    def check_catheter_removal_timing(self):
        for visit_num in [1, 2]:
            catheter_in_col = f'V{visit_num}_{"08" if visit_num == 1 else "07"}_EXDTC'
            catheter_out_col = f'V{visit_num}_{"09" if visit_num == 1 else "08"}_PRCATHOUTDTC'

            for index, row in self.df.iterrows():
                # Ensure both catheter placement and removal times are not null
                if pd.isnull(row[catheter_in_col]) or pd.isnull(row[catheter_out_col]):
                    continue

                catheter_in_time = pd.to_datetime(row[catheter_in_col])
                catheter_out_time = pd.to_datetime(row[catheter_out_col])

                # Calculate the time difference in hours
                time_difference_hours = (catheter_out_time - catheter_in_time).total_seconds() / 3600
                print(catheter_in_time, catheter_out_time, time_difference_hours)
                # Check if the catheter is removed exactly 12 hours after placement
                if time_difference_hours != 12:
                    self.log_error(index, catheter_out_col)

    def check_vitals_and_physical_exam_timing(self):
        for visit_num in [1, 2]:
            drug_admin_col = f'V{visit_num}_{"08" if visit_num == 1 else "07"}_EXDTC'
            urine_analysis_col = f'V{visit_num}_{"16" if visit_num == 1 else "15"}_LBDTC'
            vitals_prefix = f'V{visit_num}_{"12" if visit_num == 1 else "11"}_'
            physical_exam_prefix = f'V{visit_num}_{"13" if visit_num == 1 else "12"}_'

            # Define the expected time windows for vital signs and physical exams
            time_windows = [
                (2, -120, -5),     # 2 hours before, up to 5 minutes before
                (3, -40, 40),      # 3 hours after, ± 40 minutes
                (6, -40, 40),      # 6 hours after, ± 40 minutes
                (9, -40, 40),      # 9 hours after, ± 40 minutes
                (12, -40, 40),     # 12 hours after, ± 40 minutes
                (24, -120, 120),   # 24 hours after, ± 2 hours
                (48, -120, 120),   # 48 hours after, ± 2 hours
                (72, -120, 120),   # 72 hours after, ± 2 hours
            ]

            for index, row in self.df.iterrows():
                drug_admin_time = pd.to_datetime(row[drug_admin_col])
                urine_analysis_time = pd.to_datetime(row[urine_analysis_col]) if pd.notnull(row[urine_analysis_col]) else None

                for i, (hours_after, window_start, window_end) in enumerate(time_windows, start=1):
                    vitals_col = f'{vitals_prefix}{i}_VSDTC'
                    physical_exam_col = f'{physical_exam_prefix}{i}_PEDTC'
                    
                    # Skip if either vital signs or physical exam time is null
                    if pd.isnull(row[vitals_col]) or pd.isnull(row[physical_exam_col]):
                        continue

                    vitals_time = pd.to_datetime(row[vitals_col])
                    physical_exam_time = pd.to_datetime(row[physical_exam_col])

                    # Calculate the time difference from drug administration in minutes
                    vitals_time_diff = (vitals_time - drug_admin_time).total_seconds() / 60
                    physical_exam_time_diff = (physical_exam_time - drug_admin_time).total_seconds() / 60

                    # Check if times are within the window and not the same as urine analysis time
                    if not (window_start <= vitals_time_diff <= window_end) or (urine_analysis_time and vitals_time == urine_analysis_time):
                        self.log_error(index, vitals_col)
                    if not (window_start <= physical_exam_time_diff <= window_end) or (urine_analysis_time and physical_exam_time == urine_analysis_time):
                        self.log_error(index, physical_exam_col)

    def check_meal_and_fluid_intake_timing(self):
        for visit_num in [1, 2]:
            drug_admin_col = f'V{visit_num}_{"08" if visit_num == 1 else "07"}_EXDTC'
            meal_end_dinner_col = f'V{visit_num}_{"18" if visit_num == 1 else "17"}_01_MLENDTC'
            fluid_end_col = f'V{visit_num}_{"18" if visit_num == 1 else "17"}_02_MLENDTC'
            fluid_start_col = f'V{visit_num}_{"18" if visit_num == 1 else "17"}_03_MLSTDTC'
            breakfast_col = f'V{visit_num}_{"18" if visit_num == 1 else "17"}_04_MLSTDTC'
            lunch_col = f'V{visit_num}_{"18" if visit_num == 1 else "17"}_05_MLSTDTC'
            dinner_col = f'V{visit_num}_{"18" if visit_num == 1 else "17"}_06_MLSTDTC'

            for index, row in self.df.iterrows():
                drug_admin_time = pd.to_datetime(row[drug_admin_col])

                # Check dinner end time (at least 10 hours before drug administration)
                if not pd.isnull(row[meal_end_dinner_col]):
                    dinner_end_time = pd.to_datetime(row[meal_end_dinner_col])
                    if (drug_admin_time - dinner_end_time).total_seconds() < 36000:  # 10 hours in seconds
                        self.log_error(index, meal_end_dinner_col)

                # Check fluid end time (1 hour before drug administration)
                if not pd.isnull(row[fluid_end_col]):
                    fluid_end_time = pd.to_datetime(row[fluid_end_col])
                    if (drug_admin_time - fluid_end_time).total_seconds() < 3600:  # 1 hour in seconds
                        self.log_error(index, fluid_end_col)

                # Check fluid start time (at least 2 hours after drug administration)
                if not pd.isnull(row[fluid_start_col]):
                    fluid_start_time = pd.to_datetime(row[fluid_start_col])
                    if (fluid_start_time - drug_admin_time).total_seconds() < 7200:  # 2 hours in seconds
                        self.log_error(index, fluid_start_col)

                # Check breakfast time (30 minutes after drug administration)
                if not pd.isnull(row[breakfast_col]):
                    breakfast_time = pd.to_datetime(row[breakfast_col])
                    if (breakfast_time - drug_admin_time).total_seconds() < 1800:  # 30 minutes in seconds
                        self.log_error(index, breakfast_col)

                # Check lunch time (6 hours after drug administration)
                if not pd.isnull(row[lunch_col]):
                    lunch_time = pd.to_datetime(row[lunch_col])
                    if abs((lunch_time - drug_admin_time).total_seconds() - 21600) > 300:  # 6 hours ± 5 min in seconds
                        self.log_error(index, lunch_col)

                # Check dinner time (11 hours after drug administration)
                if not pd.isnull(row[dinner_col]):
                    dinner_time = pd.to_datetime(row[dinner_col])
                    if abs((dinner_time - drug_admin_time).total_seconds() - 39600) > 300:  # 11 hours ± 5 min in seconds
                        self.log_error(index, dinner_col)

    def check_discharge_timing(self):
        for visit_num in [1, 2]:
            discharge_col = f'V{visit_num}_{"19" if visit_num == 1 else "18"}_HOENDTC'
            procedures_cols = [
                f'V{visit_num}_{"12" if visit_num == 1 else "11"}_8_VSDTC',
                f'V{visit_num}_{"13" if visit_num == 1 else "12"}_8_PEDTC',
                f'V{visit_num}_14_LBDTC',
                f'V{visit_num}_15_LBDTC',
                f'V{visit_num}_16_LBDTC' if visit_num == 1 else f'V{visit_num}_15_LBDTC'
            ]

            for index, row in self.df.iterrows():
                # Ensure discharge time is not null
                if pd.isnull(row[discharge_col]):
                    continue

                discharge_time = pd.to_datetime(row[discharge_col])
                latest_procedure_time = None

                # Loop through the procedures to find the latest time
                for proc_col in procedures_cols:
                    if pd.notnull(row[proc_col]):
                        procedure_time = pd.to_datetime(row[proc_col])
                        if not latest_procedure_time or procedure_time > latest_procedure_time:
                            latest_procedure_time = procedure_time

                # Check if discharge is at least 72 hours after the latest procedure time
                if latest_procedure_time and (discharge_time - latest_procedure_time).total_seconds() < (72 * 3600):
                    self.log_error(index, discharge_col)



    def perform_checks(self):
        # checks 1 and 2
        self.check_1_dates(1)
        self.check_1_dates(2)
        # check 3
        self.check_3_drug_intolerance_assessment_dates()
        # checks 4 and 5
        self.check_4_5_screening_procedure_dates_to_consent()
        # check 6
        self.check_6_sample_collection_dates()
        # check 7
        self.check_7_first_sample_collection(hospitalization_num=1, section_num=8, section2_num=9)
        self.check_7_first_sample_collection(hospitalization_num=2, section_num=7, section2_num=8)

        self.check_7_sample_intervals(hospitalization_num=1, section_num=8, section2_num=9)
        self.check_7_sample_intervals(hospitalization_num=2, section_num=7, section2_num=8)
        # check 8
        self.check_8_screening_procedure_dates_time()
        

        ip_ps_v1 = f'V1_08_EXDTC'
        sample_intervals_v1 = {
            f'V1_14_LBDTC': (timedelta(hours=72), timedelta(minutes=120)),
            f'V1_15_LBDTC': (timedelta(hours=72), timedelta(minutes=120)),
            f'V1_16_LBDTC': (timedelta(hours=72), timedelta(minutes=120)),
        }

        ip_ps_v2 = f'V2_07_EXDTC'
        sample_intervals_v2 = {
            f'V2_13_LBDTC': (timedelta(hours=72), timedelta(minutes=120)),
            f'V2_14_LBDTC': (timedelta(hours=72), timedelta(minutes=120)),
            f'V2_15_LBDTC': (timedelta(hours=72), timedelta(minutes=120)),
        }
        # check 9
        self.check_9_blood_pee_72h(sample_intervals=sample_intervals_v1, reference_col=ip_ps_v1)
        self.check_9_blood_pee_72h(sample_intervals=sample_intervals_v2, reference_col=ip_ps_v2)

        self.check_unique_analyzis_values(blood_analysis=['V1_14_LBDTC', 'V1_15_LBDTC'], pee_analysis=['V1_16_LBDTC'], fo_jvp=['V1_13_8_PEDTC', 'V1_12_8_VSDTC'])
        self.check_unique_analyzis_values(blood_analysis=['V2_13_LBDTC', 'V2_14_LBDTC'], pee_analysis=['V2_15_LBDTC'], fo_jvp=['V2_12_8_PEDTC', 'V2_11_8_VSDTC'])

        # check 10
        self.check_immunogenicity_samples()
        #check 11
        # For Visit 1: sample_section_num=10
        self.check_pharmacodynamic_samples(hospitalization_num=1, section_num=8, section2_num=18, sample_section_num=10)

        # For Visit 2: sample_section_num=9
        self.check_pharmacodynamic_samples(hospitalization_num=2, section_num=7, section2_num=17, sample_section_num="09")

        #check 11 for Screening
        self.check_screening_pharmacodynamics_sample()
        #check 12
        self.check_randomization_timing()

        #check 13
        self.check_drug_administration_timing()

        #check 14
        self.check_catheter_placement_timing()

        #check 15
        self.check_catheter_removal_timing()

        #check 16
        self.check_vitals_and_physical_exam_timing()

        #check 17
        self.check_meal_and_fluid_intake_timing()

        #check 18
        self.check_discharge_timing()

        if self.errors:
            error_df = self.filter_errors()
            error_df.to_excel('error_report.xlsx', index=False)
        else:
            print("No errors found.")
