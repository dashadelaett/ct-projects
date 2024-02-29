from datetime import datetime, timedelta
import pandas as pd
class DatesTimeChecker:
    def __init__(self, file_name):
        self.file_name = file_name
        self.df = pd.read_excel(file_name, sheet_name='Визиты')
        self.df.drop([0], inplace=True)
        self.df.reset_index(drop=True)
        self.errors = {}
        self.checked_fields = set()

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
                # Add the field to checked_fields set irrespective of whether it has an error
                self.checked_fields.add(sample_col)

                if pd.isnull(row[sample_col]):
                    continue  # Skip if the field is NaN

                sample_time = pd.to_datetime(row[sample_col])
                lower_bound = drug_admin_time + expected_interval - deviation
                upper_bound = drug_admin_time + expected_interval + deviation
                
                if not (lower_bound <= sample_time <= upper_bound):
                    self.log_error(index, sample_col)

    def check_1_dates(self, hospitalization_num):
        date_field = f'V{hospitalization_num}_01_SVSTDTC'
        fields_to_check = [ 
            f'V{hospitalization_num}_03_LBDAT', 
            f'V{hospitalization_num}_04_LBDAT', 
            f'V{hospitalization_num}_05_LBDAT'
        ]

        for index, row in self.df.iterrows():
            hospitalization_date = pd.to_datetime(row[date_field]).date()  # Convert to date
            for field in fields_to_check:
                # Add the field to checked_fields set irrespective of whether it has an error
                self.checked_fields.add(field)

                if pd.isnull(row[field]):
                    continue  # Skip if the field is NaN
                field_date = pd.to_datetime(row[field]).date()  # Convert to date
                if field_date != hospitalization_date:
                    self.log_error(index, field)

    def check_4_5_screening_procedure_dates_to_consent(self):
            consent_date_col = 'V0_01_RFICDTC'
            procedure_cols = [
                'V0_02_MBDTC', 'V0_05_VSDTC', 'V0_06_PEDTC', 'V0_07_EGDTC', 'V0_08_LBDTC',
                'V0_09_LBDTC', 'V0_10_LBDTC', 'V0_11_ISDTC', 'V0_12_LBDTC', 'V0_13_LBDTC',
                'V0_14_LBDTC'
            ]

            for index, row in self.df.iterrows():
                consent_date = pd.to_datetime(row[consent_date_col]).date()
                consent_time = pd.to_datetime(row[consent_date_col])
                
                for col in procedure_cols:
                    # Add the field to checked_fields set irrespective of whether it has an error
                    self.checked_fields.add(col)

                    if pd.isnull(row[col]):
                        continue  # Skip if the field is NaN
                        
                    procedure_date = pd.to_datetime(row[col]).date()
                    procedure_time = pd.to_datetime(row[col])
                    if procedure_date != consent_date and procedure_time <= consent_time:
                        self.log_error(index, col)


    def check_8_screening_procedure_dates_time(self):
            groups = {
                'covid': ['V0_02_MBDTC'],
                'jvp': ['V0_05_VSDTC', 'V0_06_PEDTC'],
                'ecg': ['V0_07_EGDTC'],
                'blood_work': ['V0_08_LBDTC', 'V0_09_LBDTC', 'V0_11_ISDTC'],
                'pee_pee': ['V0_10_LBDTC', 'V0_12_LBDTC', 'V0_13_LBDTC'],
                'alcohol': ['V0_14_LBDTC']
            }

            for index, row in self.df.iterrows():
                sample_dates = {}
                for group, cols in groups.items():
                    group_dates = set()
                    for col in cols:
                        # Add the field to checked_fields set before checking for errors
                        self.checked_fields.add(col)

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

    def check_7_first_sample_collection(self, hospitalization_num, section_num, section2_num):
            drug_admin_col = f'V{hospitalization_num}_0{section_num}_EXDTC'
            first_sample_col = f'V{hospitalization_num}_0{section2_num}_D1_PCDAT'

            for index, row in self.df.iterrows():
                # Add the fields to checked_fields set before proceeding with checks
                self.checked_fields.add(drug_admin_col)
                self.checked_fields.add(first_sample_col)

                if pd.isnull(row[first_sample_col]) or pd.isnull(row[drug_admin_col]):
                    continue  # Skip if either field is NaN
                first_sample_time = pd.to_datetime(row[first_sample_col])
                drug_admin_time = pd.to_datetime(row[drug_admin_col])

                if not (first_sample_time.date() == drug_admin_time.date() and first_sample_time < drug_admin_time):
                    self.log_error(index, first_sample_col)

    def check_7_sample_intervals(self, hospitalization_num, section_num, section2_num):
        drug_admin_col = f'V{hospitalization_num}_0{section_num}_EXDTC'
        
        # Словарь для определения даты отбора пробы для каждого образца
        sample_date_cols = {
            i: f'V{hospitalization_num}_0{section2_num}_D1_PCDAT' for i in range(2, 14)
        }
        sample_date_cols.update({
            14: f'V{hospitalization_num}_0{section2_num}_D2_PCDAT',
            15: f'V{hospitalization_num}_0{section2_num}_D3_PCDAT',
            16: f'V{hospitalization_num}_0{section2_num}_D4_PCDAT',
        })

        # Интервалы времени после приема препарата для каждого образца
        sample_intervals = {
            2: timedelta(minutes=30),
            3: timedelta(hours=1),
            4: timedelta(hours=1, minutes=20),
            5: timedelta(hours=1, minutes=40),
            6: timedelta(hours=2),
            7: timedelta(hours=2, minutes=20),
            8: timedelta(hours=2, minutes=40),
            9: timedelta(hours=3),
            10: timedelta(hours=4),
            11: timedelta(hours=5),
            12: timedelta(hours=6),
            13: timedelta(hours=8),
            14: timedelta(hours=10),
            15: timedelta(hours=12),
            16: timedelta(hours=24),
        }

        for sample_num, interval in sample_intervals.items():
            sample_time_col = f'V{hospitalization_num}_0{section2_num}_{str(sample_num).zfill(2)}_PCTIM'
            sample_date_col = sample_date_cols[sample_num]

            for index, row in self.df.iterrows():
                if pd.isnull(row[drug_admin_col]) or pd.isnull(row[sample_time_col]) or pd.isnull(row[sample_date_col]):
                    continue

                drug_admin_datetime = pd.to_datetime(row[drug_admin_col])
                sample_datetime_str = f"{row[sample_date_col]} {row[sample_time_col]}"
                sample_datetime = datetime.strptime(sample_datetime_str, "%Y-%m-%d %H:%M")

                self.checked_fields.add(sample_date_col)  # Добавление поля даты в проверенные
                self.checked_fields.add(sample_time_col)  # Добавление поля времени в проверенные

                # Проверка, что образец взят в заданном интервале времени после приема препарата
                expected_sample_datetime = drug_admin_datetime + interval
                if not (expected_sample_datetime - timedelta(minutes=0) <= sample_datetime <= expected_sample_datetime + timedelta(minutes=0)):
                    self.log_error(index, sample_time_col)


    def check_9_blood_pee_72h(self, sample_intervals, reference_col):
            # Explicitly add each sample interval field to checked_fields before checking them
            for sample_col in sample_intervals.keys():
                self.checked_fields.add(sample_col)
            
            self.check_intervals(sample_intervals=sample_intervals, reference_col=reference_col)


    def check_unique_analyzis_values(self, blood_analysis, pee_analysis, fo_jvp):
        for index, row in self.df.iterrows():
            blood_values = set()
            pee_values = set()
            fo_jvp_values = set()

            # Collect values for blood analysis and add to checked_fields
            for col in blood_analysis:
                self.checked_fields.add(col)
                if pd.notnull(row[col]):
                    blood_values.add(pd.to_datetime(row[col]))

            # Collect values for pee analysis and add to checked_fields
            for col in pee_analysis:
                self.checked_fields.add(col)
                if pd.notnull(row[col]):
                    pee_values.add(pd.to_datetime(row[col]))

            # Collect values from fo_jvp columns and add to checked_fields
            for col in fo_jvp:
                self.checked_fields.add(col)
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

    def check_randomization_timing(self):
            randomization_time_col = 'V1_06_DSSTDTC'
            drug_admin_time_col = 'V1_07_EXDTC'
            hospitalization_time_col = 'V1_01_SVSTDTC'

            # Add the fields to checked_fields set before proceeding with checks
            self.checked_fields.add(randomization_time_col)
            self.checked_fields.add(drug_admin_time_col)
            self.checked_fields.add(hospitalization_time_col)

            for index, row in self.df.iterrows():
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
            for visit_num in [1, 4]:
                drug_admin_col = f'V{visit_num}_{"07" if visit_num == 1 else "06"}_EXDTC'
                hospitalization_col = f'V{visit_num}_01_SVSTDTC'

                # Add the fields to checked_fields set before proceeding with checks
                self.checked_fields.add(drug_admin_col)
                self.checked_fields.add(hospitalization_col)

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
            for visit_num in [1, 4]:
                catheter_col = f'V{visit_num}_{"08" if visit_num == 1 else "07"}_PRCATHDTC'
                first_sample_col = f'V{visit_num}_{"08" if visit_num == 1 else "07"}_01_PCTIM'

                # Add the fields to checked_fields set before proceeding with checks
                self.checked_fields.add(catheter_col)
                self.checked_fields.add(first_sample_col)

                for index, row in self.df.iterrows():
                    # Ensure both catheter placement and first blood sample times are not null
                    if pd.isnull(row[catheter_col]) or pd.isnull(row[first_sample_col]):
                        continue

                    catheter_time = pd.to_datetime(row[catheter_col]).time()
                    first_sample_time = pd.to_datetime(row[first_sample_col]).time()
                   
                    catheter_time_td = timedelta(hours=catheter_time.hour, minutes=catheter_time.minute, seconds=catheter_time.second)
                    first_sample_time_td = timedelta(hours=first_sample_time.hour, minutes=first_sample_time.minute, seconds=first_sample_time.second)
                    # Calculate the time difference in minutes
                    time_difference = (first_sample_time_td - catheter_time_td).total_seconds() / 60
                    
                    # Check if the catheter placement is within 5 to 10 minutes before the first blood sample collection
                    if not 5 <= time_difference <= 10:
                        self.log_error(index, catheter_col)


    def check_catheter_removal_timing(self):
            for visit_num in [1, 4]:
                catheter_in_col = f'V{visit_num}_{"07" if visit_num == 1 else "06"}_EXDTC'
                catheter_out_col = f'V{visit_num}_{"08" if visit_num == 1 else "07"}_PRCATHOUTDTC'

                # Add the fields to checked_fields set before proceeding with checks
                self.checked_fields.add(catheter_in_col)
                self.checked_fields.add(catheter_out_col)

                for index, row in self.df.iterrows():
                    # Ensure both catheter placement and removal times are not null
                    if pd.isnull(row[catheter_in_col]) or pd.isnull(row[catheter_out_col]):
                        continue

                    catheter_in_time = pd.to_datetime(row[catheter_in_col])
                    catheter_out_time = pd.to_datetime(row[catheter_out_col])

                    # Calculate the time difference in hours
                    time_difference_hours = (catheter_out_time - catheter_in_time).total_seconds() / 3600
                    
                    # Check if the catheter is removed exactly 12 hours after placement
                    if time_difference_hours != 12:
                        self.log_error(index, catheter_out_col)

    def check_vitals_and_physical_timing(self):
            visit_params = [
                {
                    'visit_num': 1,
                    'drug_admin_col': 'V1_07_EXDTC',
                    'urine_analysis_col': 'V3_07_LBDTC',
                    'vitals_prefix': 'V1_09_',
                    'physical_prefix': 'V1_10_',
                },
                {
                    'visit_num': 4,
                    'drug_admin_col': 'V4_06_EXDTC',
                    'urine_analysis_col': 'V6_07_LBDTC',
                    'vitals_prefix': 'V4_08_',
                    'physical_prefix': 'V4_09_',
                }
            ]

            # Define time offsets for each event in minutes
            time_offsets = {
                '1': (-120, 1),  # 2 hours before
                '2': (180, 40),   # 3 hours after ± 40 minutes
                '3': (360, 40),   # 6 hours after ± 40 minutes
                '4': (540, 40),   # 9 hours after ± 40 minutes
                '5': (720, 40),   # 12 hours after ± 40 minutes
                '6': (1440, 120), # 24 hours after ± 2 hours
                
            }
            
            for params in visit_params:
                for index, row in self.df.iterrows():
                    drug_admin_time = pd.to_datetime(row[params['drug_admin_col']])
                    urine_analysis_time = pd.to_datetime(row[params['urine_analysis_col']]) if pd.notnull(row[params['urine_analysis_col']]) else None

                    for offset_key, (offset, deviation) in time_offsets.items():
                        vitals_col = f'{params["vitals_prefix"]}{offset_key}_VSDTC'
                        physical_col = f'{params["physical_prefix"]}{offset_key}_PEDTC'
                        
                        # Add the columns to checked_fields set before proceeding with checks
                        self.checked_fields.add(vitals_col)
                        self.checked_fields.add(physical_col)

                        # Skip if either vital signs or physical exam time is null
                        if pd.isnull(row[vitals_col]) or pd.isnull(row[physical_col]):
                            continue

                        vitals_time = pd.to_datetime(row[vitals_col])
                        physical_time = pd.to_datetime(row[physical_col])

                        # Check if times are within the specified window
                        if not (offset - deviation <= (vitals_time - drug_admin_time).total_seconds() / 60 <= offset + deviation) or \
                        (urine_analysis_time and vitals_time == urine_analysis_time):
                            self.log_error(index, vitals_col)
                        
                            print(vitals_time)
            
                        if not (offset - deviation <= (physical_time - drug_admin_time).total_seconds() / 60 <= offset + deviation) or \
                        (urine_analysis_time and physical_time == urine_analysis_time):
                            self.log_error(index, physical_col)


    def check_meal_and_fluid_intake_timing(self):
            for visit_num in [1,4]:
                drug_admin_col = f'V{visit_num}_{"07" if visit_num == 1 else "06"}_EXDTC'
                meal_end_dinner_col = f'V{visit_num}_{"11" if visit_num == 1 else "10"}_01_MLENDTC'
                fluid_end_col = f'V{visit_num}_{"11" if visit_num == 1 else "10"}_02_MLENDTC'
                fluid_start_col = f'V{visit_num}_{"11" if visit_num == 1 else "10"}_03_MLSTDTC'
                breakfast_col = f'V{visit_num}_{"11" if visit_num == 1 else "10"}_04_MLSTDTC'
                lunch_col = f'V{visit_num}_{"11" if visit_num == 1 else "10"}_05_MLSTDTC'
                dinner_col = f'V{visit_num}_{"11" if visit_num == 1 else "10"}_06_MLSTDTC'

                # Add the fields to checked_fields set before proceeding with checks
                self.checked_fields.add(drug_admin_col)
                self.checked_fields.add(meal_end_dinner_col)
                self.checked_fields.add(fluid_end_col)
                self.checked_fields.add(fluid_start_col)
                self.checked_fields.add(breakfast_col)
                self.checked_fields.add(lunch_col)
                self.checked_fields.add(dinner_col)

                for index, row in self.df.iterrows():
                    drug_admin_time = pd.to_datetime(row[drug_admin_col])

                    # The checks remain the same
                    if not pd.isnull(row[meal_end_dinner_col]):
                        dinner_end_time = pd.to_datetime(row[meal_end_dinner_col])
                        if (drug_admin_time - dinner_end_time).total_seconds() < 28800:  # 8 hours in seconds
                            self.log_error(index, meal_end_dinner_col)

                    if not pd.isnull(row[fluid_end_col]):
                        fluid_end_time = pd.to_datetime(row[fluid_end_col])
                        if (drug_admin_time - fluid_end_time).total_seconds() < 3600:  # 1 hour in seconds
                            self.log_error(index, fluid_end_col)

                    if not pd.isnull(row[fluid_start_col]):
                        fluid_start_time = pd.to_datetime(row[fluid_start_col])
                        if (fluid_start_time - drug_admin_time).total_seconds() < 7200:  # 2 hours in seconds
                            self.log_error(index, fluid_start_col)

                    if not pd.isnull(row[breakfast_col]):
                        breakfast_time = pd.to_datetime(row[breakfast_col])
                        if (breakfast_time - drug_admin_time).total_seconds() < 14400:  # 4 hours in seconds
                            self.log_error(index, breakfast_col)

                    if not pd.isnull(row[lunch_col]):
                        lunch_time = pd.to_datetime(row[lunch_col])
                        if abs((lunch_time - drug_admin_time).total_seconds() - 21600) > 300:  # 6 hours ± 5 min in seconds
                            self.log_error(index, lunch_col)

                    if not pd.isnull(row[dinner_col]):
                        dinner_time = pd.to_datetime(row[dinner_col])
                        if abs((dinner_time - drug_admin_time).total_seconds() - 36000) > 300:  # 10 hours ± 5 min in seconds
                            self.log_error(index, dinner_col)


    def check_discharge_timing(self):
            for visit_num in [1, 4]:
                discharge_col = f'V{visit_num}_{"12" if visit_num == 1 else "11"}_HOENDTC'
                procedures_cols = [
                    f'V{visit_num}_{"09" if visit_num == 1 else "08"}_6_VSDTC',
                    f'V{visit_num}_{"10" if visit_num == 1 else "09"}_6_PEDTC',
            
                ]

            # Add the discharge column to checked_fields set before proceeding with checks
            self.checked_fields.add(discharge_col)

            for index, row in self.df.iterrows():
                # Ensure discharge time is not null
                if pd.isnull(row[discharge_col]):
                    continue

                discharge_time = pd.to_datetime(row[discharge_col])
                latest_procedure_time = None

                # Loop through the procedures to find the latest time and add them to checked_fields
                for proc_col in procedures_cols:
                    # Add each procedure column to checked_fields set
                    self.checked_fields.add(proc_col)

                    if pd.notnull(row[proc_col]):
                        procedure_time = pd.to_datetime(row[proc_col])
                        if not latest_procedure_time or procedure_time > latest_procedure_time:
                            latest_procedure_time = procedure_time

                # Check if discharge is at least 72 hours after the latest procedure time
                if latest_procedure_time and (discharge_time - latest_procedure_time).total_seconds() < 0:
                    self.log_error(index, discharge_col)



    def generate_report(self):
        # After all checks have been performed
        with pd.ExcelWriter('error_report.xlsx') as writer:
            if self.errors:
                error_df = self.filter_errors()
                error_df.to_excel(writer, sheet_name='Errors', index=False)
            
            # Generate a DataFrame for the checked fields
            checked_df = pd.DataFrame({'Field': list(self.checked_fields), 'Status': ['Checked'] * len(self.checked_fields)})
            checked_df.to_excel(writer, sheet_name='Checked Fields', index=False)


    def perform_checks(self):
        # checks 1 and 2
        self.check_1_dates(1)
        self.check_1_dates(4)
    # checks 4 and 5
        self.check_4_5_screening_procedure_dates_to_consent()
    # check 8
        self.check_8_screening_procedure_dates_time()
    # check 7
        self.check_7_first_sample_collection(hospitalization_num=1, section_num=7, section2_num=8)
        self.check_7_first_sample_collection(hospitalization_num=4, section_num=6, section2_num=7)

        self.check_7_sample_intervals(hospitalization_num=1, section_num=7, section2_num=8)
        self.check_7_sample_intervals(hospitalization_num=4, section_num=6, section2_num=7) 
           
        ip_ps_v1 = f'V1_07_EXDTC'
        sample_intervals_v1 = {
            f'V3_05_LBDTC': (timedelta(hours=72), timedelta(minutes=60)),
            f'V3_06_LBDTC': (timedelta(hours=72), timedelta(minutes=60)),
            f'V3_07_LBDTC': (timedelta(hours=72), timedelta(minutes=60)),
        }

        ip_ps_v2 = f'V4_06_EXDTC'
        sample_intervals_v2 = {
            f'V6_05_LBDTC': (timedelta(hours=72), timedelta(minutes=60)),
            f'V6_06_LBDTC': (timedelta(hours=72), timedelta(minutes=60)),
            f'V6_07_LBDTC': (timedelta(hours=72), timedelta(minutes=60)),
        }
        # check 9
        self.check_9_blood_pee_72h(sample_intervals=sample_intervals_v1, reference_col=ip_ps_v1)
        self.check_9_blood_pee_72h(sample_intervals=sample_intervals_v2, reference_col=ip_ps_v2)

        self.check_unique_analyzis_values(blood_analysis=['V3_05_LBDTC', 'V3_06_LBDTC'], pee_analysis=['V3_07_LBDTC'], fo_jvp=['V3_04_PEDTC', 'V3_03_VSDTC'])
        self.check_unique_analyzis_values(blood_analysis=['V6_05_LBDTC', 'V6_06_LBDTC'], pee_analysis=['V6_07_LBDTC'], fo_jvp=['V6_04_PEDTC', 'V6_03_VSDTC'])
        #check 12
        self.check_randomization_timing()
        #check 13
        self.check_drug_administration_timing()
        #check 14
        self.check_catheter_placement_timing()
        #check 15
        self.check_catheter_removal_timing()
        #check 16
        self.check_vitals_and_physical_timing()
        #check 17
        self.check_meal_and_fluid_intake_timing()

        #check 18
        self.check_discharge_timing()

        if self.errors:
            error_df = self.filter_errors()
            error_df.to_excel('error_report.xlsx', index=False)
        else:
            print("No errors found.")

        self.generate_report()
