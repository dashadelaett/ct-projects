# Проверки

## Визиты

1. Даты процедур на Госпитализации 1 в полях V1_03_MBDAT V1_04_LBDAT V1_05_LBDAT V1_06_LBDAT дб равны дате Госпитализации 1 в поле V1_01_SVSTDTC
2. Даты процедур на Госпитализации 2 в полях V2_03_MBDAT V2_04_LBDAT V2_05_LBDAT V2_06_LBDAT дб равны дате Госпитализации 2 в поле V2_01_SVSTDTC
3. Дата проведения оценки непереносимости препарата дб после введения препарата на каждом Визите
   на Визите 1:
   V1_17_LIKERT_SCALE_5POINT_QSDAT > V1_08_EXDTC
   на Визите 2:
   V2_16_LIKERT_SCALE_5POINT_QSDAT > V2_07_EXDTC
4. Даты процедур на Скрининге V0_01_RFICDTC V0_02_MBDTC V0_05_VSDTC V0_06_PEDTC V0_07_EGDTC V0_08_LBDTC V0_09_LBDTC V0_10_LBDTC V0_11_ISDTC V0_12_LBDTC V0_13_LBDTC V0_14_LBDTC V0_15_PDDTC
дб равны дате Подписания Информированного согласия V0_01_RFICDTC. 
5. Также эти процедуры должны проводиться после подписания ИС [процедуры на Скрининге]>V0_01_RFICDTC.
6. Отбор проб на Визите 1 V1_09_01_PCDTC    V1_09_02_PCDTC	V1_09_03_PCDTC	V1_09_04_PCDTC	V1_09_05_PCDTC	V1_09_06_PCDTC должны быть на один день больше Даты Госпитализации 1 V1_01_SVSTDTC
[Даты отбора проб]-[Дата V1_01_SVSTDTC] = 1
   Отбор проб на Визите 2 V2_08_01_PCDTC	V2_08_02_PCDTC	V2_08_03_PCDTC	V2_08_04_PCDTC	V2_08_05_PCDTC	V2_08_06_PCDTC должны быть на один день больше Даты Госпитализации 2 V2_01_SVSTDTC
[Даты отбора проб]-[Дата V2_01_SVSTDTC] = 1

7. Пробы должны быть взяты через определенные промежутки времени после приема ИП/ПС с учетом отклонения:
Образец №1 взят до приема ИП/ПС: V1_09_01_PCDTC< V1_08_EXDTC, но в тот же день что и V1_08_EXDTC
Образец №2 - через 3 ч (±2 мин) взят 
Образец №3 - через 6 ч (±2 мин) взят? V1_09_03_PCDTC
Образец №4 - через 8 ч (±2 мин) взят? V1_09_04_PCDTC
Образец №5 - через 9 ч (±2 мин) взят? V1_09_05_PCDTC
Образец №6 - через 10 ч (±2 мин) взят? V1_09_06_PCDTC
Образец №7 - через 10 ч 30 мин (±2 мин) взят? V1_09_07_PCDTC
Образец №8 - через 11 ч (±2 мин) взят? V1_09_08_PCDTC
Образец №9 - через 11 ч 30 мин (±2 мин) взят? V1_09_09_PCDTC
Образец №10 - через 12 ч (±2 мин) взят? V1_09_10_PCDTC
Образец №11 - через 13 ч (±2 мин) взят? V1_09_11_PCDTC
Образец №12 - через 14 часов (±2 мин) взят? V1_09_12_PCDTC
Образец №13 - через 16 ч (±2 мин) взят? V1_09_13_PCDTC
Образец №14 - через 24 часа (±5 мин) взят? V1_09_14_PCDTC
Образец №15 - через 36 ч (±10 мин) взят? V1_09_15_PCDTC
Образец №16 - через 48 ч (±10 мин) взят? V1_09_16_PCDTC
Образец №17 - через 60 ч (±10 мин) взят? V1_09_17_PCDTC
Образец №18 - через 72 ч (±10 мин) взят? V1_09_18_PCDTC
## Общие формы

1. 
2. 
3. 