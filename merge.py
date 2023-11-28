import pandas as pd

loan_applications = pd.read_csv("Dataset/loan_applications_test.csv")
previous_credits = pd.read_csv("Dataset/previous_credits.csv")
credit_bureau_balance=pd.read_csv("Dataset/credit_bureau_balance.csv")
previous_POS_cash_loans=pd.read_csv("Dataset/previous_POS_cash_loans.csv")
previous_credit_cards=pd.read_csv("Dataset/previous_credit_cards.csv")
previous_loan_applications=pd.read_csv("Dataset/previous_loan_applications.csv")
repayment_history=pd.read_csv("Dataset/repayment_history.csv")

merged_data = pd.merge(loan_applications, previous_credits, on=["sk_id_curr"], how="outer")#with another common column called "amt_annuity"
merged_data.to_csv("Dataset/merged_data.csv")

# del loan_applications
del previous_credits

merged_data1 = pd.merge(merged_data, credit_bureau_balance, on=["sk_id_bureau"], how="outer")
merged_data1.to_csv("Dataset/merged_data1.csv")

del merged_data
# del credit_bureau_balance

merged_data2 = pd.merge(previous_credit_cards, previous_POS_cash_loans, on=["sk_id_prev"], how="outer")#with  common columns  months_balance and sk_id_curr
merged_data2.to_csv("merged_data2.csv")

del previous_credit_cards
del previous_POS_cash_loans

merged_data3 = pd.merge(merged_data2, credit_bureau_balance, on=["months_balance"], how="outer")
merged_data3.to_csv("merged_data3.csv")

del merged_data2
del credit_bureau_balance

merged_data4 = pd.merge(loan_applications, previous_loan_applications, on=["name_contract_type"], how="outer")#with common columns amt_credit and amt_annuity
merged_data4.to_csv("merged_data4.csv")

del loan_applications
del previous_loan_applications

merged_data5 = pd.merge(merged_data4, merged_data3, on=["sk_id_curr"], how="outer")#with another common column sk_id_prev
#repayment_history and previous_POS_cash_loans and previous_credit_cards and previous_loan_applications  have 2 common columns sk_id_prev and sk_id_curr
merged_data5.to_csv("merged_data5.csv")

del merged_data4
del merged_data3

merged_data6 = pd.merge(merged_data5, repayment_history, on=["sk_id_curr"], how="outer")
merged_data6.to_csv("merged_data6.csv")

del merged_data5
del merged_data6
