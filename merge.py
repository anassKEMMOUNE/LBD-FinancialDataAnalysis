
import pandas as pd

loan_applications = pd.read_csv("loan_applications_test.csv")
previous_credits = pd.read_csv("previous_credits.csv")
credit_bureau_balance=pd.read_csv("credit_bureau_balance.csv")
previous_POS_cash_loans=pd.read_csv("previous_POS_cash_loans.csv")
previous_credit_cards=pd.read_csv("previous_credit_cards.csv")
previous_loan_applications=pd.read_csv("previous_loan_applications.csv")
repayment_history=pd.read_csv("repayment_history.csv")

merged_data = pd.merge(loan_applications, previous_credits, on=["sk_id_curr"], how="outer")#with another common column called "amt_annuity"
merged_data1 = pd.merge(merged_data, credit_bureau_balance, on=["sk_id_bureau"], how="outer")
merged_data2 = pd.merge(previous_credit_cards, previous_POS_cash_loans, on=["sk_id_prev"], how="outer")#with  common columns  months_balance and sk_id_curr
merged_data3 = pd.merge(merged_data2, credit_bureau_balance, on=["months_balance"], how="outer")
merged_data4 = pd.merge(loan_applications, previous_loan_applications, on=["name_contract_type"], how="outer")#with common columns amt_credit and amt_annuity
merged_data5 = pd.merge(merged_data4, merged_data3,repayment_history, on=["sk_id_curr"], how="outer")#with another common column sk_id_prev
#repayment_history and previous_POS_cash_loans and previous_credit_cards and previous_loan_applications  have 2 common columns sk_id_prev and sk_id_curr
#print(merged_data.to_csv("merge.csv"))
print(merged_data5)




